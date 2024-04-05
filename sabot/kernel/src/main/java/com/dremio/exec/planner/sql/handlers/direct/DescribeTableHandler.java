/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dremio.exec.planner.sql.handlers.direct;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlDescribeDremioTable;
import com.dremio.exec.planner.sql.parser.TableVersionSpec;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.ischema.Column;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;

public class DescribeTableHandler implements SqlDirectHandler<DescribeTableHandler.DescribeResult> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DescribeTableHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final QueryContext context;
  private final Catalog catalog;
  private final UserSession session;

  public DescribeTableHandler(Catalog catalog, QueryContext context, UserSession session) {
    super();
    this.catalog = catalog;
    this.context = context;
    this.session = session;
  }

  @Override
  public List<DescribeResult> toResult(String sql, SqlNode sqlNode)
      throws RelConversionException, ForemanSetupException {
    final SqlDescribeTable sqlDescribeTable = SqlNodeUtil.unwrap(sqlNode, SqlDescribeTable.class);
    NamespaceKey path = getPath(sqlDescribeTable);
    String sourceName = path.getRoot();
    try {
      TableVersionContext sourceVersion = getVersion(sqlDescribeTable, sourceName);
      CatalogEntityKey catalogEntityKey =
          CatalogEntityKey.newBuilder()
              .keyComponents(path.getPathComponents())
              .tableVersionContext(sourceVersion)
              .build();
      final DremioTable table = catalog.getTable(catalogEntityKey);
      final RelDataType type;
      if (table == null) {
        throw UserException.validationError().message("Unknown table [%s]", path).build(logger);
      } else {
        type = table.getRowType(JavaTypeFactoryImpl.INSTANCE);
      }

      Optional<Map<String, Integer>> sortOrderPriorityMapOpt = buildSortOrderPriorityMap(table);
      final Map<String, Integer> sortOrderPriorityMap = sortOrderPriorityMapOpt.get();

      List<DescribeResult> columns = new ArrayList<>();
      String columnName = null;
      final SqlIdentifier sqlColumn = sqlDescribeTable.getColumn();
      if (sqlColumn != null) {
        final List<String> names = sqlColumn.names;
        if (names.size() > 1) {
          throw UserException.validationError()
              .message(
                  "You can only describe single component paths. You tried to describe [%s].",
                  Joiner.on('.').join(names))
              .build(logger);
        }
        columnName = sqlColumn.getSimple();
      }

      final Map<String, List<ColumnExtendedProperty>> extendedPropertyColumns =
          catalog.getColumnExtendedProperties(table);

      for (RelDataTypeField field : type.getFieldList()) {
        Column column =
            new Column("dremio", path.getParent().toUnescapedString(), path.getLeaf(), field);
        if (columnName == null || columnName.equals(field.getName())) {
          final Integer precision = column.NUMERIC_PRECISION;
          final Integer scale = column.NUMERIC_SCALE;
          final List<ColumnExtendedProperty> columnExtendedProperties =
              getColumnExtendedProperties(column.COLUMN_NAME, extendedPropertyColumns);
          final String extendedPropertiesString =
              columnExtendedPropertiesToString(columnExtendedProperties);
          String columnPolicies = getColumnPoliciesForColumn(table.getPath(), column.COLUMN_NAME);
          Integer localSortPriority = sortOrderPriorityMap.getOrDefault(field.getName(), null);
          DescribeResult describeResult =
              new DescribeResult(
                  field.getName(),
                  column.DATA_TYPE,
                  precision,
                  scale,
                  extendedPropertiesString,
                  columnPolicies,
                  localSortPriority);
          columns.add(describeResult);
        }
      }
      return columns;
    } catch (AccessControlException e) {
      throw UserException.permissionError(e)
          .message("Not authorized to describe table.")
          .build(logger);
    } catch (Exception ex) {
      throw UserException.planError(ex)
          .message("Error while rewriting DESCRIBE query: %s", ex.getMessage())
          .build(logger);
    }
  }

  private Map<String, Integer> fillSortOrderPriorityMap(
      DremioTable table, IcebergMetadata icebergMetadata) {
    if (icebergMetadata == null || icebergMetadata.getSortOrder() == null) {
      return Collections.EMPTY_MAP;
    }

    String sortOrder = icebergMetadata.getSortOrder();

    Schema icebergSchema = SchemaConverter.getBuilder().build().toIcebergSchema(table.getSchema());

    SortOrder deserializedSortOrder =
        IcebergSerDe.deserializeSortOrderFromJson(icebergSchema, sortOrder);
    final List<String> sortColumns =
        IcebergUtils.getColumnsFromSortOrder(deserializedSortOrder, context.getOptions());

    return IntStream.rangeClosed(1, sortColumns.size())
        .boxed()
        .collect(Collectors.toMap(i -> sortColumns.get(i - 1), i -> i));
  }

  private Optional<Map<String, Integer>> buildSortOrderPriorityMap(DremioTable table) {
    if (!IcebergUtils.isIcebergSortOrderFeatureEnabled(context.getOptions())) {
      return Optional.of(new HashMap<>());
    }
    try {
      TableMetadata dataset = table.getDataset();
      if (dataset == null) {
        return Optional.of(new HashMap<>());
      }

      DatasetConfig datasetConfig = dataset.getDatasetConfig();
      if (datasetConfig == null) {
        return Optional.of(new HashMap<>());
      }

      PhysicalDataset physicalDataset = datasetConfig.getPhysicalDataset();
      if (physicalDataset == null) {
        return Optional.of(new HashMap<>());
      }

      IcebergMetadata icebergMetadata = physicalDataset.getIcebergMetadata();
      return Optional.of(fillSortOrderPriorityMap(table, icebergMetadata));
    } catch (UnsupportedOperationException uoe) {
      return Optional.of(new HashMap<>());
    }
  }

  private List<ColumnExtendedProperty> getColumnExtendedProperties(
      String columnName, Map<String, List<ColumnExtendedProperty>> extendedPropertyColumns) {
    if (extendedPropertyColumns == null || !extendedPropertyColumns.containsKey(columnName)) {
      return Collections.emptyList();
    } else {
      return extendedPropertyColumns.get(columnName);
    }
  }

  private String columnExtendedPropertiesToString(
      List<ColumnExtendedProperty> columnExtendedProperties) {
    if (columnExtendedProperties == null) {
      return "[]";
    }

    try {
      return OBJECT_MAPPER.writeValueAsString(columnExtendedProperties);
    } catch (JsonProcessingException jpe) {
      logger.warn("Unable to JSON encode column extended properties.", jpe);
      return "[]";
    }
  }

  @Nullable
  protected String getColumnPoliciesForColumn(NamespaceKey key, String columnName)
      throws NamespaceException {
    return null;
  }

  private NamespaceKey getPath(SqlDescribeTable node) {
    return catalog.resolveSingle(new NamespaceKey(node.getTable().names));
  }

  /**
   * @param sqlDescribeTable
   * @param sourceName
   * @return Return tableVersionContext specified by AT syntax, if specified. Or else it will return
   *     the session version. Defaults to NOT_SPECIFIED
   */
  private TableVersionContext getVersion(SqlDescribeTable sqlDescribeTable, String sourceName) {
    if (sqlDescribeTable instanceof SqlDescribeDremioTable) {
      final SqlDescribeDremioTable sqlDescribeDremioTable =
          (SqlDescribeDremioTable) sqlDescribeTable;
      TableVersionSpec tableVersionSpec =
          sqlDescribeDremioTable.getSqlTableVersionSpec().getTableVersionSpec();
      if (tableVersionSpec.getTableVersionType() != TableVersionType.NOT_SPECIFIED) {
        return tableVersionSpec.getTableVersionContext();
      }
    }
    return TableVersionContext.of(session.getSessionVersionForSource(sourceName));
  }

  public static DescribeTableHandler create(
      Catalog catalog, QueryContext context, UserSession session) {
    try {
      final Class<?> cl =
          Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseDescribeTableHandler");
      final Constructor<?> ctor =
          cl.getConstructor(Catalog.class, QueryContext.class, UserSession.class);
      return (DescribeTableHandler) ctor.newInstance(catalog, context, session);
    } catch (ClassNotFoundException e) {
      return new DescribeTableHandler(catalog, context, session);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e2) {
      throw Throwables.propagate(e2);
    }
  }

  public static class DescribeResult {
    public final String COLUMN_NAME;
    public final String DATA_TYPE;
    public final String IS_NULLABLE = "YES";
    public final Integer NUMERIC_PRECISION;
    public final Integer NUMERIC_SCALE;
    public final String EXTENDED_PROPERTIES;
    public final String MASKING_POLICY;
    public final Integer SORT_ORDER_PRIORITY;

    public DescribeResult(
        String columnName,
        String dataType,
        Integer numericPrecision,
        Integer numericScale,
        String extendedProperties,
        String columnPolicies,
        Integer sortPriority) {
      super();
      COLUMN_NAME = columnName;
      DATA_TYPE = dataType;
      NUMERIC_PRECISION = numericPrecision;
      NUMERIC_SCALE = numericScale;
      EXTENDED_PROPERTIES = extendedProperties;
      MASKING_POLICY = columnPolicies;
      SORT_ORDER_PRIORITY = sortPriority;
    }
  }

  @Override
  public Class<DescribeResult> getResultType() {
    return DescribeResult.class;
  }
}
