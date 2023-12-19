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
package com.dremio.exec.planner.sql;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;

public class SchemaUtilities {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaUtilities.class);

  public static TableWithPath verify(final Catalog catalog, SqlIdentifier identifier, UserSession userSession, SqlTableVersionSpec sqlTableVersionSpec, OptionManager optionManager){
    NamespaceKey path = catalog.resolveSingle(new NamespaceKey(identifier.names));
    SqlHandlerUtil.validateSupportForVersionedReflections(path.getRoot(), catalog, optionManager);
    TableVersionContext tableVersionContext = null;
    if (sqlTableVersionSpec != null && sqlTableVersionSpec.getTableVersionSpec().getTableVersionType() != TableVersionType.NOT_SPECIFIED) {
      tableVersionContext = sqlTableVersionSpec.getTableVersionSpec().getTableVersionContext();
    } else if (CatalogUtil.requestedPluginSupportsVersionedTables(path.getRoot(), catalog)) {
      tableVersionContext = TableVersionContext.of(userSession.getSessionVersionForSource(path.getRoot()));
    }
    CatalogEntityKey catalogEntityKey = CatalogEntityKey.newBuilder()
      .keyComponents(path.getPathComponents())
      .tableVersionContext(tableVersionContext)
      .build();

    DremioTable table = catalog.getTable(catalogEntityKey);
    if (table == null) {
      throw UserException.parseError().message("Unable to find table %s.", path).build(logger);
    }

    return new TableWithPath(table);
  }

  /**
   * Returns all column paths for full projections
   */
  public static List<SchemaPath> allColPaths(BatchSchema batchSchema) {
    return batchSchema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList());
  }

  public static class TableWithPath {

    private final DremioTable table;
    private final List<String> path;

    public TableWithPath(DremioTable table) {
      super();
      this.table = table;
      this.path = table.getPath().getPathComponents();
    }
    public DremioTable getTable() {
      return table;
    }
    public List<String> getPath() {
      return path;
    }

    public List<String> qualifyColumns(List<String> strings){
      final RelDataType type = table.getRowType(JavaTypeFactoryImpl.INSTANCE);
      return FluentIterable.from(strings).transform(new Function<String, String>(){

        @Override
        public String apply(String input) {
          RelDataTypeField field = type.getField(input, false, false);
          if(field == null){
            throw UserException.validationError()
              .message("Unable to find field %s in table %s. Available fields were: %s.",
                  input,
                  SqlUtils.quotedCompound(path),
                  FluentIterable.from(type.getFieldNames()).transform(SqlUtils.QUOTER).join(Joiner.on(", "))
                ).build(logger);
          }

          return field.getName();
        }

      }).toList();

    }


  }

}
