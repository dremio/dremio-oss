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
package com.dremio.exec.planner.sql.handlers.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.catalog.DatasetCatalog.UpdateStatus;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.ViewAccessEvaluator;
import com.dremio.exec.planner.sql.parser.DataAdditionCmdCall;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

public abstract class DataAdditionCmdHandler implements SqlToPlanHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataAdditionCmdHandler.class);


  private String textPlan;
  private CreateTableEntry tableEntry = null;
  private BatchSchema tableSchemaFromKVStore = null;
  private List<String> partitionColumns = null;
  private Map<String, Object> storageOptionsMap = null;
  private boolean isIcebergTable = false;
  private DremioTable cachedDremioTable = null;
  private boolean dremioTableFetched = false;
  private boolean isVersionedTable = false;

  public DataAdditionCmdHandler() {
  }

  public boolean isIcebergTable() {
    return isIcebergTable;
  }
  private boolean isVersionedTable() {
    return isVersionedTable;
  }

  public WriterOptions.IcebergWriterOperation getIcebergWriterOperation() {
    if (!isIcebergTable) {
      return WriterOptions.IcebergWriterOperation.NONE;
    }
    return isCreate() ? WriterOptions.IcebergWriterOperation.CREATE : WriterOptions.IcebergWriterOperation.INSERT;
  }

  public abstract boolean isCreate();

  protected void cleanUp(DatasetCatalog datasetCatalog, NamespaceKey key) {}

  public PhysicalPlan getPlan(DatasetCatalog datasetCatalog,
                              NamespaceKey path,
                              SqlHandlerConfig config,
                              String sql,
                              DataAdditionCmdCall sqlCmd,
                              ResolvedVersionContext version
  ) throws Exception {
    try {
      final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, sqlCmd.getQuery());
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();

      long maxColumnCount = config.getContext().getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX);
      if (validatedRowType.getFieldCount() > maxColumnCount) {
        throw new ColumnCountTooLargeException((int) maxColumnCount);
      }

      // Get and cache table info (if not already retrieved) to avoid multiple retrievals
      getDremioTable(datasetCatalog, path);
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();
      ViewAccessEvaluator viewAccessEvaluator = null;
      if (config.getConverter().getSubstitutionProvider().isDefaultRawReflectionEnabled()) {
        final RelNode convertedRelWithExpansionNodes = ((DremioVolcanoPlanner) queryRelNode.getCluster().getPlanner()).getOriginalRoot();
        viewAccessEvaluator = new ViewAccessEvaluator(convertedRelWithExpansionNodes, config);
        config.getContext().getExecutorService().submit(viewAccessEvaluator);
      }
      final RelNode newTblRelNode = SqlHandlerUtil.resolveNewTableRel(false, sqlCmd.getFieldNames(),
          validatedRowType, queryRelNode, !isCreate());

      final long ringCount = config.getContext().getOptions().getOption(PlannerSettings.RING_COUNT);

      ByteString extendedByteString = null;
      if (!isCreate()) {
        extendedByteString = cachedDremioTable.getDatasetConfig().getReadDefinition().getExtendedProperty();
      }

      // For Insert command we make sure query schema match exactly with table schema,
      // which includes partition columns. So, not checking here
      final RelNode newTblRelNodeWithPCol = SqlHandlerUtil.qualifyPartitionCol(newTblRelNode,
        isCreate() ?
          sqlCmd.getPartitionColumns(null /*param is unused in this interface for create */) :
          Lists.newArrayList());

      PrelTransformer.log("Calcite", newTblRelNodeWithPCol, logger, null);

      final List<String> partitionFieldNames = sqlCmd.getPartitionColumns(cachedDremioTable);
      final Set<String> fieldNames = validatedRowType.getFieldNames().stream().collect(Collectors.toSet());
      final WriterOptions options = new WriterOptions(
        (int) ringCount,
        partitionFieldNames,
        sqlCmd.getSortColumns(),
        sqlCmd.getDistributionColumns(),
        sqlCmd.getPartitionDistributionStrategy(config, partitionFieldNames, fieldNames),
        sqlCmd.isSingleWriter(),
        Long.MAX_VALUE,
        getIcebergWriterOperation(),
        extendedByteString,
        version
      );

      // Convert the query to Dremio Logical plan and insert a writer operator on top.
      Rel drel = this.convertToDrel(
        config,
        newTblRelNodeWithPCol,
        datasetCatalog,
        path,
        options,
        newTblRelNode.getRowType(),
        storageOptionsMap, sqlCmd.getFieldNames(), sqlCmd);

      final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, drel);
      final Prel prel = convertToPrel.getKey();
      textPlan = convertToPrel.getValue();
      PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);

      PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop,
        isIcebergTable() && !isVersionedTable() ?
          () -> refreshDataset(datasetCatalog, path, isCreate())
          : null,
        () -> cleanUp(datasetCatalog, path));

      PrelTransformer.log(config, "Dremio Plan", plan, logger);

      if (viewAccessEvaluator != null) {
        viewAccessEvaluator.getLatch().await(config.getContext().getPlannerSettings().getMaxPlanningPerPhaseMS(), TimeUnit.MILLISECONDS);
        if (viewAccessEvaluator.getException() != null) {
          throw viewAccessEvaluator.getException();
        }
      }

      return plan;

    } catch(Exception ex){
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  protected CreateTableEntry getTableEntry() {
    return tableEntry;
  }
  // Cache the table metadata so we can retrieve schema and partition columns and avoid double lookups
  protected DremioTable getDremioTable(DatasetCatalog datasetCatalog, NamespaceKey path) {
    if (!dremioTableFetched) {
      cachedDremioTable = datasetCatalog.getTable(path);
      dremioTableFetched = true;
    }
    return cachedDremioTable;
  }

  protected void checkExistenceValidity(NamespaceKey path, DremioTable dremioTable) {
    if (dremioTable != null && isCreate()) {
      throw UserException.validationError()
        .message("A table or view with given name [%s] already exists.", path.toString())
        .build(logger);
    }
    if (dremioTable == null && !isCreate()) {
      throw UserException.validationError()
        .message("Table [%s] not found", path)
        .buildSilently();
    }
  }

  // For iceberg tables, do a refresh after the attempt completion.
  public static void refreshDataset(DatasetCatalog datasetCatalog, NamespaceKey key, boolean autopromote) {
    DatasetRetrievalOptions options = DatasetRetrievalOptions.DEFAULT.toBuilder()
        .setForceUpdate(true).build();
    if (autopromote && !options.autoPromote()) {
      options = DatasetRetrievalOptions.newBuilder()
        .setAutoPromote(true)
        .setForceUpdate(true)
        .build()
        .withFallback(options);
    }

    UpdateStatus updateStatus = datasetCatalog.refreshDataset(key, options, false);
    logger.info("refreshed{} dataset {}, update status \"{}\"",
      autopromote ? " and autopromoted" : "", key, updateStatus.name());
  }

  private Rel convertToDrel(
    SqlHandlerConfig config,
    RelNode relNode,
    DatasetCatalog datasetCatalog,
    NamespaceKey key,
    WriterOptions options,
    RelDataType queryRowType,
    final Map<String, Object> storageOptions,
    final List<String> fieldNames,
    DataAdditionCmdCall sqlCmd)
      throws SqlUnsupportedException {
    Rel convertedRelNode = PrelTransformer.convertToDrel(config, relNode);

    // Put a non-trivial topProject to ensure the final output field name is preserved, when necessary.
    // Only insert project when the field count from the child is same as that of the queryRowType.

    String queryId = "";
    IcebergTableProps icebergTableProps = null;
    if (isIcebergTable()) {
      queryId = QueryIdHelper.getQueryId(config.getContext().getQueryId());
      ByteString partitionSpec = null;
      BatchSchema tableSchema = null;
      String icebergSchema = null;
      if (!isCreate()) {
        tableSchemaFromKVStore = cachedDremioTable.getSchema();
        partitionColumns = cachedDremioTable.getDatasetConfig().getReadDefinition().getPartitionColumnsList();
        partitionSpec = IcebergUtils.getCurrentPartitionSpec(cachedDremioTable.getDatasetConfig().getPhysicalDataset(), cachedDremioTable.getSchema(), options.getPartitionColumns());
        tableSchema = cachedDremioTable.getSchema();
        icebergSchema = IcebergUtils.getCurrentIcebergSchema(cachedDremioTable.getDatasetConfig().getPhysicalDataset(), cachedDremioTable.getSchema());
        // This is insert statement update  key to use existing table from catalog
        DremioTable table = datasetCatalog.getTable(key);
        if(table != null) {
          key = table.getPath();
        }
      } else {
        tableSchema = CalciteArrowHelper.fromCalciteRowType(queryRowType);
        PartitionSpec partitionSpecBytes = IcebergUtils.getIcebergPartitionSpecFromTransforms(tableSchema, sqlCmd.getPartitionTransforms(null), null);
        partitionSpec = ByteString.copyFrom(IcebergSerDe.serializePartitionSpec(partitionSpecBytes));
        icebergSchema = IcebergSerDe.serializedSchemaAsJson(partitionSpecBytes.schema());
      }
      icebergTableProps = new IcebergTableProps(null, queryId,
        null,
        options.getPartitionColumns() ,
        isCreate() ? IcebergCommandType.CREATE : IcebergCommandType.INSERT,
        null, key.getName(), null, options.getVersion(), partitionSpec, icebergSchema); // TODO: DX-43311 Should we allow null version?
      icebergTableProps.setPersistedFullSchema(tableSchema);
      options.setIcebergTableProps(icebergTableProps);
    }

    logger.debug("Creating new table with WriterOptions : '{}' icebergTableProps : '{}' ",
      options,
      icebergTableProps);

    tableEntry = datasetCatalog.createNewTable(
      key,
      icebergTableProps,
      options,
      storageOptions);
    if (isIcebergTable()) {
      Preconditions.checkState(tableEntry.getIcebergTableProps().getTableLocation() != null &&
            !tableEntry.getIcebergTableProps().getTableLocation().isEmpty(),
        "Table folder location must not be empty");
    }

    if (!isCreate()) {
      BatchSchema partSchemaWithSelectedFields = tableSchemaFromKVStore.subset(fieldNames).orElse(tableSchemaFromKVStore);
      queryRowType = CalciteArrowHelper.wrap(partSchemaWithSelectedFields)
          .toCalciteRecordType(convertedRelNode.getCluster().getTypeFactory(), PrelUtil.getPlannerSettings(convertedRelNode.getCluster()).isFullNestedSchemaSupport());
      logger.debug("Inserting into table with schema : '{}' ", tableSchemaFromKVStore.toString());
    }

    convertedRelNode = addCastProject(convertedRelNode, queryRowType);
    convertedRelNode = new WriterRel(convertedRelNode.getCluster(),
      convertedRelNode.getCluster().traitSet().plus(Rel.LOGICAL),
      convertedRelNode, tableEntry, queryRowType);

    convertedRelNode = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(),
      config.getContext(), convertedRelNode);

    return new ScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }

  public Rel addCastProject(RelNode convertedRelNode,
                            RelDataType queryRowType) {
    RexBuilder rexBuilder = convertedRelNode.getCluster().getRexBuilder();
    List<RelDataTypeField> fields = queryRowType.getFieldList();
    List<RelDataTypeField> inputFields = convertedRelNode.getRowType().getFieldList();
    List<RexNode> castExprs = new ArrayList<>();

    if (inputFields.size() > fields.size()) {
      throw UserException.validationError().message("The number of fields in values cannot exceed " +
          "the number of fields in the schema. Schema: %d, Given: %d",
        fields.size(), inputFields.size()).buildSilently();
    }

    for (int i = 0; i < fields.size(); i++) {
      RelDataType type = fields.get(i).getType();
      RexNode rexNode = i < inputFields.size() ?
        new RexInputRef(i, inputFields.get(i).getType()) :
        rexBuilder.makeNullLiteral(type);
      RexNode castExpr;
      if (!isCastSupportedForType(type)) {
        castExpr = rexNode;
      } else if (rexBuilder instanceof DremioRexBuilder) {
        castExpr = ((DremioRexBuilder) rexBuilder).makeAbstractCastIgnoreType(type, rexNode);
      } else {
        castExpr = rexBuilder.makeAbstractCast(type, rexNode);
      }
      castExprs.add(castExpr);
    }

    return ProjectRel.create(
      convertedRelNode.getCluster(),
      convertedRelNode.getCluster().traitSet().plus(Rel.LOGICAL),
      convertedRelNode,
      castExprs,
      queryRowType);
  }

  private boolean isCastSupportedForType(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    return typeName != SqlTypeName.ARRAY &&
      typeName != SqlTypeName.ROW &&
      typeName != SqlTypeName.STRUCTURED &&
      typeName != SqlTypeName.MAP;
  }

  public void validateIcebergSchemaForInsertCommand(List<String> fieldNames) {
    IcebergTableProps icebergTableProps = tableEntry.getIcebergTableProps();
    Preconditions.checkState(icebergTableProps.getIcebergOpType() == IcebergCommandType.INSERT,
      "unexpected state found");

    BatchSchema querySchema = icebergTableProps.getFullSchema();
    Preconditions.checkState(tableEntry.getPlugin() instanceof SupportsIcebergMutablePlugin, "Plugin not instance of SupportsIcebergMutablePlugin");
    IcebergModel icebergModel = ((SupportsIcebergMutablePlugin) tableEntry.getPlugin())
      .getIcebergModel(icebergTableProps, tableEntry.getUserName(), null, null);
    Table table = icebergModel.getIcebergTable(icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()));
    SchemaConverter schemaConverter = new SchemaConverter(table.name());
    BatchSchema icebergSchema = schemaConverter.fromIceberg(table.schema());

    // this check can be removed once we support schema evolution in dremio.
    if (!icebergSchema.equalsIgnoreCase(tableSchemaFromKVStore)) {
      throw UserException.validationError().message("The schema for table %s does not match with the iceberg %s.",
        tableSchemaFromKVStore, icebergSchema).buildSilently();
    }

    BatchSchema partSchemaWithSelectedFields = tableSchemaFromKVStore.subset(fieldNames).orElse(tableSchemaFromKVStore);
    if (!querySchema.equalsIgnoreCase(partSchemaWithSelectedFields)) {
      throw UserException.validationError().message("Table %s doesn't match with query %s.",
          partSchemaWithSelectedFields, querySchema).buildSilently();
    }
  }

  private boolean comparePartitionColumnLists(List<String> icebergPartitionColumns) {
    boolean icebergHasPartitionSpec = (icebergPartitionColumns != null && icebergPartitionColumns.size() > 0);
    boolean kvStoreHasPartitionSpec = (partitionColumns != null && partitionColumns.size() > 0);
    if (icebergHasPartitionSpec != kvStoreHasPartitionSpec) {
      return false;
    }

    if(!icebergHasPartitionSpec && !kvStoreHasPartitionSpec) {
      return true;
    }

    if (icebergPartitionColumns.size() != partitionColumns.size()) {
      return false;
    }

    for (int index = 0; index < icebergPartitionColumns.size(); ++index) {
      if (!icebergPartitionColumns.get(index).equalsIgnoreCase(partitionColumns.get(index))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Helper method to create map of key, value pairs, the value is a Java type object.
   * @param args
   * @return
   */
  @VisibleForTesting
  public void createStorageOptionsMap(final SqlNodeList args) {
    if (args == null || args.size() == 0) {
      return;
    }

    final ImmutableMap.Builder<String, Object> storageOptions = ImmutableMap.builder();
    for (SqlNode operand : args) {
      if (operand.getKind() != SqlKind.ARGUMENT_ASSIGNMENT) {
        throw UserException.unsupportedError()
          .message("Unsupported argument type. Only assignment arguments (param => value) are supported.")
          .build(logger);
      }
      final List<SqlNode> operandList = ((SqlCall) operand).getOperandList();

      final String name = ((SqlIdentifier) operandList.get(1)).getSimple();
      SqlNode literal = operandList.get(0);
      if (!(literal instanceof SqlLiteral)) {
        throw UserException.unsupportedError()
          .message("Only literals are accepted for storage option values")
          .build(logger);
      }

      Object value = ((SqlLiteral)literal).getValue();
      if (value instanceof NlsString) {
        value = ((NlsString)value).getValue();
      }
      storageOptions.put(name, value);
    }

    this.storageOptionsMap = storageOptions.build();
  }

  public static boolean isStoreAsOptionIceberg(Map<String, Object> storageOptionsMap) {

    if (storageOptionsMap != null) {
      return storageOptionsMap.get("type").equals("iceberg") && storageOptionsMap.size() == 1;
    }

    return false;
  }

  public static boolean isStorageFormatIceberg(SourceCatalog sourceCatalog, NamespaceKey path, Map<String, Object> storageOptionsMap) {
    if (storageOptionsMap == null) {
      return "iceberg".equals(getDefaultCtasFormat(sourceCatalog, path));

    } else {
      return isStoreAsOptionIceberg(storageOptionsMap);
    }
  }

  public static boolean validatePath(SourceCatalog sourceCatalog,
                                     NamespaceKey path) {
    try {
      sourceCatalog.getSource(path.getRoot());
    } catch (UserException uex) {
      return false;
    }
    return true;
  }

  public static String getDefaultCtasFormat(SourceCatalog sourceCatalog,
                                            NamespaceKey path) {
    StoragePlugin storagePlugin = sourceCatalog.getSource(path.getRoot());
    Preconditions.checkState(storagePlugin instanceof MutablePlugin, "Source is not mutable");
    return ((MutablePlugin) storagePlugin).getDefaultCtasFormat();
  }

  @VisibleForTesting
  public void validateTableFormatOptions(SourceCatalog sourceCatalog, NamespaceKey path, OptionManager options) {
    if (isCreate()) {
      boolean isStorageIceberg = isStorageFormatIceberg(sourceCatalog, path, storageOptionsMap);
      if (isStorageIceberg) {
        resetStorageOptions();
      }
      boolean isIcebergTableSupported = IcebergUtils.isIcebergDMLFeatureEnabled(sourceCatalog, path, options, storageOptionsMap) &&
        IcebergUtils.validatePluginSupportForIceberg(sourceCatalog, path);
      isIcebergTable = isIcebergTableSupported && isStorageIceberg;
      if (!isIcebergTableSupported && isStorageIceberg) {
        logger.warn("Please enable required support options to perform create operation in specified/default iceberg format for {}.", path.toString());
      }
    } else {
      isIcebergTable = IcebergUtils.isIcebergDMLFeatureEnabled(sourceCatalog, path, options, storageOptionsMap) &&
        IcebergUtils.validatePluginSupportForIceberg(sourceCatalog, path);
    }
  }

  private void resetStorageOptions() {
    storageOptionsMap = null;
  }

  public static void validateCreateTableLocation(SourceCatalog sourceCatalog, NamespaceKey path, SqlCreateEmptyTable sqlCreateEmptyTable) {
    if (sqlCreateEmptyTable.getLocation() != null) {
      StoragePlugin storagePlugin = sourceCatalog.getSource(path.getRoot());
      if (storagePlugin instanceof FileSystemPlugin) {
        throw UserException.parseError().message("LOCATION clause is not supported in the query for this source").build(logger);
      }
    }
  }

  public void validateVersionedTableFormatOptions(Catalog catalog, NamespaceKey path) {
    isIcebergTable = isVersionedTable = CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog);
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }
}
