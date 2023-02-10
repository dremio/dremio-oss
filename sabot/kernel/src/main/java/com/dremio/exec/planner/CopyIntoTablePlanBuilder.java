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
package com.dremio.exec.planner;

import static com.dremio.common.utils.PathUtils.parseFullPath;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.easy.EasyScanTableFunctionPrel;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.store.metadatarefresh.RefreshExecTableMetadata;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingScanPrel;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class CopyIntoTablePlanBuilder {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopyIntoTablePlanBuilder.class);
  public static long DEFAULT_FILE_COUNT_ESTIMATE = 1000_000L;
  public static long DEFAULT_EASY_SCAN_ROW_COUNT_ESTIMATE = 1_000_000_000L;

  private final RelOptTable targetTable;
  private final NamespaceKey sourceLocationNSKey;
  private final RelOptCluster cluster;
  private final RelTraitSet traitSet;
  private final StoragePluginId storagePluginId;
  private final TableMetadata tableMetadata;
  private final CopyIntoTableContext copyIntoTableContext;
  private final String userName;
  private Path datasetPath;
  private final SupportsInternalIcebergTable plugin;
  private boolean isFileDataset = false;
  private final FileConfig format;

  private final ExtendedFormatOptions extendedFormatOptions;

  /***
   * Expand plans for 'COPY INTO' command
   */
  public CopyIntoTablePlanBuilder(RelOptTable targetTable, RelOptCluster cluster, RelTraitSet traitSet,
                                  TableMetadata tableMetadata, OptimizerRulesContext context, CopyIntoTableContext copyIntoTableContext) {
    this.targetTable = Preconditions.checkNotNull(targetTable);
    this.tableMetadata = Preconditions.checkNotNull(tableMetadata, "TableMetadata cannot be null.");
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.copyIntoTableContext = copyIntoTableContext;
    this.userName = tableMetadata.getUser();

    final Catalog catalog = context.getCatalogService().getCatalog(MetadataRequestOptions.of(
      SchemaConfig.newBuilder(CatalogUser.from(userName)).build()));
    sourceLocationNSKey = catalog.resolveSingle(new NamespaceKey(parseFullPath(this.copyIntoTableContext.getStorageLocation())));
    try {
      this.storagePluginId = context.getCatalogService().getManagedSource(sourceLocationNSKey.getRoot()).getId();
      if (storagePluginId == null) {
        throw UserException.invalidMetadataError().message("Path %s not found.", this.copyIntoTableContext.getProvidedStorageLocation()).buildSilently();
      }
    } catch(Exception e) {
      throw UserException.invalidMetadataError(e).message("Path %s not found.", this.copyIntoTableContext.getProvidedStorageLocation()).buildSilently();
    }

    this.plugin = AbstractRefreshPlanBuilder.getPlugin(catalog, sourceLocationNSKey);
    if(!(this.plugin instanceof FileSystemPlugin)) {
      throw UserException.invalidMetadataError().message("Source provided %s is not valid object storage.", this.copyIntoTableContext.getProvidedStorageLocation()).buildSilently();
    }

    setDatasetPath();
    this.format = getFileFormat(copyIntoTableContext.getFileFormat(), copyIntoTableContext.getFormatOptions()).asFileConfig();
    this.extendedFormatOptions = getExtendedFormatOptions(copyIntoTableContext.getFormatOptions());
    if(logger.isDebugEnabled()) {
      logger.debug("'COPY INTO' Options : {}, File Format : {}", extendedFormatOptions, format.toString());
    }
  }

  private ExtendedFormatOptions getExtendedFormatOptions(Map<CopyIntoTableContext.FormatOption, Object> formatOptions) {
    ExtendedFormatOptions options = new ExtendedFormatOptions();
    for (Map.Entry<CopyIntoTableContext.FormatOption, Object> option : formatOptions.entrySet()) {
      switch (option.getKey()) {
        case TRIM_SPACE:
          options.setTrimSpace((Boolean)option.getValue());
          break;
        case DATE_FORMAT:
          options.setDateFormat((String)option.getValue());
          break;
        case TIME_FORMAT:
          options.setTimeFormat((String)option.getValue());
          break;
        case EMPTY_AS_NULL:
          options.setEmptyAsNull((Boolean)option.getValue());
          break;
        case TIMESTAMP_FORMAT:
          options.setTimeStampFormat((String) option.getValue());
          break;
        case NULL_IF:
          options.setNullIfExpressions((List<String>) option.getValue());
          break;
        case RECORD_DELIMITER:
        case FIELD_DELIMITER:
        case QUOTE_CHAR:
        case ESCAPE_CHAR:
          // These options are handled in getFileFormat() and we do not need to extract them here. But, break here to avoid "Format option is not supported errors" for these options.
          break;
        default:
          throw UserException.parseError()
            .message("Specified format option '%s' is not supported", option.getKey())
            .buildSilently();
      }
    }

    return options;
  }
  Optional<FileSelection>  generateFileSelectionForSourceRoot(NamespaceKey datasetPath, String user, FileSystemPlugin plugin) throws IOException {
    final List<String> fullPath = plugin.resolveTableNameToValidPath(datasetPath.getPathComponents());
    FileSystem fs = plugin.createFS(user);
    FileSelection fileSelection = FileSelection.createNotExpanded(fs, fullPath);
    return Optional.of(fileSelection);
  }

  private void setDatasetPath() {
    FileSystemPlugin fsPlugin = (FileSystemPlugin) plugin;
    Optional<FileSelection> fileSelectionOptional;
    try {
      if(sourceLocationNSKey.size() <= 1) {
        fileSelectionOptional = generateFileSelectionForSourceRoot(sourceLocationNSKey, userName, fsPlugin);
      } else {
        fileSelectionOptional = fsPlugin.generateFileSelectionForPathComponents(sourceLocationNSKey, userName);
      }
    } catch (IOException e) {
      logger.error(String.format("Error while getting file Section for path %s", this.copyIntoTableContext.getProvidedStorageLocation()), e);
      throw UserException.ioExceptionError(e).buildSilently();
    }

    if(!fileSelectionOptional.isPresent()) {
      logger.error(String.format("Unable to get File selection for %s .", sourceLocationNSKey));
    }

    this.datasetPath = Path.of(fileSelectionOptional.orElseThrow(
        () -> UserException.invalidMetadataError().message("Path %s not found.", this.copyIntoTableContext.getProvidedStorageLocation()).buildSilently())
      .getSelectionRoot());
  }

  private static FileFormat getFileFormat(FileType fileType,
                                   Map<CopyIntoTableContext.FormatOption, Object> formatOptions) {
    switch (fileType) {
      case TEXT:
        TextFileConfig textFileConfig = new TextFileConfig();
        textFileConfig.setCtime(1L);
        textFileConfig.setExtractHeader(true);
        for (Map.Entry<CopyIntoTableContext.FormatOption, Object> option : formatOptions.entrySet()) {
          switch (option.getKey()) {
            case RECORD_DELIMITER:
              textFileConfig.setLineDelimiter((String)option.getValue());
              break;
            case FIELD_DELIMITER:
              textFileConfig.setFieldDelimiter((String)option.getValue());
              break;
            case QUOTE_CHAR:
              textFileConfig.setQuote((String)option.getValue());
              break;
            case ESCAPE_CHAR:
              textFileConfig.setEscape((String)option.getValue());
              break;
          }
        }
        return textFileConfig;
      case JSON:
        return JsonFileConfig.getDefaultInstance();
      default:
        throw UserException.parseError()
          .message("Specified File type '%s' is not supported", fileType)
          .buildSilently();
    }
  }

  /***
   *    EasyScanTableFunction
   *        |
   *        |
   *    HashToRandomExchange
   *        |
   *        |
   *    EasySplitGenFunction
   *        |
   *        |
   *    Filter
   *        |
   *        |
   *    DataFileListingPrel
   *
   */
  public Prel buildPlan() {
    // dir listing
    final Prel dirListingPrel = getDataFileListingPrel();
    Prel childPrel = dirListingPrel;
    if(copyIntoTableContext.getFilePattern().isPresent()) {
      if(logger.isDebugEnabled()) {
        logger.debug("'COPY INTO' Regex : {}", copyIntoTableContext.getFilePattern().get());
      }
      childPrel = addRegexFilter(cluster.getRexBuilder(), dirListingPrel, copyIntoTableContext.getFilePattern().get());
    }

    // split
    TableFunctionConfig easySplitGenTableFunctionConfig = TableFunctionUtil.getEasySplitGenFunctionConfig(tableMetadata, null);
    TableFunctionPrel easySplitGenTableFunction = new TableFunctionPrel(cluster,
      traitSet.plus(DistributionTrait.ANY),
      targetTable,
      childPrel,
      tableMetadata,
      easySplitGenTableFunctionConfig,
      TableFunctionUtil.getSplitRowType(cluster),
      rm -> rm.getRowCount(dirListingPrel));

    // exchange
    final Prel hashToRandomExchange = getHashToRandomExchangePrel(easySplitGenTableFunction);

    TableFunctionConfig easyScanTableFunctionConfig = TableFunctionUtil.getEasyScanTableFunctionConfig(
      tableMetadata,
      null,
      tableMetadata.getSchema(),
      tableMetadata.getSchema().getFields().stream().map(f -> f.getName()).map(SchemaPath::getSimplePath).collect(Collectors.toList()),
      format,
      extendedFormatOptions, storagePluginId);

    setupEasyScanParallelism(easyScanTableFunctionConfig);

    // table scan phase
    return new EasyScanTableFunctionPrel(cluster,
      traitSet.plus(DistributionTrait.ANY),
      targetTable,
      hashToRandomExchange,
      tableMetadata,
      easyScanTableFunctionConfig,
      targetTable.getRowType(),
      DEFAULT_EASY_SCAN_ROW_COUNT_ESTIMATE);
  }

  @VisibleForTesting
  public static long getFileCountEstimate(List<String> files) {
    long fileCount = DEFAULT_FILE_COUNT_ESTIMATE;
    if (files !=null && files.size() > 0) {
      fileCount = files.size();
    }
    return fileCount;
  }

  private void setupEasyScanParallelism(TableFunctionConfig easyScanTableFunctionConfig) {
    long fileCount = getFileCountEstimate(this.copyIntoTableContext.getFiles());
    easyScanTableFunctionConfig.setMinWidth(1);
    easyScanTableFunctionConfig.setMaxWidth(fileCount);
  }

  private void checkAndUpdateIsFileDataset() {
    try {
      this.isFileDataset = this.plugin.createFS(datasetPath.toString(), userName, null).isFile(datasetPath);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Failed to parse datasetPath to File. %s", datasetPath.toString()), e);
    }
  }

  private DatasetConfig setupDatasetConfig() {
    DatasetConfig datasetConf = AbstractRefreshPlanBuilder.setupDatasetConfig(null, sourceLocationNSKey);
    datasetConf.getPhysicalDataset().setFormatSettings(format);
    checkAndUpdateIsFileDataset();
    DatasetType datasetType = isFileDataset ? DatasetType.PHYSICAL_DATASET_SOURCE_FILE: DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
    datasetConf.setType(datasetType);
    // todo: set the value based on the format
    datasetConf.getReadDefinition().getScanStats().setScanFactor(ScanCostFactor.EASY.getFactor());
    return datasetConf;
  }

  private double getDirListingPrelRowCountEstimates() {
    return getFileCountEstimate(this.copyIntoTableContext.getFiles());
  }

  public Prel getDataFileListingPrel() {
    DatasetConfig datasetConfig = setupDatasetConfig();
    List<String> files = this.copyIntoTableContext.getFiles();
    PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
    List<PartitionValue> partitionValue = Collections.emptyList();
    if( files != null && files.size() > 0 ) {
      addFilesToPartitionChunkListing(files, partitionChunkListing);
    } else {
      DirListInputSplitProto.DirListInputSplit dirListInputSplit = DirListInputSplitProto.DirListInputSplit.newBuilder()
        .setRootPath(datasetPath.toString())
        .setOperatingPath(datasetPath.toString())
        .setReadSignature(Long.MAX_VALUE)
        .setIsFile(isFileDataset)
        .setHasVersion(false)
        .build();
      DatasetSplit split = DatasetSplit.of(Collections.emptyList(), 1, 1, dirListInputSplit::writeTo);
      partitionChunkListing.put(partitionValue, split);
    }
    if(logger.isDebugEnabled()) {
      logger.debug("'COPY INTO' DirListing Path : {}", datasetPath.toString());
    }
    partitionChunkListing.computePartitionChunks();
    SplitsPointer splitsPointer = MaterializedSplitsPointer.of(0, AbstractRefreshPlanBuilder.convertToPartitionChunkMetadata(partitionChunkListing, datasetConfig), 1);
    RefreshExecTableMetadata refreshExecTableMetadata = new RefreshExecTableMetadata(storagePluginId, datasetConfig, userName, splitsPointer, BatchSchema.EMPTY, null);

    return new DirListingScanPrel(
      cluster, traitSet, targetTable, storagePluginId, refreshExecTableMetadata, 1.0,
      ImmutableList.of(), true, x -> getDirListingPrelRowCountEstimates(), ImmutableList.of());
  }

  private void addFilesToPartitionChunkListing(List<String> files, PartitionChunkListingImpl partitionChunkListing) {
    List<PartitionValue> partitionValue = Collections.emptyList();
    DirListInputSplitProto.DirListInputSplit dirListInputSplit = DirListInputSplitProto.DirListInputSplit.newBuilder()
      .setRootPath(datasetPath.toString())
      .setOperatingPath(datasetPath.toString())
      .setReadSignature(Long.MAX_VALUE)
      .setIsFile(false)
      .setHasVersion(false)
      .addAllFiles(files)
      .setHasFiles(true)
      .build();
    DatasetSplit split = DatasetSplit.of(Collections.emptyList(), 1, 1, dirListInputSplit::writeTo);
    partitionChunkListing.put(partitionValue, split);
    if (logger.isDebugEnabled()) {
      logger.debug("'COPY INTO' Total Files provided in query : {}", files.size());
    }
  }

  public Prel getHashToRandomExchangePrel(Prel child) {
    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);

    return new HashToRandomExchangePrel(cluster, relTraitSet,
      child, distributionTrait.getFields(), TableFunctionUtil.getHashExchangeTableFunctionCreator(tableMetadata, true, storagePluginId));
  }

  private Prel addRegexFilter(RexBuilder rexBuilder, Prel dirListingChildPrel, String regexString) {

    final SqlFunction regexpMatches = new SqlFunction(
            "REGEXP_MATCHES",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.STRING);

    final SqlFunction strPos = new SqlFunction(
            "STRPOS",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.INTEGER),
            null,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.STRING
    );

    final SqlFunction substring = new SqlFunction(
            "SUBSTRING",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.VARCHAR),
            null,
            OperandTypes.STRING_INTEGER,
            SqlFunctionCategory.STRING
    );

    final SqlFunction add = new SqlFunction(
            "ADD",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.INTEGER),
            null,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.NUMERIC
    );

    /**
     * logic here is to first get starting position of dataset path in full file path.
     * then calculate ending position of first occurrence dataset path in full file path.
     * Get substring after ending position which is relative path to storage location.
     * and finally apply regex match on it.
     */
    String storageLocation = datasetPath.toString() + "/";
    Pair<Integer, RelDataTypeField> fieldWithIndex = MoreRelOptUtil.findFieldWithIndex(dirListingChildPrel.getRowType().getFieldList(), MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    RexNode startAt = rexBuilder.makeCall(strPos, rexBuilder.makeInputRef(fieldWithIndex.right.getType(), fieldWithIndex.left), rexBuilder.makeLiteral(storageLocation));
    RexNode endsAt = rexBuilder.makeCall(add, startAt,  rexBuilder.makeLiteral(String.valueOf(storageLocation.length())));
    RexNode remainingPart = rexBuilder.makeCall(substring, rexBuilder.makeInputRef(fieldWithIndex.right.getType(), fieldWithIndex.left) ,endsAt);
    RexNode regexCondition = rexBuilder.makeCall(regexpMatches, remainingPart, rexBuilder.makeLiteral(regexString));

    return FilterPrel.create(dirListingChildPrel.getCluster(), dirListingChildPrel.getTraitSet(), dirListingChildPrel, regexCondition);
  }
}
