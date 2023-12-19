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
import static com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.CopyOption.ON_ERROR;
import static com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.OnErrorAction.CONTINUE;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
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
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.ExtendedProperties;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
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
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePluginConfig;
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
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

public abstract class CopyIntoTablePlanBuilderBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopyIntoTablePlanBuilderBase.class);
  public static long DEFAULT_FILE_COUNT_ESTIMATE = 1000_000L;
  public static long DEFAULT_EASY_SCAN_ROW_COUNT_ESTIMATE = 1_000_000_000L;

  protected final RelOptTable targetTable;
  protected final RelDataType rowType;
  protected final BatchSchema targetTableSchema;
  private final NamespaceKey sourceLocationNSKey;
  protected final RelOptCluster cluster;
  protected final RelTraitSet traitSet;
  protected final StoragePluginId storagePluginId;
  protected final TableMetadata tableMetadata;
  protected final CopyIntoTableContext copyIntoTableContext;
  private final String userName;
  private Path datasetPath;
  private final SupportsInternalIcebergTable plugin;
  private boolean isFileDataset = false;
  protected final FileConfig format;

  protected final ExtendedFormatOptions extendedFormatOptions;
  protected final CopyIntoQueryProperties queryProperties;
  protected final SimpleQueryContext queryContext;

  /***
   * Expand plans for 'COPY INTO' command
   */
  public CopyIntoTablePlanBuilderBase(RelOptTable targetTable, RelDataType rowType, RelOptCluster cluster, RelTraitSet traitSet,
                                  TableMetadata tableMetadata, OptimizerRulesContext context, CopyIntoTableContext copyIntoTableContext) {
    this.targetTable = Preconditions.checkNotNull(targetTable);
    this.rowType = Preconditions.checkNotNull(rowType);
    this.tableMetadata = Preconditions.checkNotNull(tableMetadata, "TableMetadata cannot be null.");
    this.targetTableSchema = getTargetTableSchema(rowType, tableMetadata.getSchema(), copyIntoTableContext);
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.copyIntoTableContext = copyIntoTableContext;
    this.userName = tableMetadata.getUser();
    this.queryContext = getQueryContext(context);

    final Catalog catalog = context.getCatalogService().getCatalog(MetadataRequestOptions.of(
      SchemaConfig.newBuilder(CatalogUser.from(userName)).build()));
    sourceLocationNSKey = catalog.resolveSingle(new NamespaceKey(parseFullPath(this.copyIntoTableContext.getStorageLocation())));
    ManagedStoragePlugin storagePlugin = context.getCatalogService().getManagedSource(sourceLocationNSKey.getRoot());
    if (storagePlugin == null) {
      throw UserException.invalidMetadataError().message("Source '%s' not found.", sourceLocationNSKey.getRoot()).buildSilently();
    }
    this.storagePluginId = storagePlugin.getId();
    if (storagePluginId == null) {
      throw UserException.invalidMetadataError().message("Source '%s' not found.", sourceLocationNSKey.getRoot()).buildSilently();
    }

    this.plugin = AbstractRefreshPlanBuilder.getPlugin(catalog, sourceLocationNSKey);
    if(!(this.plugin instanceof FileSystemPlugin)) {
      throw UserException.invalidMetadataError().message("Source provided %s is not valid object storage.", this.copyIntoTableContext.getProvidedStorageLocation()).buildSilently();
    }

    setDatasetPath(!copyIntoTableContext.getFiles().isEmpty() || copyIntoTableContext.getFilePattern().isPresent());
    this.format = getFileFormat(copyIntoTableContext.getFileFormat(), copyIntoTableContext.getFormatOptions()).asFileConfig();
    this.extendedFormatOptions = getExtendedFormatOptions(copyIntoTableContext.getFormatOptions());
    if(logger.isDebugEnabled()) {
      logger.debug("'COPY INTO' Options : {}, File Format : {}", extendedFormatOptions, format.toString());
    }
    this.queryProperties = getQueryProperties(copyIntoTableContext.getCopyOptions());
    createSystemIcebergTablesIfNotExists(catalog.getSource(SystemIcebergTablesStoragePluginConfig.SYSTEM_ICEBERG_TABLES_PLUGIN_NAME));
  }

  /**
   * To establish the target table schema, take into account the copy into options. If the copy options include an
   * ON_ERROR 'continue' pair, the schema needs to be augmented with an additional "error" column. In such cases, the
   * input parameter {@code rowType} already contains the enhanced schema, which is then translated into a
   * {@link BatchSchema} object. The preparation of {@code rowType} object is handled by
   * {@link SqlCopyIntoTable#extendTableWithDataFileSystemColumns()}
   * @param rowType definition of a row
   * @param originalSchema schema of the target table
   * @param context copy into context that can be used to fetch the copy options
   * @return the decorated schema, always non-null
   */
  private BatchSchema getTargetTableSchema(RelDataType rowType, BatchSchema originalSchema,
                                           CopyIntoTableContext context) {
    if (isOnErrorContinue(context) && rowType.getFieldCount() > originalSchema.getFieldCount()) {
      SchemaBuilder schemaBuilder = BatchSchema.newBuilder().addFields(originalSchema.getFields());
      IntStream.range(originalSchema.getFieldCount(), rowType.getFieldCount())
        .forEach(i -> schemaBuilder.addField(Field.nullable(rowType.getFieldList().get(i).getName(), ArrowType.Utf8.INSTANCE)));
      return schemaBuilder.build();
    }
    return originalSchema;
  }

  /**
   * Utility method to check copy options for ON_ERROR
   * @param context copy into context that can be used to fetch the copy options
   * @return true, if copy options contains ON_ERROR 'continue'
   */
  private boolean isOnErrorContinue(CopyIntoTableContext context) {
    return context.getCopyOptions().entrySet().stream().anyMatch(e -> ON_ERROR == e.getKey() && CONTINUE == e.getValue());
  }

  /**
   * Retrieves the {@link CopyIntoQueryProperties} based on the provided copy options and the context of the copy operation.
   *
   * @param copyOptions A map of copy options and their corresponding values.
   * @return The {@link CopyIntoQueryProperties} object populated with the relevant information.
   * @throws UserException If an unsupported copy option is encountered.
   */
  private CopyIntoQueryProperties getQueryProperties(Map<CopyIntoTableContext.CopyOption, Object> copyOptions) {
    CopyIntoQueryProperties properties = new CopyIntoQueryProperties();
    properties.setStorageLocation(copyIntoTableContext.getStorageLocation());
    if (!copyOptions.isEmpty()) {
      for (Map.Entry<CopyIntoTableContext.CopyOption, Object> option : copyOptions.entrySet()) {
        if (option.getKey() == ON_ERROR) {
          properties.setOnErrorOption(CopyIntoQueryProperties.OnErrorOption.valueOf(((CopyIntoTableContext.OnErrorAction) option.getValue()).name()));
        } else {
          throw UserException.invalidMetadataError()
            .message("COPY INTO option %s is not supported", option.getKey().name()).buildSilently();
        }
      }
      return properties;
    }

    properties.setOnErrorOption(CopyIntoQueryProperties.OnErrorOption.ABORT);
    return properties;
  }

  /**
   * Prepare a query context object containing the query user and queryId.
   * @param context rule context, must be not null
   * @return an instance of query context, always non-null
   */
  private SimpleQueryContext getQueryContext(OptimizerRulesContext context) {
    return new SimpleQueryContext(userName, QueryIdHelper.getQueryId(((QueryContext) context).getQueryId()),
      tableMetadata.getName().toString());
  }

  /**
   * Construct an {@link ExtendedProperties} object and serialize it to {@link ByteString}. The properties object
   * works as a wrapper around additional reader properties like
   * <ul>
   *   <li>{@link CopyIntoQueryProperties}</li>
   *   <li>{@link SimpleQueryContext}</li>
   * </ul>
   */
  protected ByteString getExtendedProperties() {
    ExtendedProperties properties = new ExtendedProperties();
    properties.setProperty(ExtendedProperties.PropertyKey.QUERY_CONTEXT, queryContext);
    properties.setProperty(ExtendedProperties.PropertyKey.COPY_INTO_QUERY_PROPERTIES, queryProperties);
    return ExtendedProperties.Util.getByteString(properties);
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
        case EXTRACT_HEADER:
        case SKIP_LINES:
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

  private void setDatasetPath(boolean filesOrRegexPresent) {
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

    if (!fileSelectionOptional.get().isRootPathDirectory() && filesOrRegexPresent) {
      throw UserException.parseError()
        .message(String.format("When specifying 'FILES' or 'REGEX' location_clause must end with a directory. " +
          "Found a file: %s", fileSelectionOptional.get().getSelectionRoot())).buildSilently();
    }
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
            case EXTRACT_HEADER:
              textFileConfig.setExtractHeader((Boolean) option.getValue());
              break;
            case SKIP_LINES:
              textFileConfig.setSkipLines((Integer) option.getValue());
              break;
          }
        }
        return textFileConfig;
      case JSON:
        return JsonFileConfig.getDefaultInstance();
      case PARQUET:
        return ParquetFileConfig.getDefaultInstance();
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
    // dir listing with filtering if regex is defined
    final Prel dirListingPrel = buildDirListingPrel();

    // split
    TableFunctionPrel splitGenTableFunction = buildSplitGenTableFunction(dirListingPrel);

    // exchange
    final Prel hashToRandomExchange = getHashToRandomExchangePrel(splitGenTableFunction);

    // scan table function
    return getScanTableFunction(hashToRandomExchange);
  }

  /**
   * Create a dir listing {@link Prel} with optionally attached filter stage if a
   * REGEX was supplied for COPY INTO.
   *
   * @return dir listing with optional regex-based filtering of files
   */
  private Prel buildDirListingPrel() {
    final Prel dirListingPrel = getDataFileListingPrel();
    return copyIntoTableContext.getFilePattern().map(pattern ->
      addRegexFilter(cluster.getRexBuilder(), dirListingPrel, pattern)
    ).orElse(dirListingPrel);
  }

  /**
   * Create a split gen table function {@link TableFunctionPrel} that accepts input
   * from the provided dir listing {@link Prel}.
   *
   * The function type of the table function depends on the file type configured for
   * the COPY INTO command. For text formats, the function will produce one split per file,
   * for parquet we support intra-file splits via
   * {@link com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetBlockBasedSplitXAttr}.
   *
   * @param dirListingPrel dir listing predecessor operator
   * @return {@link TableFunctionPrel} representing a physical plan executing a dir listing
   *    followed by split generation.
   */
  private TableFunctionPrel buildSplitGenTableFunction(Prel dirListingPrel) {
    TableFunctionContext tableFunctionContext = TableFunctionUtil.getSplitProducerTableFunctionContext(
      tableMetadata, null, false);
    TableFunctionConfig.FunctionType functionType = format.getType() == FileType.PARQUET
      ? TableFunctionConfig.FunctionType.DIR_LISTING_SPLIT_GENERATION
      : TableFunctionConfig.FunctionType.EASY_SPLIT_GENERATION;
    TableFunctionConfig tableFunctionConfig =
      new TableFunctionConfig(functionType, true, tableFunctionContext);
    return new TableFunctionPrel(cluster,
      traitSet.plus(DistributionTrait.ANY),
      targetTable,
      dirListingPrel,
      tableMetadata,
      tableFunctionConfig,
      TableFunctionUtil.getSplitRowType(cluster),
      rm -> rm.getRowCount(dirListingPrel));
  }

  /**
   * Create a list of schema paths from the given schema.
   *
   * @param schema input schema
   * @return list of schema paths contained in the provided schema
   */
  protected static List<SchemaPath> getSchemaPaths(BatchSchema schema) {
    return schema.getFields().stream()
      .map(Field::getName).map(SchemaPath::getSimplePath)
      .collect(Collectors.toList());
  }

  /**
   * Create a {@link com.dremio.exec.store.parquet.ScanTableFunction} matching the
   * COPY INTO file format
   * @param hashToRandomExchange predecessor {@link Prel} handling the random exchange of generated splits.
   * @return scan table function for parquet or easy depending on the file type.
   */
  private Prel getScanTableFunction(Prel hashToRandomExchange) {
    if (format.getType() == FileType.PARQUET) {
      TableFunctionContext scanTableFunctionContext = TableFunctionUtil.getDataFileScanTableFunctionContextForCopyInto(
        format,
        tableMetadata.getSchema(),
        sourceLocationNSKey,
        storagePluginId,
        getSchemaPaths(targetTableSchema)
      );

      TableFunctionConfig scanTableFunctionConfig =
        TableFunctionUtil.getDataFileScanTableFunctionConfig(
          scanTableFunctionContext, false, 1);

      return new TableFunctionPrel(cluster,
        traitSet.plus(DistributionTrait.ANY),
        targetTable,
        hashToRandomExchange,
        tableMetadata,
        scanTableFunctionConfig,
        rowType,
        DEFAULT_EASY_SCAN_ROW_COUNT_ESTIMATE);
    } else {
      return buildEasyScanTableFunctionPrel(hashToRandomExchange);
    }
  }

  /**
   * Provides the bottom operator by building the scan table function relation
   * @param hashToRandomExchange exchange relation
   * @return
   */
  protected abstract Prel buildEasyScanTableFunctionPrel(Prel hashToRandomExchange);


  @VisibleForTesting
  public static long getFileCountEstimate(List<String> files) {
    long fileCount = DEFAULT_FILE_COUNT_ESTIMATE;
    if (files !=null && files.size() > 0) {
      fileCount = files.size();
    }
    return fileCount;
  }

  protected void setupEasyScanParallelism(TableFunctionConfig easyScanTableFunctionConfig) {
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

  /**
   * Creates a dataset config for the copy into data source, configuring the source format,
   * scan cost, and dataset type (file or directory).
   *
   * @return {@link DatasetConfig} dataset config for the data source.
   */
  private DatasetConfig setupDatasetConfig() {
    DatasetConfig datasetConf = AbstractRefreshPlanBuilder.setupDatasetConfig(null, sourceLocationNSKey);
    datasetConf.getPhysicalDataset().setFormatSettings(format);
    checkAndUpdateIsFileDataset();
    DatasetType datasetType = isFileDataset ? DatasetType.PHYSICAL_DATASET_SOURCE_FILE: DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
    datasetConf.setType(datasetType);
    if (format.getType() == FileType.PARQUET) {
      datasetConf.getReadDefinition().getScanStats().setScanFactor(ScanCostFactor.PARQUET.getFactor());
    } else {
      datasetConf.getReadDefinition().getScanStats().setScanFactor(ScanCostFactor.EASY.getFactor());
    }
    return datasetConf;
  }

  private double getDirListingPrelRowCountEstimates() {
    return getFileCountEstimate(this.copyIntoTableContext.getFiles());
  }

  /**
   * Transforms the list of rejected file paths that are full absolute paths to begin with, into relative paths to the
   * given storage location.
   * e.g.: if a source 's3Source' is defined as s3:/bucket.name and the storage location for the copy into command was
   * defined as s3Source.folder, the relative path of dremioS3:/bucket.name/folder/subfolder/subfile.csv
   * shall be: /subfolder/subfile.csv
   *
   * @param fullPathList full path list of files with rejections
   * @param storageLocation storage location defined in the original copy into command
   * @param storageLocationPath resolved absolute path of storageLocation
   * @return corresponding list of relative paths
   */
  @VisibleForTesting
  public static List<String> constructRelativeRejectedFilePathes(List<String> fullPathList, String storageLocation,
      Path storageLocationPath) {
    return fullPathList.stream().map(f -> {

      // Remove scheme part from storage location path if needed
      URI storageLocationURI = storageLocationPath.toURI();
      List<String> storageLocationComponents = PathUtils.toPathComponents(storageLocationURI.getPath());

      // Remove scheme part from file paths if needed
      List<String> fullFilePathComponents = null;
      try {
        URI fullFileURI = new URI(f);
        fullFilePathComponents = PathUtils.toPathComponents(fullFileURI.getPath());
      } catch (URISyntaxException e) {
        throw UserException.planError(e).message(String.format("Malformed file path from copy_errors table: %s", f)).buildSilently();
      }

      List<String> expectedRoot = fullFilePathComponents.subList(0, storageLocationComponents.size());

      if (!storageLocationComponents.equals(expectedRoot)) {
        throw UserException.planError().message(String.format("Found a source file with rejection that has a different " +
            "path compared to root of the provided source path. File: %s, source path: %s (mounted as %s)",
            f, storageLocation, storageLocationPath.toString())).buildSilently();
      }

      return PathUtils.toFSPathString(fullFilePathComponents.subList(storageLocationComponents.size(), fullFilePathComponents.size()));
    }).collect(toList());
  }

  public Prel getDataFileListingPrel() {
    DatasetConfig datasetConfig = setupDatasetConfig();
    List<String> files = copyIntoTableContext.getFiles();
    if (copyIntoTableContext.isValidationMode()) {
      files = constructRelativeRejectedFilePathes(files, copyIntoTableContext.getStorageLocation(), datasetPath);
    }
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
      child, distributionTrait.getFields(), TableFunctionUtil.getHashExchangeTableFunctionCreator(tableMetadata, targetTableSchema, true, storagePluginId));
  }

  protected Prel addRegexFilter(RexBuilder rexBuilder, Prel dirListingChildPrel, String regexString) {

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

  /**
   * Creates system Iceberg tables if they do not already exist based on the provided
   * {@link SystemIcebergTablesStoragePlugin} instance.
   * The method checks if the processing context indicates that the operation should continue
   * on error before attempting to create the system tables.
   *
   * @see #isOnErrorContinue(CopyIntoTableContext)
   * @see SystemIcebergTablesStoragePlugin#createEmptySystemIcebergTablesIfNotExists()
   */
  private void createSystemIcebergTablesIfNotExists(SystemIcebergTablesStoragePlugin plugin) {
    if (isOnErrorContinue(copyIntoTableContext)) {
      plugin.createEmptySystemIcebergTablesIfNotExists();
    }
  }
}
