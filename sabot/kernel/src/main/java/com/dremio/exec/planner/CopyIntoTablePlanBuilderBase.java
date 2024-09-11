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
import static com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.OnErrorAction.SKIP_FILE;
import static com.dremio.exec.store.metadatarefresh.dirlisting.DirListingUtils.addFileNameRegexFilter;
import static java.util.stream.Collectors.toList;

import com.dremio.catalog.exception.SourceDoesNotExistException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties.PropertyKey;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.SimpleQueryContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoQueryProperties;
import com.dremio.exec.physical.config.copyinto.CopyIntoTransformationProperties;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePluginConfig;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.metadatarefresh.RefreshExecTableMetadata;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingScanPrel;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionValue;
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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;

public abstract class CopyIntoTablePlanBuilderBase {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CopyIntoTablePlanBuilderBase.class);
  public static long DEFAULT_FILE_COUNT_ESTIMATE = 1000_000L;
  public static long DEFAULT_EASY_SCAN_ROW_COUNT_ESTIMATE = 1_000_000_000L;

  protected final RelOptTable targetTable;
  protected final RelDataType rowType;
  protected final BatchSchema targetTableSchema;
  protected final NamespaceKey sourceLocationNSKey;
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
  protected final CopyIntoTransformationProperties transformationProperties;

  /***
   * Expand plans for 'COPY INTO' command
   */
  public CopyIntoTablePlanBuilderBase(
      RelOptTable targetTable,
      RelNode relNode,
      RelDataType rowType,
      RelDataType transformationsRowType,
      RelOptCluster cluster,
      RelTraitSet traitSet,
      TableMetadata tableMetadata,
      OptimizerRulesContext context,
      CopyIntoTableContext copyIntoTableContext) {
    this.targetTable = Preconditions.checkNotNull(targetTable);
    this.rowType = Preconditions.checkNotNull(rowType);
    this.tableMetadata = Preconditions.checkNotNull(tableMetadata, "TableMetadata cannot be null.");
    this.targetTableSchema =
        getTargetTableSchema(rowType, tableMetadata.getSchema(), copyIntoTableContext);
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.copyIntoTableContext = copyIntoTableContext;
    this.userName = tableMetadata.getUser();
    this.queryContext = getQueryContext(context);
    final Catalog catalog =
        context
            .getCatalogService()
            .getCatalog(
                MetadataRequestOptions.of(
                    SchemaConfig.newBuilder(CatalogUser.from(userName)).build()));
    sourceLocationNSKey =
        catalog.resolveSingle(
            new NamespaceKey(parseFullPath(this.copyIntoTableContext.getStorageLocation())));
    ManagedStoragePlugin storagePlugin =
        context.getCatalogService().getManagedSource(sourceLocationNSKey.getRoot());
    if (storagePlugin == null) {
      throw getSourceNotFoundException(sourceLocationNSKey.getRoot());
    }
    this.storagePluginId = storagePlugin.getId();
    if (storagePluginId == null) {
      throw getSourceNotFoundException(sourceLocationNSKey.getRoot());
    }

    this.plugin = AbstractRefreshPlanBuilder.getPlugin(catalog, sourceLocationNSKey);
    if (!(this.plugin instanceof FileSystemPlugin)) {
      throw UserException.invalidMetadataError()
          .message(
              "Source provided %s is not valid object storage.",
              this.copyIntoTableContext.getProvidedStorageLocation())
          .buildSilently();
    }

    setDatasetPath(
        !copyIntoTableContext.getFiles().isEmpty()
            || copyIntoTableContext.getFilePattern().isPresent());
    validateAccessOnSource(catalog, copyIntoTableContext);
    this.format =
        getFileFormat(copyIntoTableContext.getFileFormat(), copyIntoTableContext.getFormatOptions())
            .asFileConfig();
    this.extendedFormatOptions = getExtendedFormatOptions(copyIntoTableContext.getFormatOptions());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "'COPY INTO' Options : {}, File Format : {}", extendedFormatOptions, format.toString());
    }
    this.queryProperties = getQueryProperties(copyIntoTableContext.getCopyOptions());
    createSystemIcebergTablesIfNotExists(
        catalog.getSource(
            SystemIcebergTablesStoragePluginConfig.SYSTEM_ICEBERG_TABLES_PLUGIN_NAME));

    if (isSkipFile()) {
      // SKIP mode scans files twice. By default Parquet footer is mutated to be trimmed
      // by RGs that were read once. Disabling trimming to avoid issues with the 2nd scan.
      ((QueryContext) context)
          .getQueryOptionManager()
          .setOption(
              OptionValue.createBoolean(
                  OptionValue.OptionType.QUERY,
                  ExecConstants.TRIM_ROWGROUPS_FROM_FOOTER.getOptionName(),
                  false));
    }

    if (FileType.PARQUET == format.getType()) {
      ((QueryContext) context)
          .getQueryOptionManager()
          .setOption(
              OptionValue.createBoolean(
                  OptionValue.OptionType.QUERY,
                  ExecConstants.ENABLE_USING_C3_CACHE.getOptionName(),
                  false));
    }

    this.transformationProperties =
        getTransformationProperties(
            context,
            relNode,
            transformationsRowType,
            copyIntoTableContext.getTransformationColNames(),
            copyIntoTableContext.getMappings(),
            cluster.getRexBuilder());
  }

  /**
   * Retrieves the transformation properties for the "Copy Into" operation. This method prepares the
   * transformation expressions and schema based on the provided {@link RelNode} and {@link
   * RelDataType} if the transformations are enabled in the context options.
   *
   * @param context The optimizer rules context containing planner settings and options.
   * @param relNode The relational node representing the logical plan.
   * @param transformationRowType The row type for the transformations.
   * @param transformationColNames The name of the columns involved in transformations
   * @param mappings The list of mappings for the transformations.
   * @param rexBuilder The RexBuilder used to construct RexNodes.
   * @return An instance of {@link CopyIntoTransformationProperties} containing the transformation
   *     expressions and schema.
   * @throws UserException If transformations are not supported and the row type is provided.
   */
  private CopyIntoTransformationProperties getTransformationProperties(
      OptimizerRulesContext context,
      RelNode relNode,
      RelDataType transformationRowType,
      List<String> transformationColNames,
      List<String> mappings,
      RexBuilder rexBuilder) {
    if (transformationRowType == null || !(relNode instanceof LogicalProject)) {
      return null;
    }

    if (!context.getOptions().getOption(ExecConstants.COPY_INTO_ENABLE_TRANSFORMATIONS)) {
      throw UserException.unsupportedError()
          .message("Copy Into with transformations is not supported")
          .buildSilently();
    }

    // validate column mappings
    if (!targetTableSchema.getFields().stream()
        .map(field -> field.getName().toLowerCase())
        .collect(toList())
        .containsAll(mappings)) {
      throw UserException.parseError()
          .message(
              "Incorrect transformation mapping definition. Some column names are not found in target table schema.")
          .buildSilently();
    }
    // convert relation expressions to logical expressions
    List<LogicalExpression> logicalExpressions =
        ((LogicalProject) relNode)
            .getChildExps().stream()
                .map(
                    rexNode ->
                        RexToExpr.toExpr(
                            new ParseContext(context.getPlannerSettings()),
                            transformationRowType,
                            rexBuilder,
                            rexNode))
                .collect(toList());

    // build a batchschema using the transformation rowtype
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    transformationRowType.getFieldList().stream()
        .map(field -> field.getName().toLowerCase())
        .filter(transformationColNames::contains)
        .forEach(name -> schemaBuilder.addField(Field.nullable(name, Utf8.INSTANCE)));

    return new CopyIntoTransformationProperties(
        logicalExpressions, mappings, schemaBuilder.build());
  }

  private UserException getSourceNotFoundException(final String sourceName) {
    final Throwable cause = new SourceDoesNotExistException(sourceName);

    return UserException.validationError(cause).message(cause.getMessage()).buildSilently();
  }

  /**
   * Validates the user's access privileges on the source storage location based on the provided
   * {@link CopyIntoTableContext}. If the context contains a list of files, it validates select
   * privileges on each source file individually. If there is no files clause, it validates the
   * source storage directory for select privileges.
   *
   * @param catalog The catalog used for privilege validation.
   * @param context The {@link CopyIntoTableContext} containing information about the COPY INTO
   *     operation.
   * @throws UserException If the user lacks the necessary privileges to execute the COPY INTO
   *     command. The exception provides a validation error message.
   */
  private void validateAccessOnSource(Catalog catalog, CopyIntoTableContext context) {
    try {
      if (context.getFiles() == null || context.getFiles().isEmpty()) {
        // if there is no files clause we are validating the source storage directory for select
        // privileges
        catalog.validatePrivilege(sourceLocationNSKey, SqlGrant.Privilege.SELECT);
      } else {
        // there is a files clause. Validate select privileges on each source file
        List<String> storageLocationPathComponents =
            PathUtils.toPathComponents(context.getProvidedStorageLocation().substring(1));
        if (context.getFileNameFromStorageLocation() != null) {
          catalog.validatePrivilege(
              new NamespaceKey(storageLocationPathComponents), SqlGrant.Privilege.SELECT);
        } else {
          context.getFiles().stream()
              .map(PathUtils::toPathComponents)
              .map(
                  l ->
                      Stream.concat(
                              new ArrayList<>(storageLocationPathComponents).stream(), l.stream())
                          .collect(toList()))
              .map(NamespaceKey::new)
              .forEach(n -> catalog.validatePrivilege(n, SqlGrant.Privilege.SELECT));
        }
      }
    } catch (UserException e) {
      throw UserException.validationError()
          .message(
              "COPY INTO command cannot be executed due to missing privileges. " + e.getMessage())
          .buildSilently();
    }
  }

  /**
   * To establish the target table schema, take into account the copy into options. If the copy
   * options include an ON_ERROR 'continue'/'skip' pair, the schema needs to be augmented with an
   * additional "history" column. In such cases, the input parameter {@code rowType} already
   * contains the enhanced schema, which is then translated into a {@link BatchSchema} object. The
   * preparation of {@code rowType} object is handled by {@link
   * SqlCopyIntoTable#extendTableWithDataFileSystemColumns()}
   *
   * @param rowType definition of a row
   * @param originalSchema schema of the target table
   * @param context copy into context that can be used to fetch the copy options
   * @return the decorated schema, always non-null
   */
  private BatchSchema getTargetTableSchema(
      RelDataType rowType, BatchSchema originalSchema, CopyIntoTableContext context) {
    if (isOnErrorHandlingRequested(context)
        && rowType.getFieldCount() > originalSchema.getFieldCount()) {
      SchemaBuilder schemaBuilder = BatchSchema.newBuilder().addFields(originalSchema.getFields());
      IntStream.range(originalSchema.getFieldCount(), rowType.getFieldCount())
          .forEach(
              i ->
                  schemaBuilder.addField(
                      Field.nullable(
                          rowType.getFieldList().get(i).getName(), ArrowType.Utf8.INSTANCE)));
      return schemaBuilder.build();
    }
    return originalSchema;
  }

  /**
   * Utility method to check copy options for ON_ERROR
   *
   * @param context copy into context that can be used to fetch the copy options
   * @return true, if copy options contains ON_ERROR 'continue' or 'skip_file'
   */
  private boolean isOnErrorHandlingRequested(CopyIntoTableContext context) {
    return context.getCopyOptions().entrySet().stream()
        .anyMatch(
            e -> ON_ERROR == e.getKey() && (CONTINUE == e.getValue() || SKIP_FILE == e.getValue()));
  }

  /**
   * Retrieves the {@link CopyIntoQueryProperties} based on the provided copy options and the
   * context of the copy operation.
   *
   * @param copyOptions A map of copy options and their corresponding values.
   * @return The {@link CopyIntoQueryProperties} object populated with the relevant information.
   * @throws UserException If an unsupported copy option is encountered.
   */
  protected CopyIntoQueryProperties getQueryProperties(
      Map<CopyIntoTableContext.CopyOption, Object> copyOptions) {
    CopyIntoQueryProperties properties = new CopyIntoQueryProperties();
    properties.setStorageLocation(copyIntoTableContext.getStorageLocation());
    properties.setBranch(copyIntoTableContext.getBranch());
    if (!copyOptions.isEmpty()) {
      for (Map.Entry<CopyIntoTableContext.CopyOption, Object> option : copyOptions.entrySet()) {
        if (option.getKey() == ON_ERROR) {
          properties.setOnErrorOption(
              CopyIntoQueryProperties.OnErrorOption.valueOf(
                  ((CopyIntoTableContext.OnErrorAction) option.getValue()).name()));
        } else {
          throw UserException.invalidMetadataError()
              .message("COPY INTO option %s is not supported", option.getKey().name())
              .buildSilently();
        }
      }
      return properties;
    }

    properties.setOnErrorOption(CopyIntoQueryProperties.OnErrorOption.ABORT);
    return properties;
  }

  /**
   * Prepare a query context object containing the query user and queryId.
   *
   * @param context rule context, must be not null
   * @return an instance of query context, always non-null
   */
  private SimpleQueryContext getQueryContext(OptimizerRulesContext context) {
    return new SimpleQueryContext(
        userName,
        QueryIdHelper.getQueryId(((QueryContext) context).getQueryId()),
        tableMetadata.getName().toString());
  }

  /**
   * Construct an {@link CopyIntoExtendedProperties} object and serialize it to {@link ByteString}.
   * The properties object works as a wrapper around additional reader properties like
   *
   * <ul>
   *   <li>{@link CopyIntoQueryProperties}
   *   <li>{@link SimpleQueryContext}
   * </ul>
   */
  protected ByteString getExtendedProperties() {
    CopyIntoExtendedProperties properties = new CopyIntoExtendedProperties();
    properties.setProperty(CopyIntoExtendedProperties.PropertyKey.QUERY_CONTEXT, queryContext);
    properties.setProperty(
        CopyIntoExtendedProperties.PropertyKey.COPY_INTO_QUERY_PROPERTIES, queryProperties);
    properties.setProperty(
        PropertyKey.COPY_INTO_TRANSFORMATION_PROPERTIES, transformationProperties);
    return CopyIntoExtendedProperties.Util.getByteString(properties);
  }

  private ExtendedFormatOptions getExtendedFormatOptions(
      Map<CopyIntoTableContext.FormatOption, Object> formatOptions) {
    ExtendedFormatOptions options = new ExtendedFormatOptions();
    for (Map.Entry<CopyIntoTableContext.FormatOption, Object> option : formatOptions.entrySet()) {
      switch (option.getKey()) {
        case TRIM_SPACE:
          options.setTrimSpace((Boolean) option.getValue());
          break;
        case DATE_FORMAT:
          options.setDateFormat((String) option.getValue());
          break;
        case TIME_FORMAT:
          options.setTimeFormat((String) option.getValue());
          break;
        case EMPTY_AS_NULL:
          options.setEmptyAsNull((Boolean) option.getValue());
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
          // These options are handled in getFileFormat() and we do not need to extract them here.
          // But, break here to avoid "Format option is not supported errors" for these options.
          break;
        default:
          throw UserException.parseError()
              .message("Specified format option '%s' is not supported", option.getKey())
              .buildSilently();
      }
    }

    return options;
  }

  Optional<FileSelection> generateFileSelectionForSourceRoot(
      NamespaceKey datasetPath, String user, FileSystemPlugin plugin) throws IOException {
    final List<String> fullPath =
        plugin.resolveTableNameToValidPath(datasetPath.getPathComponents());
    if (fullPath.isEmpty()) {
      return Optional.empty();
    }
    FileSystem fs = plugin.createFS(user);
    FileSelection fileSelection = FileSelection.createNotExpanded(fs, fullPath);
    return Optional.of(fileSelection);
  }

  private void setDatasetPath(boolean filesOrRegexPresent) {
    FileSystemPlugin fsPlugin = (FileSystemPlugin) plugin;
    Optional<FileSelection> fileSelectionOptional;
    try {
      if (sourceLocationNSKey.size() <= 1) {
        fileSelectionOptional =
            generateFileSelectionForSourceRoot(sourceLocationNSKey, userName, fsPlugin);
      } else {
        fileSelectionOptional =
            fsPlugin.generateFileSelectionForPathComponents(sourceLocationNSKey, userName);
      }
    } catch (IOException e) {
      logger.error(
          String.format(
              "Error while getting file Section for path %s",
              this.copyIntoTableContext.getProvidedStorageLocation()),
          e);
      throw UserException.ioExceptionError(e).buildSilently();
    }

    if (!fileSelectionOptional.isPresent()) {
      logger.error(String.format("Unable to get File selection for %s .", sourceLocationNSKey));
    }

    this.datasetPath =
        Path.of(
            fileSelectionOptional
                .orElseThrow(
                    () -> {
                      final String errorMessage =
                          String.format(
                              "Resource not found at path %s",
                              this.copyIntoTableContext.getProvidedStorageLocation());
                      final EntityPath entityPath =
                          new EntityPath(
                              PathUtils.toPathComponents(
                                  copyIntoTableContext.getProvidedStorageLocation()));

                      return UserException.validationError(new DatasetNotFoundException(entityPath))
                          .message(errorMessage)
                          .buildSilently();
                    })
                .getSelectionRoot());

    if (!fileSelectionOptional.get().isRootPathDirectory() && filesOrRegexPresent) {
      throw UserException.parseError()
          .message(
              String.format(
                  "When specifying 'FILES' or 'REGEX' location_clause must end with a directory. "
                      + "Found a file: %s",
                  fileSelectionOptional.get().getSelectionRoot()))
          .buildSilently();
    }
  }

  private static FileFormat getFileFormat(
      FileType fileType, Map<CopyIntoTableContext.FormatOption, Object> formatOptions) {
    switch (fileType) {
      case TEXT:
        TextFileConfig textFileConfig = new TextFileConfig();
        textFileConfig.setCtime(1L);
        textFileConfig.setExtractHeader(true);
        for (Map.Entry<CopyIntoTableContext.FormatOption, Object> option :
            formatOptions.entrySet()) {
          switch (option.getKey()) {
            case RECORD_DELIMITER:
              textFileConfig.setLineDelimiter((String) option.getValue());
              break;
            case FIELD_DELIMITER:
              textFileConfig.setFieldDelimiter((String) option.getValue());
              break;
            case QUOTE_CHAR:
              textFileConfig.setQuote((String) option.getValue());
              break;
            case ESCAPE_CHAR:
              textFileConfig.setEscape((String) option.getValue());
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
    final Prel dirListingPrel = buildFilePathSupplierPrel();

    // split
    TableFunctionPrel splitGenTableFunction = buildSplitGenTableFunction(dirListingPrel);

    // exchange
    final Prel hashToRandomExchange = getHashToRandomExchangePrel(splitGenTableFunction);

    // scan table function
    return getScanTableFunction(hashToRandomExchange);
  }

  /**
   * Create a dir listing {@link Prel} with optionally attached filter stage if a REGEX was supplied
   * for COPY INTO.
   *
   * @return dir listing with optional regex-based filtering of files
   */
  protected Prel buildFilePathSupplierPrel() {
    final Prel dirListingPrel = getDataFileListingPrel();
    return copyIntoTableContext
        .getFilePattern()
        .map(
            pattern ->
                addFileNameRegexFilter(
                    cluster.getRexBuilder(), dirListingPrel, pattern, datasetPath))
        .orElse(dirListingPrel);
  }

  /**
   * Create a split gen table function {@link TableFunctionPrel} that accepts input from the
   * provided dir listing {@link Prel}.
   *
   * <p>The function type of the table function depends on the file type configured for the COPY
   * INTO command. For text formats, the function will produce one split per file, for parquet we
   * support intra-file splits via {@link
   * com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetBlockBasedSplitXAttr}.
   *
   * @param dirListingPrel dir listing predecessor operator
   * @return {@link TableFunctionPrel} representing a physical plan executing a dir listing followed
   *     by split generation.
   */
  protected TableFunctionPrel buildSplitGenTableFunction(Prel dirListingPrel) {
    boolean oneBlockSplitForWholeParquetFile = isSkipFile();
    TableFunctionContext tableFunctionContext =
        TableFunctionUtil.getSplitProducerTableFunctionContext(
            tableMetadata,
            RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            TableFunctionUtil.getSplitGenSchemaColumns(),
            null,
            false,
            oneBlockSplitForWholeParquetFile);
    TableFunctionConfig.FunctionType functionType =
        format.getType() == FileType.PARQUET
            ? TableFunctionConfig.FunctionType.DIR_LISTING_SPLIT_GENERATION
            : TableFunctionConfig.FunctionType.EASY_SPLIT_GENERATION;
    TableFunctionConfig tableFunctionConfig =
        new TableFunctionConfig(functionType, true, tableFunctionContext);
    return new TableFunctionPrel(
        cluster,
        traitSet.plus(DistributionTrait.ANY),
        targetTable,
        dirListingPrel,
        tableMetadata,
        tableFunctionConfig,
        TableFunctionUtil.getSplitRowType(cluster),
        rm -> rm.getRowCount(dirListingPrel));
  }

  /**
   * Create a {@link com.dremio.exec.store.parquet.ScanTableFunction} matching the COPY INTO file
   * format
   *
   * @param hashToRandomExchange predecessor {@link Prel} handling the random exchange of generated
   *     splits.
   * @return scan table function for parquet or easy depending on the file type.
   */
  private Prel getScanTableFunction(Prel hashToRandomExchange) {
    if (format.getType() == FileType.PARQUET) {
      return buildParquetScanTableFunctionPrel(hashToRandomExchange);
    } else {
      return buildEasyScanTableFunctionPrel(hashToRandomExchange);
    }
  }

  /**
   * Provides the bottom operator by building the scan table function relation for Parquet input
   * files
   *
   * @param hashToRandomExchange exchange relation
   * @return
   */
  protected abstract Prel buildParquetScanTableFunctionPrel(Prel hashToRandomExchange);

  /**
   * Provides the bottom operator by building the scan table function relation for CSV or JSON input
   * files
   *
   * @param hashToRandomExchange exchange relation
   * @return
   */
  protected abstract Prel buildEasyScanTableFunctionPrel(Prel hashToRandomExchange);

  @VisibleForTesting
  public static long getFileCountEstimate(List<String> files) {
    long fileCount = DEFAULT_FILE_COUNT_ESTIMATE;
    if (files != null && files.size() > 0) {
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
      this.isFileDataset =
          this.plugin.createFS(datasetPath.toString(), userName, null).isFile(datasetPath);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("Failed to parse datasetPath to File. %s", datasetPath.toString()), e);
    }
  }

  /**
   * Creates a dataset config for the copy into data source, configuring the source format, scan
   * cost, and dataset type (file or directory).
   *
   * @return {@link DatasetConfig} dataset config for the data source.
   */
  private DatasetConfig setupDatasetConfig() {
    DatasetConfig datasetConf =
        AbstractRefreshPlanBuilder.setupDatasetConfig(null, sourceLocationNSKey);
    datasetConf.getPhysicalDataset().setFormatSettings(format);
    checkAndUpdateIsFileDataset();
    DatasetType datasetType =
        isFileDataset
            ? DatasetType.PHYSICAL_DATASET_SOURCE_FILE
            : DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
    datasetConf.setType(datasetType);
    if (format.getType() == FileType.PARQUET) {
      datasetConf
          .getReadDefinition()
          .getScanStats()
          .setScanFactor(ScanCostFactor.PARQUET.getFactor());
    } else {
      datasetConf.getReadDefinition().getScanStats().setScanFactor(ScanCostFactor.EASY.getFactor());
    }
    return datasetConf;
  }

  private double getDirListingPrelRowCountEstimates() {
    return getFileCountEstimate(this.copyIntoTableContext.getFiles());
  }

  /**
   * Transforms the list of rejected file paths that are full absolute paths to begin with, into
   * relative paths to the given storage location. e.g.: if a source 's3Source' is defined as
   * s3:/bucket.name and the storage location for the copy into command was defined as
   * s3Source.folder, the relative path of dremioS3:/bucket.name/folder/subfolder/subfile.csv shall
   * be: /subfolder/subfile.csv
   *
   * @param fullPathList full path list of files with rejections
   * @param storageLocation storage location defined in the original copy into command
   * @param storageLocationPath resolved absolute path of storageLocation
   * @return corresponding list of relative paths
   */
  @VisibleForTesting
  public static List<String> constructRelativeRejectedFilePathes(
      List<String> fullPathList, String storageLocation, Path storageLocationPath) {
    return fullPathList.stream()
        .map(
            f -> {

              // Remove scheme part from storage location path if needed
              URI storageLocationURI = storageLocationPath.toURI();
              List<String> storageLocationComponents =
                  PathUtils.toPathComponents(storageLocationURI.getPath());

              // Remove scheme part from file paths if needed
              List<String> fullFilePathComponents = null;
              try {
                URI fullFileURI = new URI(f);
                fullFilePathComponents = PathUtils.toPathComponents(fullFileURI.getPath());
              } catch (URISyntaxException e) {
                throw UserException.planError(e)
                    .message(String.format("Malformed file path from copy_errors table: %s", f))
                    .buildSilently();
              }

              List<String> expectedRoot =
                  fullFilePathComponents.subList(0, storageLocationComponents.size());

              if (!storageLocationComponents.equals(expectedRoot)) {
                throw UserException.planError()
                    .message(
                        String.format(
                            "Found a source file with rejection that has a different "
                                + "path compared to root of the provided source path. File: %s, source path: %s (mounted as %s)",
                            f, storageLocation, storageLocationPath.toString()))
                    .buildSilently();
              }

              return PathUtils.toFSPathString(
                  fullFilePathComponents.subList(
                      storageLocationComponents.size(), fullFilePathComponents.size()));
            })
        .collect(toList());
  }

  public Prel getDataFileListingPrel() {
    DatasetConfig datasetConfig = setupDatasetConfig();
    List<String> files = copyIntoTableContext.getFiles();
    if (copyIntoTableContext.isValidationMode()) {
      files =
          constructRelativeRejectedFilePathes(
              files, copyIntoTableContext.getStorageLocation(), datasetPath);
    }
    PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
    List<PartitionValue> partitionValue = Collections.emptyList();
    if (files != null && files.size() > 0) {
      addFilesToPartitionChunkListing(files, partitionChunkListing);
    } else {
      DirListInputSplitProto.DirListInputSplit dirListInputSplit =
          DirListInputSplitProto.DirListInputSplit.newBuilder()
              .setRootPath(datasetPath.toString())
              .setOperatingPath(datasetPath.toString())
              .setReadSignature(Long.MAX_VALUE)
              .setIsFile(isFileDataset)
              .setHasVersion(false)
              .build();
      DatasetSplit split =
          DatasetSplit.of(Collections.emptyList(), 1, 1, dirListInputSplit::writeTo);
      partitionChunkListing.put(partitionValue, split);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("'COPY INTO' DirListing Path : {}", datasetPath.toString());
    }
    partitionChunkListing.computePartitionChunks();
    SplitsPointer splitsPointer =
        MaterializedSplitsPointer.of(
            0,
            AbstractRefreshPlanBuilder.convertToPartitionChunkMetadata(
                partitionChunkListing, datasetConfig),
            1);
    RefreshExecTableMetadata refreshExecTableMetadata =
        new RefreshExecTableMetadata(
            storagePluginId, datasetConfig, userName, splitsPointer, BatchSchema.EMPTY, null);

    return new DirListingScanPrel(
        cluster,
        traitSet,
        targetTable,
        storagePluginId,
        refreshExecTableMetadata,
        1.0,
        ImmutableList.of(),
        true,
        x -> getDirListingPrelRowCountEstimates(),
        ImmutableList.of());
  }

  private void addFilesToPartitionChunkListing(
      List<String> files, PartitionChunkListingImpl partitionChunkListing) {
    List<PartitionValue> partitionValue = Collections.emptyList();
    DirListInputSplitProto.DirListInputSplit dirListInputSplit =
        DirListInputSplitProto.DirListInputSplit.newBuilder()
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
    DistributionTrait.DistributionField distributionField =
        new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait =
        new DistributionTrait(
            DistributionTrait.DistributionType.HASH_DISTRIBUTED,
            ImmutableList.of(distributionField));
    RelTraitSet relTraitSet =
        cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);

    return new HashToRandomExchangePrel(
        cluster,
        relTraitSet,
        child,
        distributionTrait.getFields(),
        TableFunctionUtil.getHashExchangeTableFunctionCreator(
            tableMetadata, targetTableSchema, true, storagePluginId));
  }

  /**
   * Creates system Iceberg tables if they do not already exist based on the provided {@link
   * SystemIcebergTablesStoragePlugin} instance. The method checks if the processing context
   * indicates that the operation should continue on error before attempting to create the system
   * tables.
   *
   * @see #isOnErrorHandlingRequested(CopyIntoTableContext)
   * @see SystemIcebergTablesStoragePlugin#createEmptySystemIcebergTablesIfNotExists()
   */
  private void createSystemIcebergTablesIfNotExists(SystemIcebergTablesStoragePlugin plugin) {
    if (isOnErrorHandlingRequested(copyIntoTableContext)) {
      plugin.createEmptySystemIcebergTablesIfNotExists();
    }
  }

  protected boolean isSkipFile() {
    return CopyIntoQueryProperties.OnErrorOption.SKIP_FILE.equals(
        queryProperties.getOnErrorOption());
  }

  protected Path getDatasetPath() {
    return datasetPath;
  }
}
