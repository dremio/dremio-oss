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
package com.dremio.exec.store.iceberg;

import static com.dremio.common.utils.PathUtils.removeLeadingSlash;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_SPEC_EVOL_TRANFORMATION;
import static com.dremio.exec.catalog.CatalogUtil.getAndValidateSourceForTableManagement;
import static com.dremio.exec.hadoop.DremioHadoopUtils.getContainerName;
import static com.dremio.exec.hadoop.DremioHadoopUtils.pathWithoutContainer;
import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;
import static com.dremio.exec.store.iceberg.IcebergSerDe.serializedSchemaAsJson;
import static com.dremio.io.file.Path.AZURE_AUTHORITY_SUFFIX;
import static com.dremio.io.file.Path.CONTAINER_SEPARATOR;
import static com.dremio.io.file.Path.SEPARATOR;
import static com.dremio.io.file.UriSchemes.ADL_SCHEME;
import static com.dremio.io.file.UriSchemes.AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.FILE_SCHEME;
import static com.dremio.io.file.UriSchemes.GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.HDFS_SCHEME;
import static com.dremio.io.file.UriSchemes.MAPRFS_SCHEME;
import static com.dremio.io.file.UriSchemes.S3_SCHEME;
import static com.dremio.io.file.UriSchemes.SCHEME_SEPARATOR;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UnknownFormatConversionException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DremioIndexByName;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsFileLocations;
import org.apache.iceberg.PartitionStatsMetadataUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.eclipse.jetty.util.URIUtil;
import org.joda.time.DateTimeConstants;
import org.slf4j.Logger;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.physical.base.CombineSmallFileOptions;
import com.dremio.exec.physical.base.IcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableIcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableTableFormatWriterOptions;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.TableFormatWriterOptions.TableFormatOperation;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.common.ImmutableDremioFileAttrs;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.query.OptimizeOptions;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.planner.sql.parser.SqlVacuumTable;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.FileSystemConfigurationAdapter;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.PrimaryKeyOperations;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.TableProperties;
import com.dremio.service.namespace.file.proto.FileType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;

import io.protostuff.ByteString;

/**
 * Class contains miscellaneous utility functions for Iceberg table operations
 */
public class IcebergUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergUtils.class);

  private static final int ICEBERG_TIMESTAMP_PRECISION = 6;
  public static final Pattern LOCALSORT_BY_PATTERN = Pattern.compile("LOCALSORT\\s*BY", Pattern.CASE_INSENSITIVE);

  public static final Function<RexNode, List<Integer>> getUsedIndices = cond -> {
    Set<Integer> usedIndices = new HashSet<>();
    cond.accept(new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitInputRef(RexInputRef inputRef) {
        usedIndices.add(inputRef.getIndex());
        return null;
      }
    });
    return usedIndices.stream().sorted().collect(Collectors.toList());
  };

  /**
   *
   * @param schema iceberg schema
   * @return column name to integer ID mapping
   */
  public static Map<String, Integer> getIcebergColumnNameToIDMap(Schema schema) {
    return getIcebergColumnNameToIDMap(schema.columns());
  }

  public static Map<String, Integer> getColIDMapWithReservedDeleteFields(Schema schema) {
    List<Types.NestedField> allCols = new ArrayList<>(schema.columns());

    // Not an exhaustive list, add the reserved columns as per usage
    // https://iceberg.apache.org/spec/#reserved-field-ids
    allCols.add(MetadataColumns.DELETE_FILE_PATH);
    allCols.add(MetadataColumns.DELETE_FILE_POS);

    return getIcebergColumnNameToIDMap(allCols);
  }

  private static Map<String, Integer> getIcebergColumnNameToIDMap(List<Types.NestedField> cols) {
    Map<String, Integer> schemaNameIDMap = TypeUtil.visit(Types.StructType.of(cols), new DremioIndexByName());
    CaseInsensitiveMap<Integer> nameToIDMap = CaseInsensitiveMap.newHashMap();
    nameToIDMap.putAll(schemaNameIDMap); // if two fields have the same name, ignore one of them
    return CaseInsensitiveMap.newImmutableMap(nameToIDMap);
  }

  public static Object getValueFromByteBuffer(ByteBuffer byteBuffer, Type fieldType) {
    if (byteBuffer == null) {
      return null;
    }
    Object value;
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    switch (fieldType.typeId()) {
      case INTEGER:
      case DATE:
        value = byteBuffer.getInt();
        break;
      case LONG:
        if(byteBuffer.remaining() == 4) {
          value = byteBuffer.getInt();
        } else {
          value = byteBuffer.getLong();
        }
        break;
      case TIME:
      case TIMESTAMP:
        value = byteBuffer.getLong();
        break;
      case FLOAT:
        value = byteBuffer.getFloat();
        break;
      case DOUBLE:
        if(byteBuffer.remaining() == 4) {
          value = byteBuffer.getFloat();
        } else {
          value = byteBuffer.getDouble();
        }
        break;
      case STRING:
        value = new String(byteBuffer.array(), StandardCharsets.UTF_8);
        break;
      case BINARY:
        value = byteBuffer;
        break;
      case DECIMAL:
        value = new BigDecimal(new BigInteger(byteBuffer.array()), ((Types.DecimalType) fieldType).scale());
        break;
      case BOOLEAN:
        value = byteBuffer.get() != 0;
        break;
      default:
        throw UserException.unsupportedError().message("unsupported type: " + fieldType).buildSilently();
    }
    return value;
  }

  public static void writeToVector(ValueVector vector, int idx, Object value) {
    if (value == null) {
      if (vector instanceof BaseFixedWidthVector) {
        ((BaseFixedWidthVector) vector).setNull(idx);
      } else if (vector instanceof BaseVariableWidthVector) {
        ((BaseVariableWidthVector) vector).setNull(idx);
      } else {
        throw UserException.unsupportedError().message("unexpected vector type: " + vector).buildSilently();
      }
      return;
    }
    if (vector instanceof IntVector) {
      ((IntVector) vector).setSafe(idx, (Integer) value);
    } else if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).setSafe(idx, value instanceof Integer ? (long) (Integer) value : (Long) value);
    } else if (vector instanceof Float4Vector) {
      ((Float4Vector) vector).setSafe(idx, (Float) value);
    } else if (vector instanceof Float8Vector) {
      ((Float8Vector) vector).setSafe(idx, value instanceof Float ? (double) (Float) value : (Double) value);
    } else if (vector instanceof VarCharVector) {
      ((VarCharVector) vector).setSafe(idx, new Text((String) value));
    } else if (vector instanceof VarBinaryVector) {
      if (value instanceof byte[]) {
        ((VarBinaryVector) vector).setSafe(idx, (byte[]) value);
      } else {
        ((VarBinaryVector) vector).setSafe(idx, ((ByteBuffer) value).array());
      }
    } else if (vector instanceof DecimalVector) {
      ((DecimalVector) vector).setSafe(idx, (BigDecimal) value);
    } else if (vector instanceof BitVector) {
      ((BitVector) vector).setSafe(idx, (Boolean) value ? 1 : 0);
    } else if (vector instanceof DateMilliVector) {
      ((DateMilliVector) vector).setSafe(idx, ((Integer) value) * ((long) DateTimeConstants.MILLIS_PER_DAY));
    } else if (vector instanceof TimeStampVector) {
      ((TimeStampVector) vector).setSafe(idx, (Long) value / 1_000);
    } else if (vector instanceof TimeMilliVector) {
      ((TimeMilliVector) vector).setSafe(idx, (int) ((Long) value / 1_000));
    } else {
      throw UserException.unsupportedError().message("unsupported vector type: " + vector).buildSilently();
    }
  }

  public static void writeSplitIdentity(NullableStructWriter structWriter, int index, SplitIdentity splitIdentity, ArrowBuf tmpBuf) {
    byte[] path = splitIdentity.getPath().getBytes(StandardCharsets.UTF_8);
    tmpBuf.reallocIfNeeded(path.length);
    tmpBuf.setBytes(0, path);

    structWriter.setPosition(index);
    structWriter.start();
    structWriter.varChar(SplitIdentity.PATH).writeVarChar(0, path.length, tmpBuf);
    structWriter.bigInt(SplitIdentity.OFFSET).writeBigInt(splitIdentity.getOffset());
    structWriter.bigInt(SplitIdentity.LENGTH).writeBigInt(splitIdentity.getLength());
    structWriter.bigInt(SplitIdentity.FILE_LENGTH).writeBigInt(splitIdentity.getFileLength());
    structWriter.end();
  }

  public static boolean isNonAddOnField(String fieldName) {
    return fieldName.equalsIgnoreCase(RecordReader.SPLIT_IDENTITY)
      || fieldName.equalsIgnoreCase(RecordReader.SPLIT_INFORMATION)
      || fieldName.equalsIgnoreCase(RecordReader.COL_IDS)
      || fieldName.equalsIgnoreCase(RecordReader.DATAFILE_PATH)
      || SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_COLS.contains(fieldName);
  }

    /**
     * checks:
     * 1. if iceberg feature is enabled
     * 2. table exists
     * 3. plugin support for iceberg tables
     * 4. table is an iceberg dataset
     * May return non-empty optional if ifExistsCheck is true
     */
    public static Optional<SimpleCommandResult> checkTableExistenceAndMutability(Catalog catalog, SqlHandlerConfig config,
                                                                                 NamespaceKey path, SqlOperator sqlOperator,
                                                                                 boolean shouldErrorIfTableDoesNotExist) {
      CatalogEntityKey catalogEntityKey = CatalogEntityKey.newBuilder()
        .keyComponents(path.getPathComponents())
        .tableVersionContext(TableVersionContext.of(VersionContext.NOT_SPECIFIED))
        .build();
      return checkTableExistenceAndMutability(catalog, config, catalogEntityKey, sqlOperator, shouldErrorIfTableDoesNotExist);
    }
    public static Optional<SimpleCommandResult> checkTableExistenceAndMutability(Catalog catalog, SqlHandlerConfig config,
                                                                                 CatalogEntityKey catalogEntityKey, SqlOperator sqlOperator,
                                                                                 boolean shouldErrorIfTableDoesNotExist) {
      boolean icebergFeatureEnabled = isIcebergFeatureEnabled(config.getContext().getOptions(),
          null);
      if (!icebergFeatureEnabled) {
        throw UserException.unsupportedError()
            .message("Please contact customer support for steps to enable " +
                "the iceberg tables feature.")
            .buildSilently();
      }
      NamespaceKey path = catalogEntityKey.toNamespaceKey();
      StoragePlugin maybeSource = getAndValidateSourceForTableManagement(catalog, catalogEntityKey.getTableVersionContext(), path);
      CatalogEntityKey.Builder keyBuilder = CatalogEntityKey.newBuilder().keyComponents(path.getPathComponents());
      if (maybeSource != null && maybeSource.isWrapperFor(VersionedPlugin.class)) {
        keyBuilder.tableVersionContext(catalogEntityKey.getTableVersionContext());
      }
      DremioTable table = CatalogUtil.getTableNoResolve(keyBuilder.build(), catalog);
      if (table == null) {
        if (shouldErrorIfTableDoesNotExist) {
          throw UserException.validationError()
            .message("Table [%s] does not exist.", path)
            .buildSilently();
        } else {
          return Optional.of(SimpleCommandResult.successful("Table [%s] does not exist.", path));
        }
      }

      if (table.getJdbcTableType() != org.apache.calcite.schema.Schema.TableType.TABLE) {
        // SqlHandlerUtil.validateSupportForDDLOperations passes a null for sqlOperator. However, due
        // to the way validations happen for DDLs, the caller already does a table type check. Adding
        // an assertion for an early bug detection if the assumption doesn't hold true.
        assert sqlOperator != null;
        throw UserException.validationError()
            .message("%s is not supported on this %s at [%s].", sqlOperator, table.getJdbcTableType(), path)
            .buildSilently();
      }

      if (!validatePluginSupportForIceberg(catalog, path)) {
        throw UserException.unsupportedError()
            .message("Source [%s] does not support this operation", path.getRoot())
            .buildSilently();
      }

      try {
        if (!DatasetHelper.isIcebergDataset(table.getDatasetConfig())) {
          throw UserException.unsupportedError()
              .message("Table [%s] is not configured to support DML operations", path)
              .buildSilently();
        }
      } catch (NullPointerException ex) {
        if (shouldErrorIfTableDoesNotExist) {
          throw UserException.validationError()
            .message("Table [%s] does not exist.", path)
            .buildSilently();
        } else {
          return Optional.of(SimpleCommandResult.successful("Table [%s] does not exist.", path));
        }
      }
      return Optional.empty();
    }

  public static ImmutableDremioFileAttrs getPartitionStatsFileAttrs(String rootPointer, long snapshotId, FileIO fileIO) {
    String partitionStatsMetadata = PartitionStatsMetadataUtil.toFilename(snapshotId);
    String partitionStatsFile = null;
    Long fileLength = null;

    try {
      String fullPath = resolvePath(rootPointer, partitionStatsMetadata);
      PartitionStatsFileLocations partitionStatsLocations = PartitionStatsMetadataUtil.readMetadata(
          fileIO, fullPath);
      if (partitionStatsLocations == null) {
        logger.debug("Partition stats metadata file: {} not found", partitionStatsMetadata);
        return new ImmutableDremioFileAttrs.Builder()
          .setFileName(partitionStatsFile)
          .setFileLength(fileLength)
          .build();
      }
      Map<Integer, String> partitionStatsFileBySpecId = partitionStatsLocations.all();
      if (partitionStatsFileBySpecId.isEmpty()) {
        logger.debug("Partition stats metadata file: {} was empty", partitionStatsMetadata);
        return new ImmutableDremioFileAttrs.Builder()
          .setFileName(partitionStatsFile)
          .setFileLength(fileLength)
          .build();
      }
      int maxSpecId = 0;
      if (partitionStatsFileBySpecId.size() > 1) {
        logger.info("Partition stats metadata file: {} has multiple entries", partitionStatsMetadata);
        maxSpecId = partitionStatsFileBySpecId.size() - 1;
      }
      // In the absence of partition spec evolution, we'll have just one partition spec file
      partitionStatsFile = partitionStatsLocations.getFileForSpecId(maxSpecId);
      if (partitionStatsFile != null) {
        try {
          fileLength = fileIO.newInputFile(partitionStatsFile).getLength();
        } catch (UserException uex) {
          // ignore UserException thrown by DremioFileIO while reading partition stats file
          logger.warn("Unable to read partition stats file: {}. Ignoring partition stats", partitionStatsFile);
          fileLength = null;
          partitionStatsFile = null;
        }
      }
    } catch (NotFoundException | UncheckedIOException exception) {
      logger.debug("Partition stats metadata file: {} not found", partitionStatsMetadata);
    }

    return new ImmutableDremioFileAttrs.Builder()
      .setFileName(partitionStatsFile)
      .setFileLength(fileLength)
      .build();
  }

  public static PartitionStatsFileLocations getPartitionStatsFiles(FileIO fileIO, String partitionStatsMetadataFilePath) {
    return PartitionStatsMetadataUtil.readMetadata(fileIO, partitionStatsMetadataFilePath);
  }

  @VisibleForTesting
  static String resolvePath(String rootPointer, String partitionStatsMetadata) {
    String encodedRootPointer = URIUtil.encodePath(rootPointer);
    URI rootPointerUri = URI.create(encodedRootPointer);
    String scheme = rootPointerUri.getScheme();
    String fullPath;
    if (scheme == null) {
      fullPath = resolve(encodedRootPointer, partitionStatsMetadata);
    } else {
      String path = "";
      if (rootPointerUri.getAuthority() != null) {
        path = rootPointerUri.getAuthority();
      }
      String pathToPartitionStatsMetadata = resolve(path + rootPointerUri.getPath(), partitionStatsMetadata);
      fullPath = scheme + SCHEME_SEPARATOR + pathToPartitionStatsMetadata;
    }
    return URIUtil.decodePath(fullPath);
  }

  private static String resolve(String rootPointer, String partitionStatsMetadata) {
    return Paths.get(rootPointer).getParent().resolve(partitionStatsMetadata).toString();
  }

  private static boolean addPartitionTransformToSpec(BatchSchema batchSchema,
                                                     PartitionTransform transform,
                                                     PartitionSpec.Builder builder) {
    String partitionColumn = transform.getColumnName();
    Optional<Field> fieldFromSchema = batchSchema.findFieldIgnoreCase(partitionColumn);
    if (fieldFromSchema.isPresent()) {
      if (fieldFromSchema.get().getType().getTypeID() == ArrowType.ArrowTypeID.Time) {
        throw UserException.validationError().message("Partition type TIME for column '%s' is not supported",
          fieldFromSchema.get().getName()).buildSilently();
      }
      partitionColumn = fieldFromSchema.get().getName();
      switch (transform.getType()) {
        case IDENTITY:
          builder.identity(partitionColumn);
          break;
        case YEAR:
          builder.year(partitionColumn);
          break;
        case MONTH:
          builder.month(partitionColumn);
          break;
        case DAY:
          builder.day(partitionColumn);
          break;
        case HOUR:
          builder.hour(partitionColumn);
          break;
        case BUCKET:
          builder.bucket(partitionColumn, transform.getArgumentValue(0, Integer.class));
          break;
        case TRUNCATE:
          builder.truncate(partitionColumn, transform.getArgumentValue(0, Integer.class));
          break;
        default:
          throw UserException.validationError().message("Iceberg tables do not support partition transform '%s'",
            transform.getType().getName()).buildSilently();
      }

      return true;
    }

    return false;
  }

  public static PartitionSpec getIcebergPartitionSpecFromTransforms(BatchSchema batchSchema,
                                                                    List<PartitionTransform> partitionTransforms,
                                                                    Schema existingIcebergSchema) {
    Preconditions.checkNotNull(partitionTransforms);

    try {
      Schema schema;
      if (existingIcebergSchema != null) {
        schema = existingIcebergSchema;
      } else {
        SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
        schema = schemaConverter.toIcebergSchema(batchSchema);
      }

      PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
      List<String> invalidColumns = new ArrayList<>();
      for (PartitionTransform transform : partitionTransforms) {
        if (!addPartitionTransformToSpec(batchSchema, transform, partitionSpecBuilder)) {
          invalidColumns.add(transform.getColumnName());
        }
      }

      if (!invalidColumns.isEmpty()) {
        throw UserException.validationError()
          .message("Partition column(s) %s are not found in table.", invalidColumns).buildSilently();
      }

      return partitionSpecBuilder.build();
    } catch (Exception ex) {
      throw UserException.validationError(ex).buildSilently();
    }
  }

  public static PartitionSpec getIcebergPartitionSpec(BatchSchema batchSchema,
                                                        List<String> partitionColumns, Schema existingIcebergSchema) {
    // match partition column name with name in schema
    List<String> partitionColumnsInSchemaCase = new ArrayList<>();
    if (partitionColumns != null) {
      List<String> invalidPartitionColumns = new ArrayList<>();
      for (String partitionColumn : partitionColumns) {
        if (partitionColumn.equals(IncrementalUpdateUtils.UPDATE_COLUMN)) {
          continue; // skip implicit columns
        }
        Optional<Field> fieldFromSchema = batchSchema.findFieldIgnoreCase(partitionColumn);
        if (fieldFromSchema.isPresent()) {
          if (fieldFromSchema.get().getType().getTypeID() == ArrowType.ArrowTypeID.Time) {
            throw UserException.validationError().message("Partition type TIME for column '%s' is not supported", fieldFromSchema.get().getName()).buildSilently();
          }
          partitionColumnsInSchemaCase.add(fieldFromSchema.get().getName());
        } else {
          invalidPartitionColumns.add(partitionColumn);
        }
      }
      if (!invalidPartitionColumns.isEmpty()) {
        throw UserException.validationError().message("Partition column(s) %s are not found in table.", invalidPartitionColumns).buildSilently();
      }
    }

    try {
      Schema schema;
      if (existingIcebergSchema != null) {
        schema = existingIcebergSchema;
      } else {
        SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
        schema = schemaConverter.toIcebergSchema(batchSchema);
      }
      PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
      for (String column : partitionColumnsInSchemaCase) {
        partitionSpecBuilder.identity(column);
      }
      return partitionSpecBuilder.build();
    } catch (Exception ex) {
      throw UserException.validationError(ex).buildSilently();
    }
  }

  public static SortOrder getIcebergSortOrder(BatchSchema batchSchema, List<String> sortColumns,
                                              Schema existingIcebergSchema, OptionManager options) {
    if (!isIcebergSortOrderFeatureEnabled(options)) {
      return SortOrder.unsorted();
    }
    try {
      Schema schema;
      if (existingIcebergSchema != null) {
        schema = existingIcebergSchema;
      } else {
        SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
        schema = schemaConverter.toIcebergSchema(batchSchema);
      }

      SortOrder.Builder sortOrderBuilder = SortOrder.builderFor(schema);
      for (String sortColumn : sortColumns) {
        sortOrderBuilder.asc(sortColumn, NullOrder.NULLS_FIRST); //ASC and NULLS_FIRST are default sort parameters for a column.
      }
      return sortOrderBuilder.build();
    } catch (Exception ex) {
      logger.warn("Unable to get IcebergSortOrder based on schema");
      throw UserException.validationError(ex).buildSilently();

    }
  }

  public static List<String> getColumnsFromSortOrder(SortOrder sortOrder, OptionManager options) {
    if (sortOrder == null || !isIcebergSortOrderFeatureEnabled(options)) {
      return Collections.emptyList();
    }
    Preconditions.checkNotNull(sortOrder.schema());
    try {
      Schema schema = sortOrder.schema();
      return sortOrder.fields().stream()
        .map(currentField -> schema.findField(currentField.sourceId()).name())
        .collect(Collectors.toList());
    } catch (Exception ex) {
      logger.warn("Unable to generate iceberg sort order columns");
      throw UserException.validationError(ex).buildSilently();
    }
  }

  public static String getValidIcebergPath(Path path, Configuration conf, String fsScheme) {
    return getValidIcebergPath(path, new HadoopFileSystemConfigurationAdapter(conf), fsScheme);
  }

  public static String getValidIcebergPath(String path, FileSystemConfigurationAdapter conf, String fsScheme) {
    return getValidIcebergPath(new Path(path), conf, fsScheme);
  }

  public static String getValidIcebergPath(Path path, FileSystemConfigurationAdapter conf, String fsScheme) {
    try {
      if (fsScheme == null || path.toUri().getScheme() != null) {
        return path.toString();
      }
      String modifiedPath = removeLeadingSlash(path.toString());
      if (fsScheme.equalsIgnoreCase(FileSystemConf.CloudFileSystemScheme.AZURE_STORAGE_FILE_SYSTEM_SCHEME.getScheme())) {
        String accountName = conf.get("dremio.azure.account");
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append(AZURE_SCHEME);
        urlBuilder.append(SCHEME_SEPARATOR);
        urlBuilder.append(getContainerName(path));
        urlBuilder.append(CONTAINER_SEPARATOR + accountName + AZURE_AUTHORITY_SUFFIX);
        urlBuilder.append(pathWithoutContainer(path).toString());
        return urlBuilder.toString();
      } else if (fsScheme.equalsIgnoreCase(FileSystemConf.CloudFileSystemScheme.S3_FILE_SYSTEM_SCHEME.getScheme())) {
        return S3_SCHEME + SCHEME_SEPARATOR + modifiedPath;
      } else if (fsScheme.equalsIgnoreCase(FileSystemConf.CloudFileSystemScheme.GOOGLE_CLOUD_FILE_SYSTEM.getScheme())) {
        return GCS_SCHEME + SCHEME_SEPARATOR + modifiedPath;
      } else if (fsScheme.equalsIgnoreCase(HDFS_SCHEME)) {
        String hdfsEndPoint = conf.get("fs.defaultFS");
        if (hdfsEndPoint == null || !hdfsEndPoint.toLowerCase().startsWith(HDFS_SCHEME)) {
          return HDFS_SCHEME + SCHEME_SEPARATOR + Path.SEPARATOR + modifiedPath; //Without authority
        } else {
          return hdfsEndPoint + modifiedPath;
        }
      } else if (fsScheme.equalsIgnoreCase(FILE_SCHEME)) {
        return FILE_SCHEME + SCHEME_SEPARATOR + Path.SEPARATOR + modifiedPath;
      } else if (fsScheme.equalsIgnoreCase(FileSystemConf.CloudFileSystemScheme.ADL_FILE_SYSTEM_SCHEME.getScheme())) {
        String adlsEndPoint = conf.get("fs.defaultFS", SEPARATOR);
        String[] endPointParts = adlsEndPoint.split(SCHEME_SEPARATOR);
        adlsEndPoint = (endPointParts.length > 1) ? endPointParts[1] : SEPARATOR;
        StringBuilder urlBuilder = new StringBuilder();
        return urlBuilder.append(ADL_SCHEME).append(SCHEME_SEPARATOR)
          .append(adlsEndPoint).append(modifiedPath).toString();
      } else if (fsScheme.equalsIgnoreCase(MAPRFS_SCHEME)) {
        return MAPRFS_SCHEME + SCHEME_SEPARATOR + SEPARATOR + modifiedPath;
      } else {
        throw new Exception("No File System scheme matches");
      }
    } catch (Exception ex) {
      throw new UnknownFormatConversionException("Unknown format (" + fsScheme + ") conversion for path " + path + " Error Message : " + ex.getMessage());
    }
  }

  public static SupportsInternalIcebergTable getSupportsInternalIcebergTablePlugin(FragmentExecutionContext fec, StoragePluginId pluginId) {
      StoragePlugin plugin = wrap(() -> fec.getStoragePlugin(pluginId));
      if (plugin instanceof SupportsInternalIcebergTable) {
        return (SupportsInternalIcebergTable) plugin;
      } else {
        throw UserException.validationError().message("Source identified was invalid type.").buildSilently();
      }
    }

  private static <T> T wrap(Callable<T> task) {
    try {
      return task.call();
    } catch (Exception e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  public static List<Field> convertSchemaMilliToMicro(List<Field> fields) {
    return fields.stream()
      .map(IcebergUtils::convertFieldMilliToMicro)
      .collect(Collectors.toList());
  }

  public static Field convertFieldMilliToMicro(Field field) {
    if (field.getChildren() == null || field.getChildren().isEmpty()) {
      if (field.getType().equals(org.apache.arrow.vector.types.Types.MinorType.TIMEMILLI.getType())) {
        FieldType fieldType = field.getFieldType();
        return new Field(field.getName(), new FieldType(fieldType.isNullable(), org.apache.arrow.vector.types.Types.MinorType.TIMEMICRO.getType(), fieldType.getDictionary(), field.getMetadata()), null);
      } else if (field.getType().equals(org.apache.arrow.vector.types.Types.MinorType.TIMESTAMPMILLI.getType())) {
        FieldType fieldType = field.getFieldType();
        return new Field(field.getName(), new FieldType(fieldType.isNullable(), org.apache.arrow.vector.types.Types.MinorType.TIMESTAMPMICRO.getType(), fieldType.getDictionary(), field.getMetadata()), null);
      }
      return field;
    }
    return new Field(field.getName(), field.getFieldType(), convertSchemaMilliToMicro(field.getChildren()));
  }

  public static void setPartitionSpecValue(IcebergPartitionData data, int position, Field field, PartitionProtobuf.PartitionValue partitionValue) {
    final CompleteType type = CompleteType.fromField(field);
    Object value = null;
    switch(type.toMinorType()){
      case BIGINT:
        value = partitionValue.hasLongValue() ? partitionValue.getLongValue() : null;
        data.setLong(position, (Long)value);
        break;
      case BIT:
        value =  partitionValue.hasBitValue() ? partitionValue.getBitValue() : null;
        data.setBoolean(position, (Boolean)value);
        break;
      case FLOAT4:
        value =  partitionValue.hasFloatValue() ? partitionValue.getFloatValue() : null;
        data.setFloat(position, (Float)value);
        break;
      case FLOAT8:
        value =  partitionValue.hasDoubleValue() ? partitionValue.getDoubleValue() : null;
        data.setDouble(position, (Double)value);
        break;
      case INT:
        value =  partitionValue.hasIntValue()? partitionValue.getIntValue() : null;
        data.setInteger(position, (Integer)value);
        break;
      case DECIMAL:
        value = partitionValue.hasBinaryValue() ? partitionValue.getBinaryValue().toByteArray() : null;
        if(value != null) {
          BigInteger unscaledValue = new BigInteger((byte[])value);
          data.setBigDecimal(position, new BigDecimal(unscaledValue, type.getScale()));
        } else {
          data.setBigDecimal(position, null);
        }
        break;
      case VARBINARY:
        value =  partitionValue.hasBinaryValue() ? partitionValue.getBinaryValue().toByteArray() : null;
        data.setBytes(position, (byte[])value);
        break;
      case VARCHAR:
        value =  partitionValue.hasStringValue() ? partitionValue.getStringValue() : null;
        data.setString(position, (String) value);
        break;
      case DATE:
        value =  partitionValue.hasLongValue() ? partitionValue.getLongValue() : null;
        if(value != null) {
          long days = TimeUnit.MILLISECONDS.toDays((Long)value);
          data.setInteger(position, Math.toIntExact(days));
        } else {
          data.setInteger(position, null);
        }
        break;
      case TIME:
        value = partitionValue.hasIntValue() ? partitionValue.getIntValue() : null;
        if (value != null) {
          long longValue = ((Integer)(value)).longValue() * 1000L;
          data.setLong(position, longValue);
        } else {
          data.setLong(position, null);
        }
        break;
      case TIMESTAMP:
        value = partitionValue.hasLongValue() ? partitionValue.getLongValue() : null;
        data.setLong(position, value != null? (Long)(value)*1000L: null);
        break;
      default:
        throw new UnsupportedOperationException("Unable to return partition field: "  + Describer.describe(field));
    }
  }

  // If a partition column has identity transformation in all the partition specs, that column doesn't need to be
  // scanned and can be pruned
  public static Set<String> getInvalidColumnsForPruning(Map<Integer, PartitionSpec> partitionSpecMap) {
    if(partitionSpecMap != null) {
      // create a frequency map to record identity transformations for a partition column
      // key -> column name, value -> count of partition specs in which column has identity transform
      Map<String, Integer> identityFreqMap = new HashMap<>();
      for (Map.Entry<Integer, PartitionSpec> entry : partitionSpecMap.entrySet()) {
        PartitionSpec partitionSpec = entry.getValue();
        Schema schema = partitionSpec.schema();
        for (PartitionField partitionField : partitionSpec.fields()) {
          String fieldName = schema.findField(partitionField.sourceId()).name();
          identityFreqMap.merge(fieldName, partitionField.transform().isIdentity() ? 1 : 0, Integer::sum);
        }
      }
      int totalPartitionSpecs = partitionSpecMap.size();
      return identityFreqMap.entrySet().stream()
        .filter(entry -> entry.getValue() < totalPartitionSpecs)
        .map(entry -> entry.getKey())
        .collect(Collectors.toSet());
    }
    return new HashSet<>();
  }


  public static PartitionSpec getPartitionSpecFromMap(Map<Integer, PartitionSpec> partitionSpecMap) {
    int current_id = Collections.max(partitionSpecMap.keySet());
    return partitionSpecMap != null ? partitionSpecMap.get(current_id) : null;
  }

  public static ByteString getCurrentPartitionSpec(PhysicalDataset physicalDataset, BatchSchema batchSchema, List<String> partitionColumns) {
    PartitionSpec partitionSpec = getCurrentPartitionSpec(physicalDataset);
    partitionSpec = partitionSpec != null ? partitionSpec: getIcebergPartitionSpec(batchSchema, partitionColumns, null);
    return ByteString.copyFrom(IcebergSerDe.serializePartitionSpec(partitionSpec));
  }

  public static PartitionSpec getCurrentPartitionSpec(PhysicalDataset physicalDataset) {
    PartitionSpec partitionSpec = null;
    if (physicalDataset.getIcebergMetadata() != null) {
      if (physicalDataset.getIcebergMetadata().getPartitionSpecsJsonMap() != null) {
        partitionSpec = getPartitionSpecFromMap(IcebergSerDe.deserializeJsonPartitionSpecMap(deserializedJsonAsSchema(physicalDataset.getIcebergMetadata().getJsonSchema()),
          physicalDataset.getIcebergMetadata().getPartitionSpecsJsonMap().toByteArray()));
      } else if (physicalDataset.getIcebergMetadata().getPartitionSpecs() != null) {
        partitionSpec = getPartitionSpecFromMap(IcebergSerDe.deserializePartitionSpecMap(physicalDataset.getIcebergMetadata().getPartitionSpecs().toByteArray()));
      }
    }
    return partitionSpec;
  }

  public static Map<Integer, PartitionSpec> getPartitionSpecMap(final IcebergMetadata metadata) {
    final Optional<byte[]> specsOpt = Optional.of(metadata)
      .map(IcebergMetadata::getPartitionSpecsJsonMap)
      .map(ByteString::toByteArray);
    final Optional<Schema> schemaOpt = Optional.of(metadata)
      .map(IcebergMetadata::getJsonSchema)
      .map(IcebergSerDe::deserializedJsonAsSchema);
    return specsOpt.flatMap(specs ->
        schemaOpt.map(schema -> IcebergSerDe.deserializeJsonPartitionSpecMap(schema, specs)))
      .orElseGet(ImmutableMap::of);
  }

  public static String getCurrentSortOrder(PhysicalDataset physicalDataset, OptionManager options) {
    IcebergMetadata icebergMetadata = physicalDataset.getIcebergMetadata();
    if (isIcebergSortOrderFeatureEnabled(options) && icebergMetadata != null && icebergMetadata.getSortOrder() != null) {
      return icebergMetadata.getSortOrder();
    }
    return IcebergSerDe.serializeSortOrderAsJson(SortOrder.unsorted());
  }

  public static String getCurrentIcebergSchema(PhysicalDataset physicalDataset, BatchSchema batchSchema) {
      if (physicalDataset.getIcebergMetadata() != null) {
        return physicalDataset.getIcebergMetadata().getJsonSchema();
      } else {
        return serializedSchemaAsJson(SchemaConverter.getBuilder().build().toIcebergSchema(batchSchema));
      }
  }

  public static String getPartitionFieldName(PartitionField partitionField) {
      if(partitionField.transform().toString().equals("void")) {
        return partitionField.name() + "_void";
      } else {
        return partitionField.name();
      }
  }

  public static List<String> getPartitionColumns(Table table) {
      return getPartitionColumns(table.spec(), table.schema());
  }

  public static List<String> getPartitionColumns(PartitionSpec spec, Schema schema) {
    return spec
      .fields()
      .stream()
      .filter(partitionField -> !partitionField.transform().equals(Transforms.alwaysNull()))
      .map(PartitionField::sourceId)
      .map(schema::findColumnName) // column name from schema
      .distinct()
      .collect(Collectors.toList());
  }

  public static boolean hasNonIdentityPartitionColumns(PartitionSpec partitionSpec) {
    if (partitionSpec == null) {
      return false;
    }

    for (PartitionField partitionField : partitionSpec.fields()) {
      if(!isIdentityPartitionColumn(partitionField)) {
        return true;
      }
    }

    return false;
  }

  public static boolean isIdentityPartitionColumn(PartitionField partitionField) {
      return partitionField != null && partitionField.transform().isIdentity();
  }

  public static BatchSchema getWriterSchema(BatchSchema writerSchema, WriterOptions writerOptions) {
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    // current parquet writer uses a few extra columns in the schema for partitioning and distribution
    // For iceberg, filter those extra columns
    Set<String> extraFields = new HashSet<>();
    PartitionSpec partitionSpec = Optional.ofNullable(writerOptions.getTableFormatOptions().getIcebergSpecificOptions()
        .getIcebergTableProps()).map(props -> props.getDeserializedPartitionSpec()).orElse(null);

    if (partitionSpec != null) {
      extraFields = partitionSpec.fields().stream()
        .filter(partitionField -> !isIdentityPartitionColumn(partitionField))
        .map(IcebergUtils::getPartitionFieldName)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());
    }

    for (Field field : writerSchema) {
      if (field.getName().equalsIgnoreCase(WriterPrel.PARTITION_COMPARATOR_FIELD)) {
        continue;
      }
      if (field.getName().equalsIgnoreCase(WriterPrel.BUCKET_NUMBER_FIELD)) {
        continue;
      }

      if (extraFields.contains(field.getName().toLowerCase())) {
        continue;
      }
      schemaBuilder.addField(field);
    }
    return schemaBuilder.build();
  }

  public static String getMetadataLocation(DatasetConfig configs, Iterator<PartitionChunkMetadata> splits) {
    IcebergMetadata icebergMetadata = configs.getPhysicalDataset().getIcebergMetadata();
    if (icebergMetadata != null && icebergMetadata.getMetadataFileLocation() != null &&
      !icebergMetadata.getMetadataFileLocation().isEmpty()) {
      return icebergMetadata.getMetadataFileLocation();
    }

    // following is for backward compatibility
    try {
      return EasyProtobuf.EasyDatasetSplitXAttr.parseFrom(
        splits
          .next()
          .getDatasetSplits()
          .iterator()
          .next()
          .getSplitExtendedProperty()).getPath();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean checkNonIdentityTransform(PartitionSpec partitionSpec) {
    return partitionSpec.fields().stream().anyMatch(partitionField -> !partitionField.transform().isIdentity());
  }

  public static boolean isIcebergFeatureEnabled(OptionManager options,
                                                Map<String, Object> storageOptionsMap) {
    if (!options.getOption(ExecConstants.ENABLE_ICEBERG)) {
      return false;
    }

    // Iceberg table format will not be used as a storage type explicitly
    // parquet/arrow/json formats specify in the options
    if (storageOptionsMap != null) {
      return false;
    }

    return true;
  }

  public static void validateIcebergLocalSortIfDeclared(String sql, OptionManager options) throws UserException {
    if (LOCALSORT_BY_PATTERN.matcher(sql).find() && !isIcebergSortOrderFeatureEnabled(options)) {
      throw UserException.unsupportedError().message("Iceberg Sort Order Operations are disabled").buildSilently();
    }
  }

  public static boolean isIcebergSortOrderFeatureEnabled(OptionManager options) {
    return options.getOption(ExecConstants.ENABLE_ICEBERG_SORT_ORDER);
  }

  public static boolean isIcebergDMLFeatureEnabled(SourceCatalog sourceCatalog,
                                                   NamespaceKey path, OptionManager options,
                                                   Map<String, Object> storageOptionsMap) {
    if (!isIcebergFeatureEnabled(options, storageOptionsMap)) {
      return false;
    }

    StoragePlugin storagePlugin = null;
    try {
      storagePlugin = sourceCatalog.getSource(path.getRoot());
    } catch (UserException ignored) {
    }

    // to avoid existing test failures do not check for additional flags for Versioned and FileSystem plugins
    if (storagePlugin == null || storagePlugin instanceof VersionedPlugin || storagePlugin instanceof FileSystemPlugin) {
      return true;
    }

    return options.getOption(ExecConstants.ENABLE_ICEBERG_DML);
  }

  public static void validateTablePropertiesRequest(OptionManager options) {
    if (!options.getOption(ExecConstants.ENABLE_ICEBERG_TABLE_PROPERTIES)) {
      throw UserException.unsupportedError()
              .message("TBLPROPERTIES is not supported in the query")
              .buildSilently();
    }
  }

  public static boolean validatePluginSupportForIceberg(SourceCatalog sourceCatalog, NamespaceKey path) {
      StoragePlugin storagePlugin;
      try {
          storagePlugin = sourceCatalog.getSource(path.getRoot());
      } catch (UserException uex) {
          return false;
      }

      if (storagePlugin instanceof VersionedPlugin) {
          return true;
      }

      if (storagePlugin instanceof FileSystemPlugin) {
          return ((FileSystemPlugin<?>) storagePlugin).supportsIcebergTables();
      }

      return storagePlugin instanceof MutablePlugin;
  }

  public static Term getIcebergTerm(PartitionTransform transform) {
    String partitionColumn = transform.getColumnName();
    switch (transform.getType()) {
      case IDENTITY:
        return Expressions.ref(partitionColumn);
      case YEAR:
        return Expressions.year(partitionColumn);
      case MONTH:
        return Expressions.month(partitionColumn);
      case DAY:
        return Expressions.day(partitionColumn);
      case HOUR:
        return Expressions.hour(partitionColumn);
      case BUCKET:
        return Expressions.bucket(partitionColumn, transform.getArgumentValue(0, Integer.class));
      case TRUNCATE:
        return Expressions.truncate(partitionColumn, transform.getArgumentValue(0, Integer.class));
      default:
        throw UserException.validationError().message("Iceberg tables do not support partition transform '%s'",
                transform.getType().getName()).buildSilently();
    }
  }

  public static Map<String, String> convertTableProperties(List<String> tablePropertyNameList, List<String> tablePropertyValueList, boolean expectEmptyValues) {
    if (expectEmptyValues) {
      if (!(tablePropertyValueList == null || tablePropertyValueList.isEmpty())) {
        throw UserException.parseError()
                .message("Property values list should be empty")
                .buildSilently();
      }
    } else {
      if (tablePropertyNameList.size() != tablePropertyValueList.size()) {
        throw UserException.parseError()
                .message("Number of table property names does not match values")
                .buildSilently();
      }
    }
    Map<String, String> tableProperties = new HashMap<>();
    for (int index = 0; index < tablePropertyNameList.size(); index++) {
      String nameString = tablePropertyNameList.get(index);
      String valueString = expectEmptyValues ? "" : tablePropertyValueList.get(index);
      tableProperties.put(nameString, valueString);
    }
    return tableProperties;
  }

  private static Map<String, String> convertListTablePropertiesToMap(List<TableProperties> tablePropertiesList) {
    Map<String, String> tableProperties = new HashMap<>();
    if (tablePropertiesList == null || tablePropertiesList.size() == 0) {
      return Collections.emptyMap();
    }
    for (int index = 0; index < tablePropertiesList.size(); index++) {
      TableProperties property = tablePropertiesList.get(index);
      tableProperties.put(property.getTablePropertyName(), property.getTablePropertyValue());
    }
    return tableProperties;
  }

  /**
   * We are now getting IcebergCreateTableEntry with versionContext.
   * Previously, we have always used session version as the version context, but now
   * we can also specify version with the sql.
   * if we do not have explicit versionContext, we will be using original code path
   * Else we will be using new code path containing explicit version context.
   */
  public static CreateTableEntry getIcebergCreateTableEntry(SqlHandlerConfig config, Catalog catalog, DremioTable table,
                                                            SqlOperator sqlOperator, OptimizeOptions optimizeOptions) {
    final NamespaceKey key = table.getPath();
    return getIcebergCreateTableEntry(config, catalog, table, sqlOperator, optimizeOptions,
      CatalogUtil.resolveVersionContext(catalog, key.getRoot(),
      config.getContext().getSession().getSessionVersionForSource(key.getRoot())));
  }

  public static CreateTableEntry getIcebergCreateTableEntry(SqlHandlerConfig config, Catalog catalog, DremioTable table,
                                                            SqlOperator sqlOperator, OptimizeOptions optimizeOptions, ResolvedVersionContext resolvedVersionContext) {
    final NamespaceKey key = table.getPath();
    final DatasetConfig datasetConfig = table.getDatasetConfig();
    final ReadDefinition readDefinition = datasetConfig.getReadDefinition();

    ResolvedVersionContext version = resolvedVersionContext;
    List<String> partitionColumnsList = readDefinition.getPartitionColumnsList();

    String queryId = QueryIdHelper.getQueryId(config.getContext().getQueryId());
    PhysicalDataset physicalDataset = datasetConfig.getPhysicalDataset();
    BatchSchema batchSchema = table.getSchema();
    Map<String, String> properties = convertListTablePropertiesToMap(datasetConfig.getPhysicalDataset().getIcebergMetadata().getTablePropertiesList());
    IcebergTableProps icebergTableProps = new IcebergTableProps(null,
      queryId,
      null,
      partitionColumnsList,
      getIcebergCommandType(sqlOperator),
      null,
      key.getName(),
      null,
      version,
      getCurrentPartitionSpec(physicalDataset, batchSchema, partitionColumnsList),
      getCurrentIcebergSchema(physicalDataset, batchSchema),
      null,
      getCurrentSortOrder(physicalDataset, config.getContext().getOptions()),
      properties,
      FileType.PARQUET);

    CombineSmallFileOptions combineSmallFileOptions = null;
    if ((optimizeOptions != null && config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_OPTIMIZE)) ||
      config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_DML)) {
      combineSmallFileOptions = CombineSmallFileOptions.builder()
        .setSmallFileSize(Double.valueOf(config.getContext().getOptions().getOption(ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR) *
          config.getContext().getOptions().getOption(ExecConstants.SMALL_PARQUET_BLOCK_SIZE_RATIO)).longValue())
        .build();
    }

    boolean isSingleWriter = false;

    IcebergWriterOptions icebergOptions = new ImmutableIcebergWriterOptions.Builder()
      .setIcebergTableProps(icebergTableProps).build();
    ImmutableTableFormatWriterOptions.Builder tableFormatOptionsBuilder = new ImmutableTableFormatWriterOptions.Builder()
      .setIcebergSpecificOptions(icebergOptions).setOperation(getTableFormatOperation(sqlOperator));

    if (optimizeOptions != null) {
      tableFormatOptionsBuilder.setMinInputFilesBeforeOptimize(optimizeOptions.getMinInputFiles());
      tableFormatOptionsBuilder.setTargetFileSize(optimizeOptions.getTargetFileSizeBytes());
      tableFormatOptionsBuilder.setSnapshotId(
        table.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getSnapshotId());
      isSingleWriter = optimizeOptions.isSingleDataWriter();
    }

    final WriterOptions options = new WriterOptions(
      (int) config.getContext().getOptions().getOption(PlannerSettings.RING_COUNT),
      partitionColumnsList,
      readDefinition.getSortColumnsList(),
      Collections.emptyList(),
      PartitionDistributionStrategy.getPartitionDistributionStrategy(
        config.getContext().getOptions().getOption(ExecConstants.WRITER_PARTITION_DISTRIBUTION_MODE)),
      null,
      isSingleWriter,
      Long.MAX_VALUE,
      tableFormatOptionsBuilder.build(),
      readDefinition.getExtendedProperty(),
      version,
      properties)
      .withCombineSmallFileOptions(combineSmallFileOptions);

    Schema schema = SchemaConverter.getBuilder().build().toIcebergSchema(batchSchema);
    String sortOrder = getCurrentSortOrder(physicalDataset, config.getContext().getOptions());
    List<String> sortColumns = getColumnsFromSortOrder(
      IcebergSerDe.deserializeSortOrderFromJson(schema, sortOrder), config.getContext().getOptions());
    options.setSortColumns(sortColumns);
    BatchSchema writerSchema = getWriterSchema(batchSchema, options);
    icebergTableProps.setFullSchema(writerSchema);
    icebergTableProps.setPersistedFullSchema(batchSchema);

    return catalog.createNewTable(key,
      icebergTableProps,
      options,
      null);
  }

  private static IcebergCommandType getIcebergCommandType(SqlOperator sqlOperator) {
      switch (sqlOperator.getKind()) {
        case DELETE:
          return IcebergCommandType.DELETE;
        case UPDATE:
          return IcebergCommandType.UPDATE;
        case MERGE:
          return IcebergCommandType.MERGE;
        case OTHER: {
          if (sqlOperator.getName().equalsIgnoreCase(SqlOptimize.OPERATOR.getName())) {
            return IcebergCommandType.OPTIMIZE;
          } else if (sqlOperator.getName().equalsIgnoreCase(SqlVacuumTable.OPERATOR.getName())) {
            return IcebergCommandType.VACUUM;
          }
          throw new UnsupportedOperationException(String.format("Unrecoverable Error: Invalid type: %s", sqlOperator.getKind()));
        }
        default:
          throw new UnsupportedOperationException(String.format("Unrecoverable Error: Invalid type: %s", sqlOperator.getKind()));
      }
  }

  public static boolean isIncrementalRefresh(IcebergCommandType icebergCommandType) {
    return icebergCommandType == IcebergCommandType.INCREMENTAL_METADATA_REFRESH
      || icebergCommandType == IcebergCommandType.PARTIAL_METADATA_REFRESH;
  }

  private static TableFormatOperation getTableFormatOperation(SqlOperator sqlOperator) {
    switch (sqlOperator.getKind()) {
      case DELETE:
        return TableFormatOperation.DELETE;
      case UPDATE:
        return TableFormatOperation.UPDATE;
      case MERGE:
        return TableFormatOperation.MERGE;
      case OTHER: {
        if (sqlOperator.getName().equalsIgnoreCase(SqlOptimize.OPERATOR.getName())) {
          return TableFormatOperation.OPTIMIZE;
        } else if (sqlOperator.getName().equalsIgnoreCase(SqlVacuumTable.OPERATOR.getName())) {
          return TableFormatOperation.VACUUM;
        }
        throw new UnsupportedOperationException(String.format("Unrecoverable Error: Invalid type: %s", sqlOperator.getKind()));
      }
      default:
        throw new UnsupportedOperationException(String.format("Unrecoverable Error: Invalid type: %s", sqlOperator.getKind()));
    }
  }

  public static Map<Integer, PartitionSpec> getPartitionSpecMapBySchema(Map<Integer, PartitionSpec> originalMap, Schema schema) {
    if (originalMap.size() == 0) {
      return originalMap;
    }
    Set<Integer> sourceIds = schema.columns().stream().map(Types.NestedField::fieldId).collect(Collectors.toSet());
    Map<Integer, PartitionSpec> newMap = new HashMap<>();
    for (Map.Entry<Integer, PartitionSpec> entry : originalMap.entrySet()) {
      if (isValidSpecForSchema(entry.getValue(), sourceIds)) {
        newMap.put(entry.getKey(), entry.getValue());
      }
    }
    return newMap;
  }

  private static boolean isValidSpecForSchema(PartitionSpec partitionSpec, Set<Integer> sourceIds) {
    return partitionSpec.fields().stream().map(partitionField -> partitionField.sourceId()).allMatch(sourceId -> sourceIds.contains(sourceId));
  }

  public static boolean isPrimaryKeySupported(DatasetConfig datasetConfig) {
    return datasetConfig.getPhysicalDataset() != null && // PK only supported for physical datasets
      // PK only supported for physical dataset for unlimited splits or native Iceberg format
      (DatasetHelper.isInternalIcebergTable(datasetConfig) || DatasetHelper.isIcebergDataset(datasetConfig));
  }

  public static List<String> validateAndGeneratePrimaryKey(MutablePlugin plugin, SabotContext context, NamespaceKey table,
                                                           DatasetConfig datasetConfig, SchemaConfig schemaConfig,
                                                           ResolvedVersionContext versionContext, boolean saveInKvStore) {
    if (datasetConfig.getPhysicalDataset().getPrimaryKey() != null) {
      // Return PK from KV store.
      return datasetConfig.getPhysicalDataset().getPrimaryKey().getColumnList();
    }

    // Cache the PK in the KV store.
    try {
      if (saveInKvStore) { // If we are not going to save in KV store, no point of validating this
        // Verify if the user has permission to write to the KV store.
        MetadataRequestOptions options = MetadataRequestOptions.of(schemaConfig);

        Catalog catalog = context.getCatalogService().getCatalog(options);
        catalog.validatePrivilege(table, SqlGrant.Privilege.ALTER);
      }
    } catch (UserException | java.security.AccessControlException ex) {
      if (ex instanceof java.security.AccessControlException ||
        ((UserException) ex).getErrorType() == UserBitShared.DremioPBError.ErrorType.PERMISSION) {
        return null; // The user does not have permission.
      }
      throw ex;
    }

    return plugin.getPrimaryKeyFromMetadata(table, datasetConfig, schemaConfig, versionContext, saveInKvStore);
  }

  public static List<String> getPrimaryKey(IcebergModel icebergModel, String path, NamespaceKey table,
                                           DatasetConfig datasetConfig, String userName, StoragePlugin storagePlugin,
                                           SabotContext context, boolean saveInKvStore) {
    final IcebergTableIdentifier tableIdentifier = icebergModel.getTableIdentifier(path);
    final Table icebergTable = icebergModel.getIcebergTable(tableIdentifier);

    final List<String> primaryKey = getPrimaryKeyFromTableMetadata(icebergTable);
    // This can happen if the table already had PK in the metadata, and we just promoted this table.
    // The key will not be in the KV store for that.
    // Even if PK is empty, we need to save to KV, so next time we know PK is unset and don't check from metadata again.
    if (saveInKvStore) {
      PrimaryKeyOperations.saveInKvStore(table, datasetConfig, userName, storagePlugin, context, primaryKey);
    }
    return primaryKey;
  }

  public static List<String> getPrimaryKeyFromTableMetadata(Table icebergTable) {
    ObjectMapper mapper = new ObjectMapper();
    BatchSchema batchSchema;
    try {
      batchSchema = mapper.readValue(
        icebergTable.properties().getOrDefault(
          PrimaryKeyOperations.DREMIO_PRIMARY_KEY,
          BatchSchema.EMPTY.toJson()),
        BatchSchema.class);
    } catch (JsonProcessingException e) {
      String error = "Unexpected error occurred while deserializing primary keys";
      logger.error(error, e);
      throw UserException.dataReadError(e).addContext(error).build(logger);
    }
    return batchSchema.getFields().stream()
      .map(f -> f.getName().toLowerCase(Locale.ROOT))
      .collect(Collectors.toList());
  }

  /**
   * Given an object representing a constant with a type, convert that constant to Iceberg equivalent
   * The only case we do something about is if we get a Long that represents time or timestamp
   * In that case Dremio has the constant in miliseconds, but Iceberg expects microseconds so we multiply  by 1_000
   * @param value Value to convert
   * @param type The datatype of the value
   * @return the converted value
   */
  public static Object toIcebergValue(final Object value, final TypeProtos.MajorType type) {
    if (value == null) {
      return value;
    }
    if (value instanceof Long) {
      //for iceberg timestamps we need to convert from milliseconds to microseconds
      if (type.getMinorType().equals(TypeProtos.MinorType.TIMESTAMP)
          || type.getMinorType().equals(TypeProtos.MinorType.TIMESTAMPTZ)
          || type.getMinorType().equals(TypeProtos.MinorType.TIME)
          || type.getMinorType().equals(TypeProtos.MinorType.TIMETZ)){
        final int precision = CompleteType.fromMajorType(type).getPrecision();
        final int precisionDifference = ICEBERG_TIMESTAMP_PRECISION - precision;
        final int factor = (int) Math.pow(10, Math.abs(precisionDifference));
        return  precisionDifference >= 0 ? (Long)value * factor : (Long)value / factor ;
      }
    }
    return value;
  }

  /**
   * Given a partition field extract the name of the column it is based on
   * We cannot get the name of the partitionField directly as Iceberg will attach _transfrom at the end of the column
   * We will instead use the sourceID to find the column name in the schema
   *
   * @param partitionField partition field to extract column name from
   * @param schema Iceberg Schema the current PartitionField is from
   * @return the extracted column name
   */
  public static String getColumnName(final PartitionField partitionField, Schema schema){
    return schema.findColumnName(partitionField.sourceId());
  }

  public static String getMetadataLocation(TableMetadata dataset, List<SplitWork> works) {
    if (dataset.getDatasetConfig().getPhysicalDataset().getIcebergMetadata() != null &&
      dataset.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation() != null &&
      !dataset.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation().isEmpty()) {
      return dataset.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
    } else {
      EasyProtobuf.EasyDatasetSplitXAttr extended;
      try {
        if (works.size() == 0) {
          //It's an in-valid scenario where splits size is zero.
          throw new RuntimeException("Unexpected state with zero split.");
        }
        // All the split will have the same iceberg metadata location.
        // It would be ideal to read it from any index in this case from the first index.
        extended = LegacyProtobufSerializer.parseFrom(EasyProtobuf.EasyDatasetSplitXAttr.PARSER,
          works.get(0).getSplitExtendedProperty());
        return extended.getPath();
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Could not deserialize split info", e);
      }
    }
  }

  public static List<SplitAndPartitionInfo> getSplitAndPartitionInfo(String splitPath) {
    final List<SplitAndPartitionInfo> splits = new ArrayList<>();
    IcebergProtobuf.IcebergDatasetSplitXAttr splitExtended = IcebergProtobuf.IcebergDatasetSplitXAttr.newBuilder()
      .setPath(splitPath)
      .build();
    List<DatasetSplitAffinity> splitAffinities = new ArrayList<>();
    DatasetSplit datasetSplit = DatasetSplit.of(
      splitAffinities, 0, 0, splitExtended::writeTo);

    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId(String.valueOf(1)).build();
    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
      .newBuilder()
      .setPartitionId(partitionInfo.getId())
      .setExtendedProperty(MetadataProtoUtils.toProtobuf(datasetSplit.getExtraInfo()));
    splits.add(new SplitAndPartitionInfo(partitionInfo, splitInfo.build()));
    return splits;
  }

  /**
   * Remove orphan files
   */
  public static void removeOrphanFiles(FileSystem fs, Logger logger, ExecutorService executorService, Set<String> filesToDelete) {
    logger.debug("Files to delete: {}", filesToDelete);
    Tasks.foreach(filesToDelete)
      .retry(3)
      .stopRetryOn(NotFoundException.class)
      .suppressFailureWhenFinished()
      .executeWith(executorService)
      .onFailure(
        (filePath, exc) -> {
          logger.warn("Fail to remove file: {}", filePath, exc);
        })
      .run(
        filePath -> {
          try{
            fs.delete(com.dremio.io.file.Path.of(filePath), true);
          } catch (IOException e) {
            logger.warn("Unable to remove newly added file: {}", filePath);
            // Not an error condition if cleanup fails.
          }
        });
  }

  /**
   * Loads and returns the Iceberg Table Metadata for a table
   */
  public static org.apache.iceberg.TableMetadata loadTableMetadata(FileIO io, OperatorContext context, String metadataLocation) {
    org.apache.iceberg.TableMetadata tableMetadata = TableMetadataParser.read(io, metadataLocation);
    if (!context.getOptions().getOption(ENABLE_ICEBERG_SPEC_EVOL_TRANFORMATION)) {
      checkForPartitionSpecEvolution(tableMetadata);
    }
    return tableMetadata;
  }

  /**
   * Creates a FileIO for an Iceberg Metadata
   */
  public static FileIO createFileIOForIcebergMetadata(SupportsIcebergRootPointer pluginForIceberg, OperatorContext context, String datasourcePluginUID, OpProps props, List<String> dataset, String metadataLocation) {
    FileSystem fs;
    try {
      fs = pluginForIceberg.createFSWithAsyncOptions(metadataLocation, props.getUserName(), context);
    } catch (IOException e) {
      throw new RuntimeException("Failed creating filesystem", e);
    }
    FileIO io = pluginForIceberg.createIcebergFileIO(fs, context, dataset, datasourcePluginUID, null);
    return io;
  }

  private static void checkForPartitionSpecEvolution(org.apache.iceberg.TableMetadata tableMetadata) {
    if (tableMetadata.specs().size() > 1) {
      throw UserException
        .unsupportedError()
        .message("Iceberg tables with partition spec evolution are not supported")
        .buildSilently();
    }

    if (IcebergUtils.checkNonIdentityTransform(tableMetadata.spec())) {
      throw UserException
        .unsupportedError()
        .message("Iceberg tables with Non-identity partition transforms are not supported")
        .buildSilently();
    }
  }

  public static Table getIcebergTable(CreateTableEntry createTableEntry) {
    IcebergTableProps icebergTableProps = createTableEntry.getIcebergTableProps();
    Preconditions.checkState(createTableEntry.getPlugin() instanceof SupportsIcebergMutablePlugin, "Plugin not instance of SupportsIcebergMutablePlugin");
    SupportsIcebergMutablePlugin plugin = (SupportsIcebergMutablePlugin) createTableEntry.getPlugin();

    try (FileSystem fs = plugin.createFS(icebergTableProps.getTableLocation(), createTableEntry.getUserName(), null)) {
      FileIO fileIO = plugin.createIcebergFileIO(fs, null, null, null, null);
      IcebergModel icebergModel = plugin.getIcebergModel(icebergTableProps, createTableEntry.getUserName(), null, fileIO);
      return icebergModel.getIcebergTable(icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

     /** locates the current (soon-to-be previous) metadata root
      * @param icebergMetadata the iceberg table metadata instance
      * @return the correct path for the iceberg metadata
      */
    public static com.dremio.io.file.Path getPreviousTableMetadataRoot(IcebergMetadata icebergMetadata) {
      com.dremio.io.file.Path metadataFilePath = com.dremio.io.file.Path.of(icebergMetadata.getMetadataFileLocation());
      com.dremio.io.file.Path containerSpecificMetadataFilePath = com.dremio.io.file.Path.of(com.dremio.io.file.Path.getContainerSpecificRelativePath(metadataFilePath));
      com.dremio.io.file.Path finalPath = com.dremio.io.file.Path.of(containerSpecificMetadataFilePath.toURI().getPath());
      return Objects.requireNonNull(Objects.requireNonNull(finalPath).getParent()).getParent();
    }
}
