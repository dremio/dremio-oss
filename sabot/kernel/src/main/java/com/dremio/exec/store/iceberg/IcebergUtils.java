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
import static com.dremio.exec.hadoop.DremioHadoopUtils.getContainerName;
import static com.dremio.exec.hadoop.DremioHadoopUtils.pathWithoutContainer;
import static com.dremio.exec.hadoop.HadoopFileSystem.HDFS_SCHEME;
import static com.dremio.exec.hadoop.HadoopFileSystem.NAS_SCHEME;
import static com.dremio.io.file.Path.ADL_SCHEME;
import static com.dremio.io.file.Path.AZURE_AUTHORITY_SUFFIX;
import static com.dremio.io.file.Path.AZURE_SCHEME;
import static com.dremio.io.file.Path.CONTAINER_SEPARATOR;
import static com.dremio.io.file.Path.GCS_SCHEME;
import static com.dremio.io.file.Path.S3_SCHEME;
import static com.dremio.io.file.Path.SCHEME_SEPARATOR;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UnknownFormatConversionException;

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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DremioIndexByName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTimeConstants;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Class contains miscellaneous utility functions for Iceberg table operations
 */
public class IcebergUtils {

  /**
   *
   * @param schema iceberg schema
   * @return column name to integer ID mapping
   */
  public static Map<String, Integer> getIcebergColumnNameToIDMap(Schema schema) {
    Map<String, Integer> schemaNameIDMap = TypeUtil.visit(Types.StructType.of(schema.columns()), new DremioIndexByName());
    return CaseInsensitiveMap.newImmutableMap(schemaNameIDMap);
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
      case TIME:
      case TIMESTAMP:
        value = byteBuffer.getLong();
        break;
      case FLOAT:
        value = byteBuffer.getFloat();
        break;
      case DOUBLE:
        value = byteBuffer.getDouble();
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
      ((VarBinaryVector) vector).setSafe(idx, ((ByteBuffer) value).array());
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

  public static boolean isNonAddOnField(String fieldName) {
    return fieldName.equalsIgnoreCase(RecordReader.SPLIT_IDENTITY)
      || fieldName.equalsIgnoreCase(RecordReader.SPLIT_INFORMATION)
      || fieldName.equalsIgnoreCase(RecordReader.COL_IDS)
      || fieldName.equalsIgnoreCase(RecordReader.DATAFILE_PATH);
  }

    /**
     * checks:
     * 1. if iceberg feature is enabled
     * 2. table exists
     * 3. plugin support for iceberg tables
     * 4. table is an iceberg dataset
     * May return non-empty optional if ifExistsCheck is true
     * @param catalog
     * @param config
     * @param path
     * @param ifExistsCheck
     * @return
     */
    public static Optional<SimpleCommandResult> checkTableExistenceAndMutability(Catalog catalog, SqlHandlerConfig config,
                                                                                 NamespaceKey path, boolean ifExistsCheck) {
      boolean icebergFeatureEnabled = DataAdditionCmdHandler.isIcebergFeatureEnabled(config.getContext().getOptions(),
          null);
      if (!icebergFeatureEnabled) {
        throw UserException.unsupportedError()
            .message("Please contact customer support for steps to enable " +
                "the iceberg tables feature.")
            .buildSilently();
      }

      DremioTable table = catalog.getTableNoResolve(path);
      if (table == null) {
        if (ifExistsCheck) {
          return Optional.of(SimpleCommandResult.successful("Table [%s] does not exist.", path));
        } else {
          throw UserException.validationError()
              .message("Table [%s] not found", path)
              .buildSilently();
        }
      }

      if (table.getJdbcTableType() != org.apache.calcite.schema.Schema.TableType.TABLE) {
        throw UserException.validationError()
            .message("[%s] is a %s", path, table.getJdbcTableType())
            .buildSilently();
      }

      if (!DataAdditionCmdHandler.validatePluginSupportForIceberg(catalog, path)) {
        throw UserException.unsupportedError()
            .message("Source [%s] does not support DML operations", path.getRoot())
            .buildSilently();
      }

      try {
        if (!DatasetHelper.isIcebergDataset(table.getDatasetConfig())) {
          throw UserException.unsupportedError()
              .message("Table [%s] is not configured to support DML operations", path)
              .buildSilently();
        }
      } catch (NullPointerException ex) {
        if (ifExistsCheck) {
          return Optional.of(SimpleCommandResult.successful("Table [%s] does not exist.", path));
        } else {
          throw UserException.validationError()
              .message("Table [%s] not found", path)
              .buildSilently();
        }
      }
      return Optional.empty();
    }

    public static PartitionSpec getIcebergPartitionSpec(BatchSchema batchSchema,
                                                        List<String> partitionColumns, Schema existingIcebergSchema) {
      // match partition column name with name in schema
      List<String> partitionColumnsInSchemaCase = new ArrayList<>();
      if (partitionColumns != null) {
        List<String> invalidPartitionColumns = new ArrayList<>();
        for (String partitionColumn : partitionColumns) {
          Optional<Field> fieldFromSchema = batchSchema.findFieldIgnoreCase(partitionColumn);
          if (fieldFromSchema.isPresent()) {
            if (fieldFromSchema.get().getType().getTypeID() == ArrowType.ArrowTypeID.Time) {
              throw UserException.validationError().message("Partition column %s of type time is not supported", fieldFromSchema.get().getName()).buildSilently();
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
        if(existingIcebergSchema != null) {
          schema = existingIcebergSchema;
        } else {
          schema = SchemaConverter.toIcebergSchema(batchSchema);
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

  public static String getValidIcebergPath(Path path, Configuration conf, String fsScheme) {
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
        } else if (fsScheme.equalsIgnoreCase(NAS_SCHEME)) {
          return NAS_SCHEME + SCHEME_SEPARATOR + Path.SEPARATOR + modifiedPath;
        } else if (fsScheme.equals(FileSystemConf.CloudFileSystemScheme.ADL_FILE_SYSTEM_SCHEME)) {
          return ADL_SCHEME + SCHEME_SEPARATOR + modifiedPath; //TODO: Decide proper conf property name to get source name
        } else {
          throw new Exception("No File System scheme matches");
        }
      } catch (Exception ex) {
        throw new UnknownFormatConversionException("Unknown format conversion for path" + path + " Error Message : " + ex.getMessage());
      }
  }
}
