/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.hive;

import static com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Properties;

import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.hive.exec.HiveReaderProtoUtil;
import com.dremio.exec.work.ExecErrorConstants;
import com.dremio.hive.proto.HiveReaderProto.HiveSplitXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.proto.HiveReaderProto.SerializedInputSplit;
import com.dremio.options.OptionManager;
import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;

public class HiveUtilities {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveUtilities.class);

  private static final String ERROR_MSG = "Unsupported Hive data type %s. \n"
      + "Following Hive data types are supported in Dremio for querying: "
      + "BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, BINARY, DECIMAL, STRING, VARCHAR and CHAR";

  public static void throwUnsupportedHiveDataTypeError(String unsupportedType) {
    throw UserException.unsupportedError()
        .message(ERROR_MSG, unsupportedType)
        .build(logger);
  }

  /**
   * Helper methods that properties in <i>propsToAdd</i> to both <i>jobConf</i> and <i>outputProps</i>.
   *
   * @param jobConf
   * @param outputProps
   * @param propsToAdd
   */
  public static final void addProperties(JobConf jobConf, Properties outputProps, List<Prop> propsToAdd){
    for(Prop p : propsToAdd){
      outputProps.setProperty(p.getKey(), p.getValue());
      jobConf.set(p.getKey(), p.getValue());
    }

    addACIDPropertiesIfNeeded(jobConf);
  }

  /**
   * Utility method which creates a SerDe object for given SerDe class name and properties.
   *
   * @param jobConf Configuration to use when creating SerDe class
   * @param sLib {@link SerDe} class name
   * @param properties SerDe properties
   * @return
   * @throws Exception
   */
  public static final SerDe createSerDe(final JobConf jobConf, final String sLib, final Properties properties) throws Exception {
    final Class<? extends SerDe> c = Class.forName(sLib).asSubclass(SerDe.class);
    final SerDe serde = c.getConstructor().newInstance();
    serde.initialize(jobConf, properties);

    return serde;
  }

  /**
   * Get {@link InputFormat} class name for given table and partition definitions. We try to load the input format class
   * from partition definition if it exists. If not we load from the table definition.
   * @param jobConf
   * @param tableXattr
   * @param splitXattr
   * @return
   * @throws Exception
   */
  public static final Class<? extends InputFormat<?, ?>> getInputFormatClass(final JobConf jobConf,
      final HiveTableXattr tableXattr, final HiveSplitXattr splitXattr) throws Exception {
    if (splitXattr != null) {
      final Optional<String> inputFormat =
          HiveReaderProtoUtil.getPartitionInputFormat(tableXattr, splitXattr.getPartitionId());
      if (inputFormat.isPresent()) {
        return (Class<? extends InputFormat<?, ?>>) Class.forName(inputFormat.get());
      }

      final Optional<String> storageHandlerName =
          HiveReaderProtoUtil.getPartitionStorageHandler(tableXattr, splitXattr.getPartitionId());
      if (storageHandlerName.isPresent()) {
        final HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(jobConf, storageHandlerName.get());
        return (Class<? extends InputFormat<?, ?>>) storageHandler.getInputFormatClass();
      }
    }

    if (tableXattr != null) {
      final Optional<String> inputFormat = HiveReaderProtoUtil.getTableInputFormat(tableXattr);
      if (inputFormat.isPresent()) {
        return (Class<? extends InputFormat<?, ?>>) Class.forName(inputFormat.get());
      }

      final Optional<String> storageHandlerName = HiveReaderProtoUtil.getTableStorageHandler(tableXattr);
      if (storageHandlerName.isPresent()) {
        final HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(jobConf, storageHandlerName.get());
        return (Class<? extends InputFormat<?, ?>>) storageHandler.getInputFormatClass();
      }
    }

    throw new ExecutionSetupException("Unable to get Hive table InputFormat class. There is neither " +
        "InputFormat class explicitly specified nor a StorageHandler class provided.");
  }

  /**
   * Helper method that converts Hive type definition to Dremio type definition.
   *
   * @param typeInfo Hive type info
   * @param options
   * @return
   */
  public static MajorType getMajorTypeFromHiveTypeInfo(final TypeInfo typeInfo, final OptionManager options) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE: {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        MinorType minorType = getMinorTypeFromHivePrimitiveTypeInfo(primitiveTypeInfo, options);
        MajorType.Builder typeBuilder = MajorType.newBuilder().setMinorType(getMinorTypeFromArrowMinorType(minorType))
                .setMode(DataMode.OPTIONAL); // Hive columns (both regular and partition) could have null values

        if (primitiveTypeInfo.getPrimitiveCategory() == PrimitiveCategory.DECIMAL) {
          DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
          typeBuilder.setPrecision(decimalTypeInfo.precision())
                  .setScale(decimalTypeInfo.scale()).build();
        }

        return typeBuilder.build();
      }

      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      default:
        throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    }

    return null; // never reached
  }

  /**
   * Helper method which converts Hive primitive type to Dremio primitive type
   * @param primitiveTypeInfo
   * @param options
   * @return
   */
  private static final MinorType getMinorTypeFromHivePrimitiveTypeInfo(PrimitiveTypeInfo primitiveTypeInfo,
      OptionManager options) {
    switch(primitiveTypeInfo.getPrimitiveCategory()) {
      case BINARY:
        return MinorType.VARBINARY;
      case BOOLEAN:
        return MinorType.BIT;
      case DECIMAL: {

        if (options.getOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY).getBoolVal() == false) {
          throw UserException.unsupportedError()
              .message(ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG)
              .build(logger);
        }
        return MinorType.DECIMAL;
      }
      case DOUBLE:
        return MinorType.FLOAT8;
      case FLOAT:
        return MinorType.FLOAT4;
      // TODO (DRILL-2470)
      // Byte and short (tinyint and smallint in SQL types) are currently read as integers
      // as these smaller integer types are not fully supported in Dremio today.
      case SHORT:
      case BYTE:
      case INT:
        return MinorType.INT;
      case LONG:
        return MinorType.BIGINT;
      case STRING:
      case VARCHAR:
      case CHAR:
        return MinorType.VARCHAR;
      case TIMESTAMP:
        return MinorType.TIMESTAMPMILLI;
      case DATE:
        return MinorType.DATEMILLI;
    }
    throwUnsupportedHiveDataTypeError(primitiveTypeInfo.getPrimitiveCategory().toString());
    return null;
  }

  public static InputSplit deserializeInputSplit(SerializedInputSplit split) throws IOException, ReflectiveOperationException{
    Constructor<?> constructor = Class.forName(split.getInputSplitClass()).getDeclaredConstructor();
    if (constructor == null) {
      throw new ReflectiveOperationException("Class " + split.getInputSplitClass() + " does not implement a default constructor.");
    }
    constructor.setAccessible(true);
    InputSplit deserializedSplit = (InputSplit) constructor.newInstance();
    deserializedSplit.readFields(ByteStreams.newDataInput(split.getInputSplit().toByteArray()));
    return deserializedSplit;
  }

  public static StructObjectInspector getStructOI(final SerDe serDe) throws Exception {
    ObjectInspector oi = serDe.getObjectInspector();
    if (oi.getCategory() != Category.STRUCT) {
      throw new UnsupportedOperationException(String.format("%s category not supported", oi.getCategory()));
    }
    return (StructObjectInspector) oi;
  }

  /**
   * Helper method which sets config to read transactional (ACID) tables. Prerequisite is <i>job</i>
   * contains the table properties.
   * @param job
   */
  public static void addACIDPropertiesIfNeeded(final JobConf job) {
    if (!AcidUtils.isTablePropertyTransactional(job)) {
      return;
    }

    AcidUtils.setTransactionalTableScan(job, true);

    // Add ACID related properties
    if (Utilities.isSchemaEvolutionEnabled(job, true) &&
        job.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS) != null &&
        job.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES) != null) {
      // If the schema evolution columns and types are already set, then there is no additional conf to set.
      return;
    }

    // Get them from table properties and set them as schema evolution properties
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, job.get(serdeConstants.LIST_COLUMNS));
    job.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, job.get(serdeConstants.LIST_COLUMN_TYPES));

  }
}

