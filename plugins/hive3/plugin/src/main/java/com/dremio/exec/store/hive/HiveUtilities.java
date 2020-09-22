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
package com.dremio.exec.store.hive;

import static com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Properties;

import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
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
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.io.Input;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.util.Closeable;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.work.ExecErrorConstants;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.proto.HiveReaderProto.SerializedInputSplit;
import com.dremio.options.OptionManager;
import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;

public class HiveUtilities {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveUtilities.class);
  public static final String MAP_KEY_FIELD_NAME = "key";
  public static final String MAP_VALUE_FIELD_NAME = "value";

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
   * Utility method which creates a AbstractSerDe object for given AbstractSerDe class name and properties.
   *
   * @param jobConf Configuration to use when creating AbstractSerDe class
   * @param sLib {@link AbstractSerDe} class name
   * @param properties AbstractSerDe properties
   * @return
   * @throws Exception
   */
  public static final AbstractSerDe createSerDe(final JobConf jobConf, final String sLib, final Properties properties) throws Exception {
    final Class<? extends AbstractSerDe> c = Class.forName(sLib).asSubclass(AbstractSerDe.class);
    final AbstractSerDe serde = c.getConstructor().newInstance();
    serde.initialize(jobConf, properties);

    return serde;
  }

  /**
   * Get {@link InputFormat} class name for given table and partition definitions. We try to get the InputFormat class name
   * from inputFormat if explicitly specified in inputFormat, else we get the InputFormat class name from storageHandlerName.
   * @param jobConf
   * @param inputFormat
   * @param storageHandlerName
   * @return InputFormat
   * @throws Exception
   */
  public static final Class<? extends InputFormat<?, ?>> getInputFormatClass(final JobConf jobConf, Optional<String> inputFormat,
    Optional<String> storageHandlerName) throws Exception {
    if (inputFormat.isPresent()) {
      return (Class<? extends InputFormat<?, ?>>) Class.forName(inputFormat.get());
    }

    if (storageHandlerName.isPresent()) {
      try (final Closeable swapper = HivePf4jPlugin.swapClassLoader()) {
        // HiveUtils.getStorageHandler() depends on the current context classloader if you query and HBase table,
        // and don't have an HBase session open.
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

      case LIST: {
        MinorType minorType = MinorType.LIST;
        MajorType.Builder typeBuilder = MajorType.newBuilder().setMinorType(getMinorTypeFromArrowMinorType(minorType))
          .setMode(DataMode.OPTIONAL);
        return typeBuilder.build();
      }
      case STRUCT:  {
        MinorType minorType = MinorType.STRUCT;
        MajorType.Builder typeBuilder = MajorType.newBuilder().setMinorType(getMinorTypeFromArrowMinorType(minorType))
          .setMode(DataMode.OPTIONAL);
        return typeBuilder.build();
      }
      case MAP: {
        // Treating hive map datatype as a "list of structs" datatype in arrow.
        MinorType minorType = MinorType.LIST;
        MajorType.Builder typeBuilder = MajorType.newBuilder().setMinorType(getMinorTypeFromArrowMinorType(minorType))
          .setMode(DataMode.OPTIONAL);
        return typeBuilder.build();
      }
      case UNION: {
        MinorType minorType = MinorType.UNION;
        MajorType.Builder typeBuilder = MajorType.newBuilder().setMinorType(getMinorTypeFromArrowMinorType(minorType))
            .setMode(DataMode.OPTIONAL);
        return typeBuilder.build();
      }
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

  public static StructObjectInspector getStructOI(final AbstractSerDe serDe) throws Exception {
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

    AcidUtils.setAcidOperationalProperties(job, true, null);

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

  /**
   * Encodes a SearchArgument to base64.
   * @param sarg
   * @return
   */
  public static String encodeSearchArgumentAsBas64(final SearchArgument sarg) {
    try(Output out = new Output(4 * 1024, 10 * 1024 * 1024)) {
      new Kryo().writeObject(out, sarg);
      out.flush();
      return Base64.encodeBase64String(out.toBytes());
    }
  }

  /**
   * Encodes a SearchArgument from base64.
   * @param kryoBase64EncodedFilter
   * @return
   */
  public static SearchArgument decodeSearchArgumentFromBase64(final String kryoBase64EncodedFilter) {
    try (Input input = new Input(Base64.decodeBase64(kryoBase64EncodedFilter))) {
      return new Kryo().readObject(input, SearchArgumentImpl.class);
    }
  }
}
