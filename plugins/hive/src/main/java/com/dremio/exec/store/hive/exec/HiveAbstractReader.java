/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.hive.exec;

import static com.dremio.common.util.MajorTypeHelper.getFieldForNameAndMajorType;
import static com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.exec.work.ExecErrorConstants;
import com.dremio.hive.proto.HiveReaderProto.HiveSplitXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.PartitionProp;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.proto.HiveReaderProto.SerializedInputSplit;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.protobuf.InvalidProtocolBufferException;

import io.netty.buffer.ArrowBuf;

public abstract class HiveAbstractReader extends AbstractRecordReader {
  protected final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
  private static final org.slf4j.Logger abstractLogger = org.slf4j.LoggerFactory.getLogger(HiveAbstractReader.class);

  protected ArrowBuf managedBuffer;

  protected StructField[] selectedStructFieldRefs;
  protected ObjectInspector[] selectedColumnObjInspectors;
  protected HiveFieldConverter[] selectedColumnFieldConverters;
  protected RecordReader<Object, Object> reader;
  protected ValueVector[] vectors;
  protected HiveConf hiveConf;

  // SerDe of the reading partition (or table if the table is non-partitioned)
  protected SerDe partitionSerDe;

  // ObjectInspector to read data from partitionSerDe (for a non-partitioned table this is same as the table
  // ObjectInspector).
  protected StructObjectInspector partitionOI;

  // Final ObjectInspector. We may not use the partitionOI directly if there are schema changes between the table and
  // partition. If there are no schema changes then this is same as the partitionOI.
  protected StructObjectInspector finalOI;

  // Converter which converts data from partition schema to table schema.
  protected Converter partTblObjectInspectorConverter;

  private final ReadDefinition def;
  private final DatasetSplit split;

  protected OperatorContext context;
  protected String defaultPartitionValue;

  public HiveAbstractReader(
      ReadDefinition def,
      DatasetSplit split,
      List<SchemaPath> projectedColumns,
      OperatorContext context,
      final HiveConf hiveConf) throws ExecutionSetupException {
    super(context, projectedColumns);
    this.def = def;
    this.split = split;
    this.hiveConf = hiveConf;
    this.context = context;
  }

  public abstract void internalInit(Properties tableProperties, RecordReader<Object, Object> reader);

  public static Properties addProperties(Properties output, List<Prop> props){
    for(Prop p : props){
      output.setProperty(p.getKey(), p.getValue());
    }
    return output;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.managedBuffer = context.getManagedBuffer(256);
    final HiveTableXattr tableAttr;
    final HiveSplitXattr splitAttr;
    try {
      tableAttr = HiveTableXattr.parseFrom(def.getExtendedProperty().toByteArray());
      splitAttr = HiveSplitXattr.parseFrom(split.getExtendedProperty().toByteArray());
    } catch (InvalidProtocolBufferException e) {
      throw new ExecutionSetupException("Failure deserializing Hive extended attributes.");
    }

    final JobConf job = new JobConf(hiveConf);

    // Get the configured default val
    defaultPartitionValue = hiveConf.get(ConfVars.DEFAULTPARTITIONNAME.varname);

    final Properties tableProperties = addProperties(new Properties(), tableAttr.getTablePropertyList());

    List<String> selectedColumnNames;
    List<TypeInfo> selectedColumnTypes = new ArrayList<>();

    try {
      final SerDe tableSerDe = createSerDe(job, tableAttr.getSerializationLib(), tableProperties);
      final StructObjectInspector tableOI = getStructOI(tableSerDe);

      boolean isPartitioned = def.getPartitionColumnsList() != null && !def.getPartitionColumnsList().isEmpty();
      if(isPartitioned){
        final Properties partitionProperties = addProperties(addProperties(new Properties(),
          tableAttr.getPartitionProperties(splitAttr.getPartitionId()).getPartitionPropertyList()), tableAttr.getTablePropertyList());
        partitionSerDe = createSerDe(job,
          tableAttr.getPartitionProperties(splitAttr.getPartitionId()).getSerializationLib(), partitionProperties);
        partitionOI = getStructOI(partitionSerDe);

        finalOI = (StructObjectInspector)ObjectInspectorConverters.getConvertedOI(partitionOI, tableOI);
        partTblObjectInspectorConverter = ObjectInspectorConverters.getConverter(partitionOI, finalOI);
        job.setInputFormat(getInputFormatClass(job, tableAttr, splitAttr));
      } else {
        // For non-partitioned tables, there is no need to create converter as there are no schema changes expected.
        partitionSerDe = tableSerDe;
        partitionOI = tableOI;
        partTblObjectInspectorConverter = null;
        finalOI = tableOI;
        job.setInputFormat(getInputFormatClass(job, tableAttr, null));
      }

      if (logger.isTraceEnabled()) {
        for (StructField field: finalOI.getAllStructFieldRefs()) {
          logger.trace("field in finalOI: {}", field.getClass().getName());
        }
        logger.trace("partitionSerDe class is {} {}", partitionSerDe.getClass().getName());
      }

      // We should always get the columns names from ObjectInspector. For some of the tables (ex. avro) metastore
      // may not contain the schema, instead it is derived from other sources such as table properties or external file.
      // SerDe object knows how to get the schema with all the config and table properties passed in initialization.
      // ObjectInspector created from the SerDe object has the schema.
      final StructTypeInfo sTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(finalOI);
      final List<String> tableColumnNames = sTypeInfo.getAllStructFieldNames();

      // Select list of columns for project pushdown into Hive SerDe readers.
      final List<Integer> columnIds = Lists.newArrayList();
      if (isStarQuery()) {
        selectedColumnNames = tableColumnNames;
        for(int i=0; i<selectedColumnNames.size(); i++) {
          columnIds.add(i);
        }
      } else {
        selectedColumnNames = Lists.newArrayList();
        for (SchemaPath field : getColumns()) {
          String columnName = field.getRootSegment().getPath();
          columnIds.add(tableColumnNames.indexOf(columnName));
          selectedColumnNames.add(columnName);
        }
      }

      ColumnProjectionUtils.appendReadColumns(job, columnIds, selectedColumnNames);

      List<StructField> selectedStructFieldRefs = new ArrayList<>();
      List<ObjectInspector> selectedColumnObjInspectors = new ArrayList<>();
      List<HiveFieldConverter> selectedColumnFieldConverters = new ArrayList<>();


      for (String columnName : selectedColumnNames) {
        StructField fieldRef = finalOI.getStructFieldRef(columnName);
        selectedStructFieldRefs.add(fieldRef);
        ObjectInspector fieldOI = fieldRef.getFieldObjectInspector();

        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fieldOI.getTypeName());

        selectedColumnObjInspectors.add(fieldOI);
        selectedColumnTypes.add(typeInfo);
        selectedColumnFieldConverters.add(HiveFieldConverter.create(typeInfo, context));
      }

      if (logger.isTraceEnabled()) {
        for(int i=0; i<selectedColumnNames.size(); ++i){
          logger.trace("inspector:typeName={}, className={}, TypeInfo: {}, converter:{}",
              selectedColumnObjInspectors.get(i).getTypeName(),
              selectedColumnObjInspectors.get(i).getClass().getName(),
              selectedColumnTypes.get(i).toString(),
              selectedColumnFieldConverters.get(i).getClass().getName());
        }
      }

      this.selectedStructFieldRefs = selectedStructFieldRefs.toArray(new StructField[selectedStructFieldRefs.size()]);
      this.selectedColumnObjInspectors = selectedColumnObjInspectors.toArray(new ObjectInspector[selectedColumnObjInspectors.size()]);
      this.selectedColumnFieldConverters = selectedColumnFieldConverters.toArray(new HiveFieldConverter[selectedColumnFieldConverters.size()]);

    } catch (Exception e) {
      throw new ExecutionSetupException("Failure while initializing Hive Reader " + this.getClass().getName(), e);
    }

    try {
      final InputSplit inputSplit = deserializeInputSplit(splitAttr.getInputSplit());
      reader = job.getInputFormat().getRecordReader(inputSplit, job, Reporter.NULL);
      if(logger.isTraceEnabled()) {
        logger.trace("hive reader created: {} for inputSplit {}", reader.getClass().getName(), inputSplit.toString());
      }
    } catch (Exception e) {
      throw new ExecutionSetupException("Failed to get o.a.hadoop.mapred.RecordReader from Hive InputFormat", e);
    }

    internalInit(tableProperties, reader);

    List<ValueVector> vectors = new ArrayList<>();
    final OptionManager options = context.getOptions();
    for (int i = 0; i < selectedColumnNames.size(); i++) {
      MajorType type = getMajorTypeFromHiveTypeInfo(selectedColumnTypes.get(i), options);
      Field field = getFieldForNameAndMajorType(selectedColumnNames.get(i), type);
      vectors.add(output.addField(field, ValueVector.class));
    }
    this.vectors = vectors.toArray(new ValueVector[vectors.size()]);

  }

  @Override
  public final int next() {
    try {
      for (ValueVector vv : vectors) {
        AllocationHelper.allocateNew(vv, (int) numRowsPerBatch);
      }

      int records = populateData();

      for (ValueVector v : vectors) {
        v.getMutator().setValueCount(records);
      }

      return records;
    } catch (IOException | SerDeException ex){
      throw UserException.dataReadError(ex).message("Unexpected failure while reading hive table.").build(logger);
    }
  }

  protected abstract int populateData() throws IOException, SerDeException;

  @Override
  public void close() throws IOException {
    if(reader != null){
      reader.close();
    }
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

  /**
   * Utility method which creates a SerDe object for given SerDe class name and properties.
   */
  public static SerDe createSerDe(final JobConf job, final String sLib, final Properties properties) throws Exception {
    final Class<? extends SerDe> c = Class.forName(sLib).asSubclass(SerDe.class);
    final SerDe serde = c.getConstructor().newInstance();
    serde.initialize(job, properties);

    return serde;
  }


  public static StructObjectInspector getStructOI(final SerDe serDe) throws Exception {
    ObjectInspector oi = serDe.getObjectInspector();
    if (oi.getCategory() != Category.STRUCT) {
      throw new UnsupportedOperationException(String.format("%s category not supported", oi.getCategory()));
    }
    return (StructObjectInspector) oi;
  }

  public static Class<? extends InputFormat<?, ?>> getInputFormatClass(final JobConf job, HiveTableXattr tableXattr, HiveSplitXattr xattr) throws Exception {
    if(xattr != null){
      PartitionProp partitionProp = tableXattr.getPartitionPropertiesList().get(xattr.getPartitionId());
      if(partitionProp.hasInputFormat()){
        return (Class<? extends InputFormat<?, ?>>) Class.forName(partitionProp.getInputFormat());
      }

      if(partitionProp.hasStorageHandler()){
        final HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(job, partitionProp.getStorageHandler());
        return (Class<? extends InputFormat<?, ?>>) storageHandler.getInputFormatClass();
      }
    }

    if(tableXattr != null){
      if(tableXattr.hasInputFormat()){
        return (Class<? extends InputFormat<?, ?>>) Class.forName(tableXattr.getInputFormat());
      }

      if(tableXattr.hasStorageHandler()){
        final HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(job, tableXattr.getStorageHandler());
        return (Class<? extends InputFormat<?, ?>>) storageHandler.getInputFormatClass();
      }
    }

    throw new ExecutionSetupException("Unable to get Hive table InputFormat class. There is neither " +
        "InputFormat class explicitly specified nor a StorageHandler class provided.");
  }



  @Override
  protected boolean supportsSkipAllQuery() {
    return true;
  }

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
        HiveUtilities.throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    }

    return null;
  }

  public static MinorType getMinorTypeFromHivePrimitiveTypeInfo(PrimitiveTypeInfo primitiveTypeInfo, OptionManager options) {
    switch(primitiveTypeInfo.getPrimitiveCategory()) {
      case BINARY:
        return MinorType.VARBINARY;
      case BOOLEAN:
        return MinorType.BIT;
      case DECIMAL: {

        if (options.getOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY).bool_val == false) {
          throw UserException.unsupportedError()
              .message(ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG)
              .build(abstractLogger);
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
    HiveUtilities.throwUnsupportedHiveDataTypeError(primitiveTypeInfo.getPrimitiveCategory().toString());
    return null;
  }

}

