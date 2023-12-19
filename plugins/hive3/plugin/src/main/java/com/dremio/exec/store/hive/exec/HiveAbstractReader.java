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
package com.dremio.exec.store.hive.exec;

import static com.dremio.common.util.MajorTypeHelper.getFieldForNameAndMajorType;
import static com.dremio.exec.store.hive.HiveUtilities.addProperties;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.util.Closeable;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.hive.HiveConfFactory;
import com.dremio.exec.store.hive.HiveFsUtils;
import com.dremio.exec.store.hive.HivePf4jPlugin;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.hive.proto.HiveReaderProto.HiveSplitXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValue;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

public abstract class HiveAbstractReader extends AbstractRecordReader {
  protected final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());

  public enum HiveFileFormat {Avro, Orc, Parquet, RCFile, Default, Text};

  protected StructField[] selectedStructFieldRefs;
  protected ObjectInspector[] selectedColumnObjInspectors;
  protected HiveFieldConverter[] selectedColumnFieldConverters;
  protected ValueVector[] vectors;

  protected JobConf jobConf;

  protected AbstractSerDe tableSerDe;
  protected StructObjectInspector tableOI;

  protected AbstractSerDe partitionSerDe;
  protected StructObjectInspector partitionOI;

  protected ScanFilter filter;

  // Final ObjectInspector. We may not use the partitionOI directly if there are schema changes between the table and
  // partition. If there are no schema changes then this is same as the partitionOI.
  protected StructObjectInspector finalOI;

  private final SplitAndPartitionInfo split;
  private final HiveTableXattr tableAttr;
  private final Collection<List<String>> referencedTables;
  private final UserGroupInformation readerUgi;
  protected HiveOperatorContextOptions operatorContextOptions;

  public HiveAbstractReader(final HiveTableXattr tableAttr, final SplitAndPartitionInfo split,
                            final List<SchemaPath> projectedColumns, final OperatorContext context, final JobConf jobConf,
                            final AbstractSerDe tableSerDe, final StructObjectInspector tableOI, final AbstractSerDe partitionSerDe,
                            final StructObjectInspector partitionOI, final ScanFilter filter,
                            final Collection<List<String>> referencedTables, final UserGroupInformation readerUgi) {
    super(context, projectedColumns);
    this.tableAttr = tableAttr;
    this.split = split;
    this.jobConf = new JobConf(HiveFsUtils.getClonedConfWithDremioWrapperFs(jobConf));
    this.tableSerDe = tableSerDe;
    this.tableOI = tableOI;
    this.partitionSerDe = partitionSerDe == null ? tableSerDe : partitionSerDe;
    this.partitionOI = partitionOI == null ? tableOI : partitionOI;
    this.filter = filter;
    this.referencedTables = referencedTables;
    this.readerUgi = readerUgi;
  }

  @Override
  public final void setup(OutputMutator output) {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      final HiveSplitXattr splitAttr;
      try {
        splitAttr = HiveSplitXattr.parseFrom(split.getDatasetSplitInfo().getExtendedProperty());
      } catch (InvalidProtocolBufferException e) {
        throw createExceptionWithContext("Failure deserializing Hive extended attributes.", e);
      }

      addProperties(jobConf, null, HiveReaderProtoUtil.getTableProperties(tableAttr));

      List<String> selectedColumnNames;
      List<TypeInfo> selectedColumnTypes = new ArrayList<>();

      try {
        if (partitionSerDe != null) {
          finalOI = (StructObjectInspector) ObjectInspectorConverters.getConvertedOI(partitionOI, tableOI);
        } else {
          finalOI = tableOI;
        }

        if (logger.isTraceEnabled()) {
          for (StructField field : finalOI.getAllStructFieldRefs()) {
            logger.trace("field in finalOI: {}", field.getClass().getName());
          }
          logger.trace("partitionSerDe class is {}", partitionSerDe.getClass().getName());
        }

        // We should always get the columns names from ObjectInspector. For some of the tables (ex. avro) metastore
        // may not contain the schema, instead it is derived from other sources such as table properties or external file.
        // AbstractSerDe object knows how to get the schema with all the config and table properties passed in initialization.
        // ObjectInspector created from the AbstractSerDe object has the schema.
        final StructTypeInfo sTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(finalOI);
        final List<String> tableColumnNames = sTypeInfo.getAllStructFieldNames();

        // Select list of columns for project pushdown into Hive AbstractSerDe readers.
        final List<Integer> columnIds = Lists.newArrayList();
        selectedColumnNames = Lists.newArrayList();
        for (SchemaPath field : getColumns()) {
          String columnName = field.getRootSegment().getPath();
          if (!selectedColumnNames.contains(columnName)) {
            columnIds.add(tableColumnNames.indexOf(columnName));
            selectedColumnNames.add(columnName);
          }
        }

        ColumnProjectionUtils.appendReadColumns(jobConf, columnIds);

        List<StructField> selectedStructFieldRefs = new ArrayList<>();
        List<ObjectInspector> selectedColumnObjInspectors = new ArrayList<>();
        List<HiveFieldConverter> selectedColumnFieldConverters = new ArrayList<>();
        this.operatorContextOptions = new HiveOperatorContextOptions(context, jobConf, getHiveFileFormat());

        for (String columnName : selectedColumnNames) {
          StructField fieldRef = finalOI.getStructFieldRef(columnName);
          selectedStructFieldRefs.add(fieldRef);
          ObjectInspector fieldOI = fieldRef.getFieldObjectInspector();

          TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fieldOI.getTypeName());

          selectedColumnObjInspectors.add(fieldOI);
          selectedColumnTypes.add(typeInfo);
          selectedColumnFieldConverters.add(HiveFieldConverter.create(typeInfo, context, this.operatorContextOptions));
        }

        if (logger.isTraceEnabled()) {
          for (int i = 0; i < selectedColumnNames.size(); ++i) {
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
        throw createExceptionWithContext("Failure while initializing Hive Reader", e);
      }

      this.vectors = new ValueVector[selectedColumnNames.size()];
      final OptionManager options = context.getOptions();
      int i = 0;
      Set<String> seenColumns = new HashSet<>();
      for (SchemaPath selectedColumn : getColumns()) {
        final String colName = selectedColumn.getRootSegment().getPath();
        if (seenColumns.contains(colName)) {
          continue;
        }
        seenColumns.add(colName);
        MajorType type = HiveUtilities.getMajorTypeFromHiveTypeInfo(selectedColumnTypes.get(i), options);
        Field field = getFieldForNameAndMajorType(colName, type);
        vectors[i] = output.addField(field, ValueVector.class);
        i++;
      }

      try {
        final InputSplit inputSplit = HiveUtilities.deserializeInputSplit(splitAttr.getInputSplit());
        final PrivilegedExceptionAction<Void> internalInitAction = () -> {
          internalInit(inputSplit, jobConf, vectors);
          return null;
        };
        readerUgi.doAs(internalInitAction);
      } catch (Exception e) {
        throw createExceptionWithContext("Failed to initialize Hive record reader", e);
      }
    }
  }

  protected abstract void internalInit(InputSplit inputSplit, JobConf jobConf, ValueVector[] vectors) throws IOException;

  @Override
  public final int next() {
    try {
      return populateData();
    } catch (Throwable ex){
      throw createExceptionWithContext("Unexpected failure while reading hive table.", ex);
    }
  }

  protected abstract int populateData() throws IOException, SerDeException;

  protected abstract HiveFileFormat getHiveFileFormat();

  @Override
  public void close() throws IOException {
    // This is important to null out these to
    selectedStructFieldRefs = null;
    selectedColumnObjInspectors = null;
    selectedColumnFieldConverters = null;
    vectors = null;
    tableSerDe = null;
    tableOI = null;
    partitionSerDe = null;
    partitionOI = null;
    finalOI = null;
    filter = null;
  }

  @Override
  protected boolean supportsSkipAllQuery() {
    return true;
  }

  /**
   * Helper method which create a {@link UserException} with useful context for trouble shooting purposes when an error
   * occurs in Hive readers.
   * @param errorMessage
   * @param t (optional) exception thrown in the error context
   * @return {@link UserException} with context
   */

  protected void logDebugMessages() {
    logger.debug("Class loader is {}", this.getClass().getClassLoader().toString());
  }

  UserException createExceptionWithContext(String errorMessage, Throwable t) {
    if(logger.isDebugEnabled()) {
      logDebugMessages();
    }
    if (t instanceof FileNotFoundException) {
      return UserException.invalidMetadataError(t)
        .message(errorMessage)
        .addContext("Dataset split key", split.getPartitionInfo().getSplitKey())
        .setAdditionalExceptionContext(new InvalidMetadataErrorContext(ImmutableList.copyOf(referencedTables)))
        .build(logger);
    } else {
      UserException.Builder builder = UserException.dataReadError(t)
        .message(errorMessage)
        .addContext("Dataset split key", split.getPartitionInfo().getSplitKey());
      final List<PartitionValue> partitionValues = split.getPartitionInfo().getValuesList();
      if (partitionValues != null && !partitionValues.isEmpty()) {
        final String partition = Joiner.on(",").join(
          Iterables.transform(partitionValues, new Function<PartitionValue, String>() {
              @Override
              public String apply(@Nullable PartitionValue input) {
                final Object value;
                if (input.hasBinaryValue()) {
                  value = input.getBinaryValue();
                } else if (input.hasBitValue()) {
                  value = input.getBitValue();
                } else if (input.hasIntValue()) {
                  value = input.getIntValue();
                } else if (input.hasDoubleValue()) {
                  value = input.getDoubleValue();
                } else if (input.hasFloatValue()) {
                  value = input.getFloatValue();
                } else if (input.hasLongValue()) {
                  value = input.getLongValue();
                } else if (input.hasStringValue()) {
                  value = input.getStringValue();
                } else {
                  value = "";
                }

                return input.getColumn() + "=" + String.valueOf(value);
              }
            }
          )
        );
        builder = builder.addContext("Partition values", partition);
      }

      String tableProperties = HiveReaderProtoUtil.getTableProperties(tableAttr)
        .map(input -> input.getKey() + " -> " + input.getValue())
        .collect(Collectors.joining("\n"));
      builder = builder.addContext("Table properties", tableProperties);
      return builder.build(logger);
    }
  }

  public static class HiveOperatorContextOptions {
    private int maxCellSize;
    private int hiveVersion = 3;
    private HiveFileFormat hiveFileFormat;
    public HiveOperatorContextOptions(OperatorContext context, JobConf jobConf, HiveFileFormat hiveFileFormat) {
      if (HiveConfFactory.isHive2SourceType(jobConf)) {
        this.hiveVersion = 2;
      }
      this.maxCellSize = Math.toIntExact(context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
      this.hiveFileFormat = hiveFileFormat;
    }
    public int getMaxCellSize() {
      return maxCellSize;
    }

    public int getHiveVersion() {
      return hiveVersion;
    }

    public HiveFileFormat getHiveFileFormat() {
      return hiveFileFormat;
    }
  }
}
