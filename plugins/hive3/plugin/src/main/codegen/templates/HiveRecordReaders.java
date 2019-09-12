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

/**
 * This template is used to generate different Hive record reader classes for different data formats
 * to avoid JIT profile pullusion. These readers are derived from HiveAbstractReader which implements
 * codes for init and setup stage, but the repeated - and performance critical part - next() method is
 * separately implemented in the classes generated from this template. The internal SkipRecordReeader
 * class is also separated as well due to the same reason.
 *
 * As to the performance gain with this change, please refer to:
 * https://issues.apache.org/jira/browse/DRILL-4982
 *
 */
<@pp.dropOutputFile />
<#list hiveFormat.map as entry>
<@pp.changeOutputFile name="//com/dremio/exec/store/hive/exec/Hive${entry.hiveReader}Reader.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store.hive.exec;

import java.io.IOException;
import java.util.List;
import java.util.Collection;

import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.ValueVector;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.hive.Hive3PluginOptions;
import com.dremio.exec.store.hive.exec.HiveProxyingOrcScanFilter;
import com.dremio.exec.store.hive.exec.apache.HadoopFileSystemWrapper;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.scan.ScanOperator.Metric;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;

import org.apache.hadoop.fs.FSError;
<#if entry.hiveReader == "Orc">
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
</#if>
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
<#if entry.hiveReader == "Orc">
import org.apache.hadoop.hive.ql.io.orc.Reader;
</#if>
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
<#if entry.hiveReader == "Orc">
import org.apache.hadoop.io.NullWritable;
</#if>
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
<#if entry.hiveReader == "Orc">
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.DataReaderProperties;
</#if>
public class Hive${entry.hiveReader}Reader extends HiveAbstractReader {

<#-- Specialized reader for Hive ORC, allowing zero copy to use the Dremio allocator -->
<#if entry.hiveReader == "Orc">
  <#assign KeyType="NullWritable">
  <#assign ValueType="OrcStruct">
<#else>
  <#assign KeyType="Object">
  <#assign ValueType="Object">
</#if>
  private ${KeyType} key;
  private ${ValueType} value;
  private RecordReader<${KeyType}, ${ValueType}> reader;
  <#if entry.hiveReader == "Orc">
  private DremioORCRecordUtils.DefaultDataReader dataReader;
  </#if>
  // Converter which converts data from partition schema to table schema.
  protected Converter partTblObjectInspectorConverter;

  public Hive${entry.hiveReader}Reader(final HiveTableXattr tableAttr, final SplitAndPartitionInfo split,
      final List<SchemaPath> projectedColumns, final OperatorContext context, final JobConf jobConf,
      final AbstractSerDe tableSerDe, final StructObjectInspector tableOI, final AbstractSerDe partitionSerDe,
      final StructObjectInspector partitionOI, final ScanFilter filter, final Collection<List<String>> referencedTables,
      final UserGroupInformation readerUgi) {
    super(tableAttr, split, projectedColumns, context, jobConf, tableSerDe, tableOI, partitionSerDe, partitionOI, filter,
      referencedTables, readerUgi);
  }

  public void internalInit(InputSplit inputSplit, JobConf jobConf, ValueVector[] vectors) throws IOException {

<#-- Push down the filter in Hive ORC AbstractSerDe reader -->
<#if entry.hiveReader == "Orc">
    if (filter != null && filter instanceof HiveProxyingOrcScanFilter) {
      final HiveProxyingOrcScanFilter scanFilter = (HiveProxyingOrcScanFilter) filter;
      jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, jobConf.get(serdeConstants.LIST_COLUMNS));
      jobConf.set(ConvertAstToSearchArg.SARG_PUSHDOWN, scanFilter.getProxiedOrcScanFilter().getKryoBase64EncodedFilter());
    }

    final OrcFile.ReaderOptions opts = OrcFile.readerOptions(jobConf);

    final OrcSplit fSplit = (OrcSplit)inputSplit;
    final org.apache.hadoop.fs.Path path = fSplit.getPath();
    // TODO: DX-16001 make enabling async configurable.

    final FileSystem fs = path.getFileSystem(jobConf);
    final HadoopFileSystemWrapper hfs = new HadoopFileSystemWrapper(jobConf, fs);
    opts.filesystem(hfs);

    final Reader hiveReader = OrcFile.createReader(path, opts);

    final List<OrcProto.Type> types = hiveReader.getTypes();

    final Reader.Options options = new Reader.Options();
    Boolean zeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(jobConf);
    Boolean useDirectMemory = context.getOptions().getOption(Hive3PluginOptions.HIVE_ORC_READER_USE_DIRECT_MEMORY);
    dataReader = DremioORCRecordUtils.createDefaultDataReader(context.getAllocator(), DataReaderProperties.builder()
      .withBufferSize(hiveReader.getCompressionSize())
      .withCompression(hiveReader.getCompressionKind())
      .withFileSystem(fs)
      .withPath(path)
      .withTypeCount(types.size())
      .withZeroCopy(zeroCopy)
      .build(), useDirectMemory);
    options.dataReader(dataReader);
    reader = ((OrcInputFormat)jobConf.getInputFormat()).getRecordReader(inputSplit, jobConf, Reporter.NULL, options);
<#else>
    reader = jobConf.getInputFormat().getRecordReader(inputSplit, jobConf, Reporter.NULL);
</#if>

    if(logger.isTraceEnabled()) {
      logger.trace("hive reader created: {} for inputSplit {}", reader.getClass().getName(), inputSplit.toString());
    }

    this.key = reader.createKey();
    this.value = reader.createValue();

<#-- Hive ORC reader already handles the schema changes, so we do not need to create a type converter -->
<#if entry.hiveReader != "Orc">
    if (!partitionOI.equals(finalOI)) {
      // If the partition and table have different schemas, create a converter
      partTblObjectInspectorConverter = ObjectInspectorConverters.getConverter(partitionOI, finalOI);
    }
</#if>
  }

  @Override
  public int populateData() throws IOException, SerDeException {
    final RecordReader<${KeyType}, ${ValueType}> reader = this.reader;
    final Converter partTblObjectInspectorConverter = this.partTblObjectInspectorConverter;
    final int numRowsPerBatch = (int) this.numRowsPerBatch;

    final StructField[] selectedStructFieldRefs = this.selectedStructFieldRefs;
    final AbstractSerDe partitionSerDe = this.partitionSerDe;
    final StructObjectInspector finalOI = this.finalOI;
    final ObjectInspector[] selectedColumnObjInspectors = this.selectedColumnObjInspectors;
    final HiveFieldConverter[] selectedColumnFieldConverters = this.selectedColumnFieldConverters;
    final ValueVector[] vectors = this.vectors;
    final ${KeyType} key = this.key;
    final ${ValueType} value = this.value;

    int recordCount = 0;
    while (recordCount < numRowsPerBatch) {
      try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(this.context.getStats())) {
        boolean hasNext = reader.next(key, value);
        if (!hasNext) {
          break;
        }
      }
      catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
      Object deSerializedValue = partitionSerDe.deserialize((Writable) value);
      if (partTblObjectInspectorConverter != null) {
        deSerializedValue = partTblObjectInspectorConverter.convert(deSerializedValue);
      }
      for (int i = 0; i < selectedStructFieldRefs.length; i++) {
        Object hiveValue = finalOI.getStructFieldData(deSerializedValue, selectedStructFieldRefs[i]);
        if (hiveValue != null) {
          selectedColumnFieldConverters[i].setSafeValue(selectedColumnObjInspectors[i], hiveValue, vectors[i], recordCount);
        }
      }
      recordCount++;
    }
    for (int i = 0; i < selectedStructFieldRefs.length; i++) {
      vectors[i].setValueCount(recordCount);
    }

    return recordCount;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      <#if entry.hiveReader == "Orc">
      reader.close();
      <#else>
      try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(this.context.getStats())){
        reader.close();
      }
      catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
      </#if>
      reader = null;
    }
<#if entry.hiveReader == "Orc">
  if (dataReader != null) {
  if (dataReader.isRemoteRead()) {
  context.getStats().addLongStat(ScanOperator.Metric.NUM_REMOTE_READERS, 1);
  } else {
  context.getStats().addLongStat(ScanOperator.Metric.NUM_REMOTE_READERS, 0);
  }
  dataReader = null;
  }
</#if>
    this.partTblObjectInspectorConverter = null;
    super.close();
  }
}
</#list>
