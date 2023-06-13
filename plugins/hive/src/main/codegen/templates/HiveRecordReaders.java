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
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.hive.HiveSettings;
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
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.AcidUtils;
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
<#if entry.hiveReader == "Orc">
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.DataReaderProperties;
</#if>
import org.apache.hadoop.security.UserGroupInformation;

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

<#-- Push down the filter in Hive ORC SerDe reader -->
<#if entry.hiveReader == "Orc">
    /*
     * There are three cases possible if acid table gets updated
     * and dremio metadata for file paths becomes invalid as hive
     * would have compacted the data files that store the table data.
     *
     * (1) We started with a base file, the table was updated that led
     * to some new delta files and eventually compaction cleaned up everything
     * into a new single base file (with new name). This is something we
     * already handled in DX-14414 since the read will be done by hive orc
     * vectorized reader which will try to create inner record reader
     * as OrcFile.createReader(split_path, opts) and this will attempt
     * to open the non-existent old base file. We will catch FileNotFound
     * exception which will then be rethrown as invalid metadata error
     * and dremio will refresh the acid table metadata before reattempting.

     * (2) We started with a set of delta files, the table was updated that
     * led to new delta files and compaction cleaned up everything into a
     * single base file. Since the old
     * metadata references delta files, we will use the non-vectorized orc
     * reader. Unlike (1), here the reader creation happens internally in
     * OrcInputFormal class. There we check for the existence of each delta
     * file and so if none of the delta files exist, we get an
     * empty reader. This is why reflection refresh on an acid table was returning
     * 0 bytes. Since dremio doesn't catch any exception during creation
     * of reader using input split path, there is no way
     * for us to rethrow a invalid metadata error and refresh/reattempt.
     * This problem is fixed in DX-16380. The non vectorized orc reader
     * checks for the existence of each delta file before trying to create
     * the reader and explicitly throws FileNotFound exception which
     * is then handled by HiveAbstractReader apropriately.

     * (3) We have a combination of base+delta files. This is similar to (2)
     * since as long as there is one delta file in our metadata, we will
     * use the non vectorized reader.
     */
    AcidUtils.setTransactionalTableRaiseErrorIfDeltaFileNotFound(jobConf, true);
    final Reader.Options options = new Reader.Options();
    if ( !((OrcInputFormat)jobConf.getInputFormat()).isAcidRead(jobConf, inputSplit) ) {
      if (filter != null && filter instanceof HiveProxyingOrcScanFilter) {
        final HiveProxyingOrcScanFilter scanFilter = (HiveProxyingOrcScanFilter) filter;
        jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, jobConf.get(serdeConstants.LIST_COLUMNS));
        jobConf.set(ConvertAstToSearchArg.SARG_PUSHDOWN, scanFilter.getProxiedOrcScanFilter().getKryoBase64EncodedFilter());
      }

      final OrcFile.ReaderOptions opts = OrcFile.readerOptions(jobConf);

      final OrcSplit fSplit = (OrcSplit)inputSplit;
      final org.apache.hadoop.fs.Path path = fSplit.getPath();
      // TODO: DX-16001 make enabling async configurable.
      final HadoopFileSystemWrapper fs = new HadoopFileSystemWrapper(path, jobConf, this.context.getStats());
        opts.filesystem(fs);
      final Reader hiveReader = OrcFile.createReader(path, opts);

      final List<OrcProto.Type> types = hiveReader.getTypes();

      final Boolean zeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(jobConf);
      final boolean useDirectMemory = new HiveSettings(context.getOptions()).useDirectMemoryForOrcReaders();
      dataReader = DremioORCRecordUtils.createDefaultDataReader(context.getAllocator(), DataReaderProperties.builder()
        .withBufferSize(hiveReader.getCompressionSize())
        .withCompression(hiveReader.getCompressionKind())
        .withFileSystem(fs)
        .withPath(path)
        .withTypeCount(types.size())
        .withZeroCopy(zeroCopy)
        .build(), useDirectMemory, context.getOptions().getOption(ExecConstants.SCAN_COMPUTE_LOCALITY));
      options.dataReader(dataReader);
    }
    try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(this.context.getStats())) {
      reader = ((OrcInputFormat)jobConf.getInputFormat()).getRecordReader(inputSplit, jobConf, Reporter.NULL, options);
    } catch (FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
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
      } catch (FSError e) {
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
      try (OperatorStats.WaitRecorder recorder = OperatorStats.getWaitRecorder(this.context.getStats())){
        reader.close();
      } catch (FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
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
