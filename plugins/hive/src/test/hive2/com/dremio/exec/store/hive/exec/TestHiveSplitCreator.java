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

import static org.apache.hadoop.hive.ql.io.IOConstants.AVRO;
import static org.apache.hadoop.hive.ql.io.IOConstants.ORC;
import static org.apache.hadoop.hive.ql.io.IOConstants.PARQUET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.junit.Test;

import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.exec.store.hive.metadata.ParquetInputFormat;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class TestHiveSplitCreator {

  private static final long splitSize = 256*1024*1024;
  private static final long mtime = 0;
  private static final long splitOffset = 0;
  private static final long fileSize = 1000;

  @Test
  public void testParquetSplit() throws IOException, ReflectiveOperationException {

    SplitAndPartitionInfo datasetSplit = buildSplit(PARQUET);
    InputSplit inputSplit = extractSplit(datasetSplit);

    assertTrue(inputSplit instanceof ParquetInputFormat.ParquetSplit);

    ParquetInputFormat.ParquetSplit split = (ParquetInputFormat.ParquetSplit) inputSplit;
    assertEquals(mtime, split.getModificationTime());
    assertEquals(splitSize, split.getLength());
  }

  @Test
  public void testOrcSplit() throws IOException, ReflectiveOperationException {

    SplitAndPartitionInfo datasetSplit = buildSplit(ORC);
    InputSplit inputSplit = extractSplit(datasetSplit);

    assertTrue(inputSplit instanceof OrcSplit);

    OrcSplit split = (OrcSplit) inputSplit;
    assertEquals(splitSize, split.getLength());
    assertEquals(fileSize, split.getFileLength());
  }

  @Test
  public void testAvroSplit() throws IOException, ReflectiveOperationException {

    SplitAndPartitionInfo datasetSplit = buildSplit(AVRO);
    InputSplit inputSplit = extractSplit(datasetSplit);

    assertTrue(inputSplit instanceof FileSplit);

    FileSplit split = (FileSplit) inputSplit;
    assertEquals(splitSize, split.getLength());
    assertEquals(splitOffset, split.getStart());
  }

  @Test (expected = UnsupportedOperationException.class)
  public void testUnknownSplit() throws InvalidProtocolBufferException {
    buildSplit("UNKNOWN");
  }

  private static SplitAndPartitionInfo buildSplit(String fileFormat) throws InvalidProtocolBufferException {
    final int id = 1;
    String extendedProp = "partition_extended_" + id;
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId(String.valueOf(id))
      .setSize(1000)
      .setExtendedProperty(ByteString.copyFrom(extendedProp.getBytes()))
      .build();

    SplitIdentity splitIdentity = new SplitIdentity("/path/to/data-1", splitOffset, splitSize, fileSize);

    HiveReaderProto.HiveTableXattr.Builder tableXattrbuilder = HiveReaderProto.HiveTableXattr.newBuilder();
    tableXattrbuilder.setInputFormat("TempString");
    tableXattrbuilder.setStorageHandler("TempString");
    tableXattrbuilder.setSerializationLib("TempString");

    OperatorContext operatorContext = mock(OperatorContextImpl.class);
    OptionManager optionManager = mock(OptionManager.class);
    when(operatorContext.getOptions()).thenReturn(optionManager);
    when(optionManager.getOption(ExecConstants.ORC_SPLIT_SIZE)).thenReturn(ExecConstants.ORC_SPLIT_SIZE_VALIDATOR.getDefault());
    when(optionManager.getOption(ExecConstants.PARQUET_SPLIT_SIZE)).thenReturn(ExecConstants.PARQUET_SPLIT_SIZE_VALIDATOR.getDefault());
    when(optionManager.getOption(ExecConstants.AVRO_SPLIT_SIZE)).thenReturn(ExecConstants.AVRO_SPLIT_SIZE_VALIDATOR.getDefault());

    final HiveSplitCreator splitCreator = new HiveSplitCreator(operatorContext, tableXattrbuilder.build().toByteArray());
    return splitCreator.createSplit(partitionInfo, splitIdentity, fileFormat, fileSize, mtime);
  }

  private static InputSplit extractSplit(SplitAndPartitionInfo datasetSplit) throws IOException, ReflectiveOperationException {
    HiveReaderProto.HiveSplitXattr hiveSplitXattr = HiveReaderProto.HiveSplitXattr.parseFrom(datasetSplit.getDatasetSplitInfo().getExtendedProperty());
    return HiveUtilities.deserializeInputSplit(hiveSplitXattr.getInputSplit());
  }
}
