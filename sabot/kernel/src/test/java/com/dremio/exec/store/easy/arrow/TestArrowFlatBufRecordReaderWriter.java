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
package com.dremio.exec.store.easy.arrow;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.ExecTest;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.store.RecordWriter;
import com.dremio.io.FSInputStream;
import com.dremio.io.FSOutputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.VectorContainerMutator;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.File;
import java.util.List;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestArrowFlatBufRecordReaderWriter extends ExecTest {

  @Test
  public void testReadWrite() throws Exception {
    @SuppressWarnings("checkstyle:LocalFinalVariableName")
    final int RECORD_COUNT = 4;
    final List<ValueVector> vectorList = Lists.newArrayList();

    try (final IntVector intVector = new IntVector("int", allocator);
        final VarBinaryVector binVector = new VarBinaryVector("binary", allocator)) {
      AllocationHelper.allocate(intVector, 4, 4);
      AllocationHelper.allocate(binVector, 4, 5);
      vectorList.add(intVector);
      vectorList.add(binVector);

      String[] binaryValues = {"ZERO", "ONE", "TWO", "THREE"};

      intVector.setSafe(0, 0);
      binVector.setSafe(0, "ZERO".getBytes(), 0, "ZERO".getBytes().length);
      intVector.setSafe(1, 1);
      binVector.setSafe(1, "ONE".getBytes(), 0, "ONE".getBytes().length);
      intVector.setSafe(2, 2);
      binVector.setSafe(2, "TWO".getBytes(), 0, "TWO".getBytes().length);
      intVector.setSafe(3, 3);
      binVector.setSafe(3, "THREE".getBytes(), 0, "THREE".getBytes().length);
      intVector.setValueCount(4);
      binVector.setValueCount(4);

      VectorContainer container = new VectorContainer();
      container.addCollection(vectorList);
      container.buildSchema();
      container.setRecordCount(RECORD_COUNT);

      Configuration conf = new Configuration();
      conf.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

      try (final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.get(conf))) {
        final File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();

        OperatorContext context = mock(OperatorContext.class);
        when(context.getAllocator()).thenReturn(allocator);

        // Create recordwriter
        FSOutputStream outputStream =
            fs.create(Path.of(tempDir.getAbsolutePath()).resolve("arrowfile"));
        ArrowFlatBufRecordWriter recordWriter = new ArrowFlatBufRecordWriter(context, outputStream);

        RecordWriter.WriteStatsListener byteCountListener = (b) -> {};
        RecordWriter.OutputEntryListener fileWriteListener =
            (recordCount,
                fileSize,
                path,
                metadata,
                partitionNumber,
                icebergMetadata,
                schema,
                partition,
                operationType,
                partitionValue,
                rejectedRecordCount,
                referencedDatafiles) -> {};
        recordWriter.setup(container, fileWriteListener, byteCountListener);

        recordWriter.writeBatch(0, container.getRecordCount());
        recordWriter.close();
        outputStream.close();

        container.close();

        VectorContainer outgoing = new VectorContainer(allocator);

        // create record reader;
        Path p = Path.of(tempDir.getAbsolutePath()).resolve("arrowfile");
        FSInputStream is = fs.open(Path.of(tempDir.getAbsolutePath()).resolve("arrowfile"));
        long size = fs.getFileAttributes(p).size();
        ArrowFlatBufRecordReader recordReader = new ArrowFlatBufRecordReader(context, is, size);

        VectorContainerMutator mutator = new VectorContainerMutator(outgoing);
        recordReader.setup(mutator);

        Assert.assertEquals(1, recordReader.getBatchCount());
        Assert.assertEquals(RECORD_COUNT, recordReader.getRecordBatchSize(0));
        recordReader.next();

        for (VectorWrapper<?> w : outgoing) {
          try (ValueVector vv = w.getValueVector()) {
            int values = vv.getValueCount();
            Assert.assertEquals(4, values);
            for (int i = 0; i < values; i++) {
              final Object o = vv.getObject(i);
              if (o instanceof byte[]) {
                Assert.assertArrayEquals(binaryValues[i].getBytes(), (byte[]) o);
              } else {
                Assert.assertEquals(i, ((Integer) o).intValue());
              }
            }
          }
        }

        outgoing.close();
        recordReader.close();
        is.close();
      }
    }
  }

  private void writeAndVerifyArrowFile(int batchCount) throws Exception {
    // common state
    Configuration conf = new Configuration();
    conf.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    final File tempDir = Files.createTempDir();
    OperatorContext context = mock(OperatorContext.class);
    when(context.getAllocator()).thenReturn(allocator);
    tempDir.deleteOnExit();

    // write arrow file having multiple batches with varying batch counts
    try (final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.get(conf));
        FSOutputStream outputStream =
            fs.create(Path.of(tempDir.getAbsolutePath()).resolve("arrowfile"));
        ArrowFlatBufRecordWriter recordWriter =
            new ArrowFlatBufRecordWriter(context, outputStream); ) {

      for (int batchNo = 1; batchNo <= batchCount; ++batchNo) {
        try (final IntVector intVector = new IntVector("int", allocator);
            final VarBinaryVector binVector = new VarBinaryVector("binary", allocator);
            VectorContainer container = new VectorContainer()) {
          final List<ValueVector> vectorList = Lists.newArrayList();
          vectorList.add(intVector);
          vectorList.add(binVector);
          container.addCollection(vectorList);
          container.buildSchema();
          int batchSize = batchNo * 13;
          for (int row = 0; row < batchSize; ++row) {
            intVector.setSafe(row, row * batchNo);
            String strValue = "A-" + row + "-" + batchNo;
            binVector.setSafe(row, strValue.getBytes(), 0, strValue.length());
          }

          container.setAllCount(batchSize);
          container.setRecordCount(batchSize);

          RecordWriter.WriteStatsListener byteCountListener = (b) -> {};
          RecordWriter.OutputEntryListener fileWriteListener =
              (recordCount,
                  fileSize,
                  path,
                  metadata,
                  partitionNumber,
                  icebergMetadata,
                  schema,
                  partition,
                  operationType,
                  partitionValue,
                  rejectedRecordCount,
                  referencedDataFiles) -> {};
          recordWriter.setup(container, fileWriteListener, byteCountListener);

          recordWriter.writeBatch(0, container.getRecordCount());
        }
      }
    }

    // create arrow file reader and check expected batch counts
    Path p = Path.of(tempDir.getAbsolutePath()).resolve("arrowfile");
    try (final FileSystem fs = HadoopFileSystem.get(org.apache.hadoop.fs.FileSystem.get(conf));
        FSInputStream is = fs.open(Path.of(tempDir.getAbsolutePath()).resolve("arrowfile"));
        VectorContainer outgoing = new VectorContainer(allocator); ) {

      long size = fs.getFileAttributes(p).size();
      try (ArrowFlatBufRecordReader recordReader =
          new ArrowFlatBufRecordReader(context, is, size)) {
        VectorContainerMutator mutator = new VectorContainerMutator(outgoing);
        recordReader.setup(mutator);
        Assert.assertEquals(batchCount, recordReader.getBatchCount());
        for (int batchNo = 0; batchNo < batchCount; ++batchNo) {
          Assert.assertEquals((batchNo + 1) * 13, recordReader.getRecordBatchSize(batchNo));
        }

        for (int batchNo = 1; batchNo <= batchCount; ++batchNo) {
          recordReader.next();
          for (VectorWrapper<?> w : outgoing) {
            try (ValueVector vv = w.getValueVector()) {
              int values = vv.getValueCount();
              Assert.assertEquals(batchNo * 13, values);
              for (int row = 0; row < values; row++) {
                final Object o = vv.getObject(row);
                if (o instanceof byte[]) {
                  String strValue = "A-" + row + "-" + batchNo;
                  Assert.assertArrayEquals(strValue.getBytes(), (byte[]) o);
                } else {
                  Assert.assertEquals(row * batchNo, ((Integer) o).intValue());
                }
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void testBatchCounts() throws Exception {
    writeAndVerifyArrowFile(5);
  }

  @Test
  public void testLargeFileLargeFooter() throws Exception {
    writeAndVerifyArrowFile(1500);
  }

  @Test
  public void testLargeFileMediumFooter() throws Exception {
    writeAndVerifyArrowFile(1000);
  }
}
