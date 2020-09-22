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
package com.dremio.service.jobs;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.store.RecordWriter.OutputEntryListener;
import com.dremio.exec.store.RecordWriter.WriteStatsListener;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.exec.store.easy.arrow.ArrowFileReader;
import com.dremio.exec.store.easy.arrow.ArrowFormatPluginConfig;
import com.dremio.exec.store.easy.arrow.ArrowRecordWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link ArrowFileReader}
 */
public class TestArrowFileReader extends DremioTest {
  @Rule
  public TemporaryFolder dateGenFolder = new TemporaryFolder();

  private static final Configuration FS_CONF = new Configuration();

  private static final List<Boolean> TEST_BIT_VALUES = new ArrayList<>(5);
  private static final List<String> TEST_VARCHAR_VALUES = new ArrayList<>(5);

  static {
    TEST_BIT_VALUES.add(true);
    TEST_BIT_VALUES.add(false);
    TEST_BIT_VALUES.add(null);
    TEST_BIT_VALUES.add(true);
    TEST_BIT_VALUES.add(true);

    TEST_VARCHAR_VALUES.add("value1");
    TEST_VARCHAR_VALUES.add("long long long long long long long long long long long long long long long long value");
    TEST_VARCHAR_VALUES.add("long long long long value");
    TEST_VARCHAR_VALUES.add(null);
    TEST_VARCHAR_VALUES.add("l");

    // to accommodate MapR profile that sets default FS to maprfs, while we use local FS for testing here
    FS_CONF.set("fs.default.name","file:///");
  }

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  /**
   * Test reading a arrow file that contains just a single empty record batch (possible when the query/fragment returns
   * no results).
   * @throws Exception
   */
  @Test
  public void readingZeroRecordFile() throws Exception {
    VectorContainer batchData = null;
    try (final BufferAllocator allocator = allocatorRule.newAllocator("test-arrow-file-reader", 0, Long.MAX_VALUE)) {
      // generate a test file with just the empty record batch
      Path basePath = new Path(dateGenFolder.getRoot().getPath());

      batchData = createBatch(0,
          new BitVector("colBit", allocator),
          new VarCharVector("colVarChar", allocator),
          testEmptyListVector(allocator),
          testEmptyUnionVector(allocator));
      ArrowFileMetadata metadata = writeArrowFile(batchData);
      try(ArrowFileReader reader = new ArrowFileReader(HadoopFileSystem.getLocal(FS_CONF), com.dremio.io.file.Path.of(basePath.toUri()), metadata, allocator)) {
        {
          List<RecordBatchHolder> batchHolders = getRecords(reader, 0, 0, allocator);
          assertEquals(1, batchHolders.size());

          //verifyBatchHolder(batchHolders.get(0), 0, 0);

          BatchSchema schema = batchHolders.get(0).getData().getContainer().getSchema();
          assertEquals(4, schema.getFieldCount());

          assertEquals("colBit", schema.getColumn(0).getName());
          assertEquals(MinorType.BIT, Types.getMinorTypeForArrowType(schema.getColumn(0).getType()));

          assertEquals("colVarChar", schema.getColumn(1).getName());
          assertEquals(MinorType.VARCHAR, Types.getMinorTypeForArrowType(schema.getColumn(1).getType()));

          assertEquals("emptyListVector", schema.getColumn(2).getName());
          assertEquals(MinorType.LIST, Types.getMinorTypeForArrowType(schema.getColumn(2).getType()));

          assertEquals("unionVector", schema.getColumn(3).getName());
          assertEquals(MinorType.UNION, Types.getMinorTypeForArrowType(schema.getColumn(3).getType()));
          assertEquals(MinorType.INT, Types.getMinorTypeForArrowType(schema.getColumn(3).getChildren().get(0).getType()));
          assertEquals(MinorType.DECIMAL, Types.getMinorTypeForArrowType(schema.getColumn(3).getChildren().get(1).getType()));

          releaseBatches(batchHolders);
        }
        {
          try {
            reader.read(0, 1);
            fail("shouldn't be here");
          } catch (IllegalArgumentException e) {
            assertEquals("Invalid start index (0) and limit (1) combination. Record count in file (0)", e.getMessage());
          }
        }
        {
          try {
            reader.read(1, 1);
            fail("shouldn't be here");
          } catch (IllegalArgumentException e) {
            assertEquals("Invalid start index (1). Record count in file (0)", e.getMessage());
          }
        }
      }
    } finally {
      if (batchData != null) {
        batchData.clear();
      }
    }
  }

  @Test
  public void readingSingleBatchFile() throws Exception {
    VectorContainer batchData = null;
    try (final BufferAllocator allocator = allocatorRule.newAllocator("test-arrow-file-reader", 0, Long.MAX_VALUE)) {
      Path basePath = new Path(dateGenFolder.getRoot().getPath());
      // generate a test file with just a single batch with 5 records.
      batchData = createBatch(5, testBitVector(allocator), testVarCharVector(allocator));

      ArrowFileMetadata metadata = writeArrowFile(batchData);
      try(ArrowFileReader reader = new ArrowFileReader(HadoopFileSystem.getLocal(FS_CONF), com.dremio.io.file.Path.of(basePath.toUri()), metadata, allocator)) {
        {
          // Get everything
          List<RecordBatchHolder> batchHolders = getRecords(reader, 0, 5, allocator);
          assertEquals(1, batchHolders.size());

          verifyBatchHolder(batchHolders.get(0), 0, 5);

          VectorContainer batchContainer = batchHolders.get(0).getData().getContainer();
          assertEquals(TEST_BIT_VALUES, getBitValues(batchContainer, 0, 5));
          assertEquals(TEST_VARCHAR_VALUES, getVarCharValues(batchContainer, 0, 5));
          releaseBatches(batchHolders);
        }
        {
          // Get a part of the batch starting from beginning
          List<RecordBatchHolder> batchHolders = getRecords(reader, 0, 2, allocator);
          assertEquals(1, batchHolders.size());

          verifyBatchHolder(batchHolders.get(0), 0, 2);

          VectorContainer batchContainer = batchHolders.get(0).getData().getContainer();
          assertEquals(TEST_BIT_VALUES.subList(0, 2), getBitValues(batchContainer, 0, 2));
          assertEquals(TEST_VARCHAR_VALUES.subList(0, 2), getVarCharValues(batchContainer, 0, 2));
          releaseBatches(batchHolders);
        }
        {
          // Get a part of the batch starting from the middle of the batch to end of the batch
          List<RecordBatchHolder> batchHolders = getRecords(reader, 2,2, allocator);
          assertEquals(1, batchHolders.size());

          verifyBatchHolder(batchHolders.get(0), 2, 4);

          VectorContainer batchContainer = batchHolders.get(0).getData().getContainer();
          assertEquals(TEST_BIT_VALUES.subList(2, 4), getBitValues(batchContainer, 2, 4));
          assertEquals(TEST_VARCHAR_VALUES.subList(2, 4), getVarCharValues(batchContainer, 2, 4));
          releaseBatches(batchHolders);
        }
        {
          try {
            reader.read(-1, 1);
            fail("shouldn't be here");
          } catch (IllegalArgumentException e) {
            assertEquals("Invalid start index (-1). Record count in file (5)", e.getMessage());
          }
        }
        {
          try {
            reader.read(6, 1);
            fail("shouldn't be here");
          } catch (IllegalArgumentException e) {
            assertEquals("Invalid start index (6). Record count in file (5)", e.getMessage());
          }
        }
      }

    } finally {
      if (batchData != null) {
        batchData.clear();
      }
    }
  }

  @Test
  public void readingMultiBatchFile() throws Exception {
    List<VectorContainer> containers = Lists.newArrayList();
    try (final BufferAllocator allocator = allocatorRule.newAllocator("test-arrow-file-reader", 0, Long.MAX_VALUE)) {
      // generate a test file with multiple record batches each containing 5 records.
      containers.add(createBatch(5, testBitVector(allocator), testVarCharVector(allocator)));
      containers.add(createBatch(5, testBitVector(allocator), testVarCharVector(allocator)));
      containers.add(createBatch(5, testBitVector(allocator), testVarCharVector(allocator)));

      Path basePath = new Path(dateGenFolder.getRoot().getPath());
      ArrowFileMetadata metadata = writeArrowFile(containers.toArray(new VectorContainer[3]));
      try(ArrowFileReader reader = new ArrowFileReader(HadoopFileSystem.getLocal(FS_CONF), com.dremio.io.file.Path.of(basePath.toUri()), metadata, allocator)) {
        {
          // Get everything
          List<RecordBatchHolder> batchHolders = getRecords(reader, 0, 15, allocator);
          assertEquals(3, batchHolders.size());

          for(int i=0; i<3; i++) {
            verifyBatchHolder(batchHolders.get(i), 0, 5);

            VectorContainer batchContainer = batchHolders.get(i).getData().getContainer();
            assertEquals(TEST_BIT_VALUES, getBitValues(batchContainer, 0, 5));
            assertEquals(TEST_VARCHAR_VALUES, getVarCharValues(batchContainer, 0, 5));
          }

          releaseBatches(batchHolders);
        }
        {
          // Get a part of the batch starting from beginning spanning two batches
          List<RecordBatchHolder> batchHolders = getRecords(reader, 0, 7, allocator);
          assertEquals(2, batchHolders.size());

          verifyBatchHolder(batchHolders.get(0), 0, 5);

          VectorContainer batchContainer = batchHolders.get(0).getData().getContainer();
          assertEquals(TEST_BIT_VALUES, getBitValues(batchContainer, 0, 5));
          assertEquals(TEST_VARCHAR_VALUES, getVarCharValues(batchContainer, 0, 5));

          verifyBatchHolder(batchHolders.get(1), 0, 2);

          batchContainer = batchHolders.get(1).getData().getContainer();
          assertEquals(TEST_BIT_VALUES.subList(0, 2), getBitValues(batchContainer, 0, 2));
          assertEquals(TEST_VARCHAR_VALUES.subList(0, 2), getVarCharValues(batchContainer, 0, 2));

          releaseBatches(batchHolders);
        }
        {
          // Get a part of the batch starting from the middle of the first batch to middle of the third batch
          List<RecordBatchHolder> batchHolders = getRecords(reader, 2, 11, allocator);
          assertEquals(3, batchHolders.size());

          verifyBatchHolder(batchHolders.get(0), 2, 5);
          VectorContainer batchContainer = batchHolders.get(0).getData().getContainer();
          assertEquals(TEST_BIT_VALUES.subList(2, 5), getBitValues(batchContainer, 2, 5));
          assertEquals(TEST_VARCHAR_VALUES.subList(2, 5), getVarCharValues(batchContainer, 2, 5));

          verifyBatchHolder(batchHolders.get(1), 0, 5);
          batchContainer = batchHolders.get(1).getData().getContainer();
          assertEquals(TEST_BIT_VALUES, getBitValues(batchContainer, 0, 5));
          assertEquals(TEST_VARCHAR_VALUES, getVarCharValues(batchContainer, 0, 5));

          verifyBatchHolder(batchHolders.get(2), 0, 3);
          batchContainer = batchHolders.get(2).getData().getContainer();
          assertEquals(TEST_BIT_VALUES.subList(0, 3), getBitValues(batchContainer, 0, 3));
          assertEquals(TEST_VARCHAR_VALUES.subList(0, 3), getVarCharValues(batchContainer, 0, 3));

          releaseBatches(batchHolders);
        }
        {
          // Get a part of the batch starting from the middle of the second batch to middle of thrid batch
          List<RecordBatchHolder> batchHolders = getRecords(reader, 7, 5, allocator);
          assertEquals(2, batchHolders.size());

          verifyBatchHolder(batchHolders.get(0), 2, 5);
          VectorContainer batchContainer = batchHolders.get(0).getData().getContainer();
          assertEquals(TEST_BIT_VALUES.subList(2, 5), getBitValues(batchContainer, 2, 5));
          assertEquals(TEST_VARCHAR_VALUES.subList(2, 5), getVarCharValues(batchContainer, 2, 5));

          verifyBatchHolder(batchHolders.get(1), 0, 2);
          batchContainer = batchHolders.get(1).getData().getContainer();
          assertEquals(TEST_BIT_VALUES.subList(0, 2), getBitValues(batchContainer, 0, 2));
          assertEquals(TEST_VARCHAR_VALUES.subList(0, 2), getVarCharValues(batchContainer, 0, 2));

          releaseBatches(batchHolders);
        }
      }
    } finally {
      for(VectorContainer container : containers) {
        if (container != null) {
          container.clear();
        }
      }
    }
  }

  @Test
  public void writeAndReadEmptyListVectors() throws Exception {
    try (final BufferAllocator allocator = allocatorRule.newAllocator("test-arrow-file-reader", 0, Long.MAX_VALUE);
         final VectorContainer batchData = createBatch(1, testEmptyListVector(allocator))) {

      final Path basePath = new Path(dateGenFolder.getRoot().getPath());
      final ArrowFileMetadata metadata = writeArrowFile(batchData);
      try (final ArrowFileReader reader =
               new ArrowFileReader(HadoopFileSystem.getLocal(FS_CONF), com.dremio.io.file.Path.of(basePath.toUri()), metadata, allocator)) {

        final List<RecordBatchHolder> batchHolders = reader.read(0, 1);
        assertEquals(1, batchHolders.size());
        assertNotNull(batchHolders.get(0).getData());
        assertEquals(0, batchHolders.get(0).getStart());
        assertEquals(1, batchHolders.get(0).getEnd());

        final BatchSchema schema = batchHolders.get(0).getData().getContainer().getSchema();
        assertEquals(1, schema.getFieldCount());
        assertEquals("emptyListVector", schema.getColumn(0).getName());
        assertEquals(MinorType.LIST, Types.getMinorTypeForArrowType(schema.getColumn(0).getType()));

        final VectorContainer batchContainer = batchHolders.get(0).getData().getContainer();
        assertTrue(Iterators.size(batchContainer.iterator()) == 1);
        for (final VectorWrapper<?> wrapper : batchContainer) {
          assertTrue(wrapper.getValueVector() instanceof ListVector);
          assertTrue(((ListVector) (wrapper.getValueVector())).getDataVector() instanceof NullVector);
        }

        releaseBatches(batchHolders);
      }
    }
  }

  /** Helper method which creates a test bit vector */
  private static BitVector testBitVector(BufferAllocator allocator) {
    BitVector colBitV = new BitVector("colBit", allocator);
    colBitV.allocateNew(5);
    for(int i=0; i<TEST_BIT_VALUES.size(); i++) {
      if (TEST_BIT_VALUES.get(i) == null) {
        colBitV.setNull(i);
      } else {
        colBitV.set(i, TEST_BIT_VALUES.get(i) ? 1 : 0);
      }
    }

    return colBitV;
  }

  /** Helper method which creates a test varchar vector */
  private static VarCharVector testVarCharVector(BufferAllocator allocator) {
    VarCharVector colVarCharV = new VarCharVector("colVarChar", allocator);
    colVarCharV.allocateNew(500, 5);
    for(int i=0; i<TEST_VARCHAR_VALUES.size(); i++) {
      if (TEST_VARCHAR_VALUES.get(i) == null) {
        colVarCharV.setNull(i);
      } else {
        colVarCharV.set(i, TEST_VARCHAR_VALUES.get(i).getBytes());
      }
    }

    return colVarCharV;
  }

  /** Helper method which creates a empty list vector */
  private static ListVector testEmptyListVector(BufferAllocator allocator) {
    final ListVector vector =
        new ListVector("emptyListVector", allocator, FieldType.nullable(ArrowType.Null.INSTANCE), null);
    vector.allocateNew();
    return vector;
  }

  /** Helper method which creates a union vector with no data */
  private static UnionVector testEmptyUnionVector(BufferAllocator allocator) {
    final UnionVector unionVector = new UnionVector("unionVector", allocator, null);
    unionVector.initializeChildrenFromFields(
        asList(
            Field.nullable("intType", new ArrowType.Int(32, true)),
            Field.nullable("decimalType", new ArrowType.Decimal(4, 10))
        )
    );

    return unionVector;
  }

  /** Helper method to get the values in given range in colBit vector used in this test class. */
  private static List<Boolean> getBitValues(VectorContainer container, int start, int end) {
    FieldReader reader = container.getValueAccessorById(BitVector.class, 0).getValueVector().getReader();

    List<Boolean> values = Lists.newArrayList();
    for(int i=start; i<end; i++) {
      reader.setPosition(i);
      if (reader.isSet()) {
        values.add(reader.readBoolean());
      } else {
        values.add(null);
      }
    }

    return values;
  }

  /** Helper method to get the values in given range in colVarChar vector used in this test class. */
  private static List<String> getVarCharValues(VectorContainer container, int start, int end) {
    FieldReader reader = container.getValueAccessorById(VarCharVector.class, 1).getValueVector().getReader();

    List<String> values = Lists.newArrayList();
    for(int i=start; i<end; i++) {
      reader.setPosition(i);
      if (reader.isSet()) {
        final Text val = reader.readText();
        values.add(val == null ? null : val.toString());
      } else {
        values.add(null);
      }
    }

    return values;
  }

  /**
   * Helper method to verify that the batch holder contains valid data including the standard two columns
   * (colBit - BIT, colVarChar - VARCHAR) used in this test class.
   */
  private static void verifyBatchHolder(RecordBatchHolder holder, int expStart, int expEnd) {
    assertNotNull(holder);
    assertNotNull(holder.getData());
    assertEquals(expStart, holder.getStart());
    assertEquals(expEnd, holder.getEnd());

    // verify schema
    BatchSchema schema = holder.getData().getContainer().getSchema();
    assertEquals(2, schema.getFieldCount());

    assertEquals("colBit", schema.getColumn(0).getName());
    assertEquals(MinorType.BIT, Types.getMinorTypeForArrowType(schema.getColumn(0).getType()));

    assertEquals("colVarChar", schema.getColumn(1).getName());
    assertEquals(MinorType.VARCHAR, Types.getMinorTypeForArrowType(schema.getColumn(1).getType()));
  }

  protected static void releaseBatches(List<RecordBatchHolder> holders) throws Exception {
    for(RecordBatchHolder holder : holders) {
      holder.getData().close();
    }
  }

  private static VectorContainer createBatch(int recordCount, ValueVector... vv) {
    VectorContainer container = new VectorContainer();
    if (recordCount != 0) {
      for (ValueVector v : vv) {
        v.setValueCount(recordCount);
      }
    }
    container.addCollection(asList(vv));
    container.setRecordCount(recordCount);
    container.buildSchema(SelectionVectorMode.NONE);

    return container;
  }

  public OperatorContext getOperatorContext() {
    return Mockito.mock(OperatorContext.class);
  }

  /** Helper method that write the given batches to a file with given name and returns the file metadata */
  private ArrowFileMetadata writeArrowFile(VectorContainer... batches) throws Exception {
    OperatorContext opContext = getOperatorContext();
    when(opContext.getFragmentHandle()).thenReturn(FragmentHandle.newBuilder().setMajorFragmentId(2323).setMinorFragmentId(234234).build());

    final EasyWriter writerConf = mock(EasyWriter.class);
    when(writerConf.getLocation()).thenReturn(dateGenFolder.getRoot().toString());
    when(writerConf.getProps()).thenReturn(OpProps.prototype());

    final EasyFormatPlugin formatPlugin = mock(EasyFormatPlugin.class);
    final FileSystemPlugin fsPlugin = mock(FileSystemPlugin.class);
    when(writerConf.getFormatPlugin()).thenReturn(formatPlugin);
    when(formatPlugin.getFsPlugin()).thenReturn(fsPlugin);
    when(fsPlugin.createFS(notNull(String.class), notNull(OperatorContext.class))).thenReturn(HadoopFileSystem.getLocal(FS_CONF));

    ArrowRecordWriter writer = new ArrowRecordWriter(
        opContext,
        writerConf,
        new ArrowFormatPluginConfig());

    OutputEntryListener outputEntryListener = Mockito.mock(OutputEntryListener.class);
    WriteStatsListener writeStatsListener = Mockito.mock(WriteStatsListener.class);
    ArgumentCaptor<Long> recordWrittenCaptor = ArgumentCaptor.forClass(long.class);
    ArgumentCaptor<Long> fileSizeCaptor = ArgumentCaptor.forClass(long.class);
    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<byte[]> metadataCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> partitionCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> bytesWrittenCaptor = ArgumentCaptor.forClass(long.class);
    ArgumentCaptor<byte[]> icebergMetadataCaptor = ArgumentCaptor.forClass(byte[].class);

    final VectorContainer incoming = batches[0];
    writer.setup(incoming, outputEntryListener, writeStatsListener);
    writer.writeBatch(0, incoming.getRecordCount());

    for(int i=1; i<batches.length; i++) {
      incoming.clear();
      for(VectorWrapper<?> vw : batches[i]) {
        incoming.add(vw.getValueVector());
      }
      writer.writeBatch(0, incoming.getRecordCount());
    }
    incoming.clear();

    writer.close();

    verify(outputEntryListener, times(1)).recordsWritten(recordWrittenCaptor.capture(),
      fileSizeCaptor.capture(), pathCaptor.capture(), metadataCaptor.capture(),
      partitionCaptor.capture(), icebergMetadataCaptor.capture());
    verify(writeStatsListener, times(batches.length)).bytesWritten(bytesWrittenCaptor.capture());

    Path path = new Path(dateGenFolder.getRoot().getPath());
    FileSystem fs = path.getFileSystem(FS_CONF);
    for (FileStatus file : fs.listStatus(path)) {
      assertEquals(Long.valueOf(fileSizeCaptor.getValue()), Long.valueOf(file.getLen()));
    }

    ArrowFileMetadata arrowFileMetadata =
      ArrowFileReader.toBean(ArrowFileFormat.ArrowFileMetadata.parseFrom(metadataCaptor.getValue()));

    assertArrowFileMetadata(arrowFileMetadata);
    return arrowFileMetadata;
  }

  public void assertArrowFileMetadata(ArrowFileMetadata arrowFileMetadata) {
     // no-op. This is overridden in derived class.
  }

  public List<RecordBatchHolder> getRecords(ArrowFileReader reader, long start, long limit, BufferAllocator allocator) throws Exception {
    return reader.read(start, limit);
  }
}
