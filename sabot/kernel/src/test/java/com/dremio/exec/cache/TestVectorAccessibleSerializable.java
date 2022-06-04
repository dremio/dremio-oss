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
package com.dremio.exec.cache;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static com.dremio.exec.cache.VectorAccessibleSerializable.readIntoArrowBuf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecTest;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.SabotNode;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestVectorAccessibleSerializable extends ExecTest {
  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(90, TimeUnit.SECONDS); // 90secs

  @Test
  public void test() throws Exception {
    final List<ValueVector> vectorList = Lists.newArrayList();
    try (final ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
        final SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true)) {
      bit.run();
      final SabotContext context = bit.getContext();

      try (final IntVector intVector = new IntVector("int", allocator);
           final VarBinaryVector binVector =
              new VarBinaryVector("binary", allocator)) {
        AllocationHelper.allocate(intVector, 4, 4);
        AllocationHelper.allocate(binVector, 4, 5);
        vectorList.add(intVector);
        vectorList.add(binVector);

        intVector.setSafe(0, 0);
        binVector.setSafe(0, "ZERO".getBytes(), 0, "ZERO".getBytes().length);
        intVector.setSafe(1, 1);
        binVector.setSafe(1, "ONE".getBytes(), 0, "ONE".getBytes().length);
        intVector.setSafe(2, 2);
        binVector.setSafe(2, "TWO".getBytes(), 0, "TWO".getBytes().length);
        intVector.setSafe(3, 3);
        binVector.setSafe(3, "THREE".getBytes(), 0, "TWO".getBytes().length);
        intVector.setValueCount(4);
        binVector.setValueCount(4);

        VectorContainer container = new VectorContainer();
        container.addCollection(vectorList);
        container.setRecordCount(4);
        WritableBatch batch = WritableBatch.getBatchNoHVWrap(container.getRecordCount(), container, false);
        VectorAccessibleSerializable wrap = new VectorAccessibleSerializable(batch, allocator);

        Configuration conf = new Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

        final VectorAccessibleSerializable newWrap = new VectorAccessibleSerializable(allocator);
        try (final FileSystem fs = FileSystem.get(conf)) {
          final File tempDir = Files.createTempDir();
          tempDir.deleteOnExit();
          final Path path = new Path(tempDir.getAbsolutePath(), "dremioSerializable");
          try (final FSDataOutputStream out = fs.create(path)) {
            wrap.writeToStream(out);
            out.close();
          }

          try (final FSDataInputStream in = fs.open(path)) {
            newWrap.readFromStream(in);
          }
        }

        final VectorAccessible newContainer = newWrap.get();
        for (VectorWrapper<?> w : newContainer) {
          try (ValueVector vv = w.getValueVector()) {
            int values = vv.getValueCount();
            for (int i = 0; i < values; i++) {
              final Object o = vv.getObject(i);
              if (o instanceof byte[]) {
                System.out.println(new String((byte[]) o));
              } else {
                System.out.println(o);
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void testCompressSerDe() throws Exception {
    testCompressSerDeHelper(10);
    testCompressSerDeHelper(100);
    testCompressSerDeHelper(1000);
    testCompressSerDeHelper(1024);
    testCompressSerDeHelper(8000);
    testCompressSerDeHelper(10000);
    testCompressSerDeHelper(16000);
    testCompressSerDeHelper(32000);
    testCompressSerDeHelper(64000);
  }

  private void testCompressSerDeHelper(int records) throws Exception {
    final List<ValueVector> vectorList = Lists.newArrayList();
    try (final ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
         final SabotNode bit = new SabotNode(DEFAULT_SABOT_CONFIG, clusterCoordinator, CLASSPATH_SCAN_RESULT, true)) {
      bit.run();
      final SabotContext context = bit.getContext();

      final FieldType mapType = CompleteType.struct(
          CompleteType.VARCHAR.toField("varchar"),
          CompleteType.INT.toField("int"),
          CompleteType.BIT.asList().toField("bits")
      ).toField("map").getFieldType();

      try (final IntVector intVector = new IntVector("int", allocator);
           final Float8Vector float8Vector = new Float8Vector("float8", allocator);
           final StructVector mapVector = new StructVector("map", allocator, mapType, null);
           final ArrowBuf tempBuf = allocator.buffer(2048)) {
        AllocationHelper.allocateNew(intVector, records);
        AllocationHelper.allocateNew(float8Vector, records);
        AllocationHelper.allocateNew(mapVector, records);
        vectorList.add(intVector);
        vectorList.add(float8Vector);
        vectorList.add(mapVector);

        int intBaseValue = 100;
        double doubleBaseValue = 100.375;
        NullableStructWriter mapWriter = mapVector.getWriter();
        for (int i = 0; i < records; i++) {
          intVector.set(i, intBaseValue + i);
          float8Vector.set(i, doubleBaseValue + i);

          mapWriter.setPosition(i);
          byte[] bytes = ("varchar" + i).getBytes();
          tempBuf.setBytes(0, bytes, 0, bytes.length);
          mapWriter.varChar("varchar").writeVarChar(0, bytes.length, tempBuf);
          mapWriter.integer("int").writeInt(i);
          ListWriter listWriter = mapWriter.list("bits");
          listWriter.startList();
          listWriter.setPosition(i);
          final Boolean[] bits = new Boolean[] { true, false};
          for(int j = 0; j < bits.length; j++) {
            listWriter.bit().writeBit(bits[j] ? 1 : 0);
          }
          listWriter.endList();
          // weird call to mark the call explicitly not-null??
          mapVector.setIndexDefined(i);
        }

        intVector.setValueCount(records);
        float8Vector.setValueCount(records);
        mapVector.setValueCount(records);

        for (int i = 0; i < records; i++) {
          assertEquals(intBaseValue + i, intVector.get(i));
          assertEquals(doubleBaseValue + i, float8Vector.get(i), 0);
          assertEquals(
              mapOf(
                  "varchar", "varchar" + i,
                  "int", i,
                  "bits", listOf(true, false)
              ), mapVector.getObject(i)
          );
        }

        SerDe(vectorList, true, records, context, 100, 100.375);

      }
    }
  }

  private void SerDe(List<ValueVector> vectorList, boolean compression, int records, SabotContext context,
                     int intBaseValue, double doubleBaseValue) throws Exception {
    VectorContainer container = new VectorContainer();
    container.addCollection(vectorList);
    container.setRecordCount(records);
    WritableBatch batch = WritableBatch.getBatchNoHVWrap(container.getRecordCount(), container, false);
    VectorAccessibleSerializable wrap = new VectorAccessibleSerializable(batch, null, allocator, compression);

    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

    final VectorAccessibleSerializable newWrap = new VectorAccessibleSerializable(allocator, true, allocator);
    try (final FileSystem fs = FileSystem.get(conf)) {
      final File tempDir = Files.createTempDir();
      tempDir.deleteOnExit();
      final Path path = new Path(tempDir.getAbsolutePath(), "dremioSerializable");
      try (final FSDataOutputStream out = fs.create(path)) {
        wrap.writeToStream(out);
        out.close();
      }

      try (final FSDataInputStream in = fs.open(path)) {
        newWrap.readFromStream(in);
      }
    }

    final VectorAccessible newContainer = newWrap.get();
    for (VectorWrapper<?> w : newContainer) {
      try (ValueVector vv = w.getValueVector()) {
        int values = vv.getValueCount();
        for (int i = 0; i < values; i++) {
          final Object o = vv.getObject(i);
          if (o instanceof Integer) {
            assertEquals(intBaseValue + i, ((Integer) o).intValue());
          } else if (o instanceof  Double) {
            assertEquals(doubleBaseValue + i, ((Double) o).doubleValue(), 0);
          } else {
            assertEquals(
                mapOf(
                    "varchar", "varchar" + i,
                    "int", i,
                    "bits", listOf(true, false)
                ),
                o);
          }
        }
      }
    }
  }

  @Test
  public void testReadIntoArrowBuf() throws Exception {
    try (final ArrowBuf buffer = allocator.buffer(256)) {
      final InputStream inputStream = mock(InputStream.class);
      when(inputStream.read(any(byte[].class))).thenReturn(0);
      readIntoArrowBuf(inputStream, buffer, 0);
      assertEquals(0, buffer.writerIndex());
    }

    try (final ArrowBuf buffer = allocator.buffer(256)) {
      final InputStream inputStream = mock(InputStream.class);
      when(inputStream.read(any(byte[].class), any(int.class), any(int.class))).thenAnswer(new Answer() {
        @Override
        public Integer answer(InvocationOnMock invocation) throws Throwable {
          byte[] byteBuf = invocation.getArgument(0, byte[].class);
          int start = invocation.getArgument(1, Integer.class);
          int length = invocation.getArgument(2, Integer.class);
          for(int i = start; i < Math.min(length, byteBuf.length); i++) {
            byteBuf[i] = (byte)i;
          }
          return Math.min(length, byteBuf.length);
        }
      });
      readIntoArrowBuf(inputStream, buffer, 256);
      assertEquals(256, buffer.writerIndex());
      for(int i=0; i<256; i++) {
        assertEquals((byte)i, buffer.getByte(i));
      }
    }

    try (final ArrowBuf buffer = allocator.buffer(256)) {
      final InputStream inputStream = mock(InputStream.class);
      when(inputStream.read(any(byte[].class), any(int.class), any(int.class))).thenAnswer(new Answer() {
        @Override
        public Integer answer(InvocationOnMock invocation) throws Throwable {
          byte[] byteBuf = invocation.getArgument(0, byte[].class);
          int start = invocation.getArgument(1, Integer.class);
          int length = invocation.getArgument(2, Integer.class);
          int i=start;
          int toFill = Math.min(byteBuf.length, 20);
          toFill = Math.min(toFill, length);
          while(i<toFill) {
            byteBuf[i] = (byte)i;
            i++;
          }
          return i;
        }
      });
      readIntoArrowBuf(inputStream, buffer, 256);
      assertEquals(256, buffer.writerIndex());
      for(int i=0; i<256; i++) {
        assertEquals((byte)(i%20), buffer.getByte(i));
      }
    }

    try (final ArrowBuf buffer = allocator.buffer(256)) {
      final InputStream inputStream = mock(InputStream.class);
      when(inputStream.read(any(byte[].class), any(int.class), any(int.class))).thenReturn(-1);
      try {
        readIntoArrowBuf(inputStream, buffer, 256);
        fail("Expected above call to fail");
      } catch (EOFException ex) {
        /* expected*/
      }
    }
  }
}
