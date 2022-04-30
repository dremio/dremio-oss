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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.ExecTest;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.google.common.collect.Lists;
import com.google.common.io.Files;


public class TestVectorAccessibleFlatBufSerializable extends ExecTest {

  @Test
  public void testReadWrite() throws Exception {
    final List<ValueVector> vectorList = Lists.newArrayList();

    try (final IntVector intVector = new IntVector("int", allocator);
         final VarBinaryVector binVector =
           new VarBinaryVector("binary", allocator)) {
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
      container.setRecordCount(4);

      VectorAccessibleFlatBufSerializable writeSerializable = new VectorAccessibleFlatBufSerializable(container, allocator);

      Configuration conf = new Configuration();
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

      try (final FileSystem fs = FileSystem.get(conf)) {
        final File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        final Path path = new Path(tempDir.getAbsolutePath(), "dremioSerializable");
        try (final FSDataOutputStream out = fs.create(path)) {
          writeSerializable.writeToStream(out);
        }

        container.zeroVectors();
        VectorAccessibleFlatBufSerializable readSerializable = new VectorAccessibleFlatBufSerializable(container, allocator);
        try (final FSDataInputStream in = fs.open(path)) {
          readSerializable.readFromStream(in);
        }
      }


      Assert.assertEquals(4, container.getRecordCount());
      for (VectorWrapper<?> w : container) {
        try (ValueVector vv = w.getValueVector()) {
          int values = vv.getValueCount();
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

      container.close();
    }
  }

  @Test
  public void testListReadWrite() throws Exception {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try (ListVector listVector = ListVector.empty("listInt", allocator)) {
      listVector.allocateNew();

      // write data to vector
      // [[0, 0, 0, 0, 0, null], [0, 1, 2, 3, 4, null], [0, 2, 4, 6, 8, null], [null, null, null, null, null],
      // [0, null, 2, null, 4, null], null]

      UnionListWriter writer = listVector.getWriter();
      for (int i = 0; i < 4; i++) {
        writer.startList();
        writer.setPosition(i);
        for (int j = 0; j < 5; j++) {
          if (i != 3) {
            writer.writeInt(j * i);
          } else {
            writer.writeNull();
          }
        }
        writer.writeNull();
        writer.setValueCount(6);
        writer.endList();
      }
      writer.startList();
      writer.setPosition(4);
      for(int i = 0; i < 6; i++) {
        if(i%2 == 0) {
          writer.writeInt(i);
        } else {
          writer.writeNull();
        }
      }
      writer.setValueCount(6);
      writer.endList();

      writer.startList();
      writer.setPosition(5);
      writer.writeNull();
      writer.setValueCount(1);
      writer.endList();

      listVector.setValueCount(6);
      VectorContainer container = new VectorContainer();
      container.add(listVector);
      container.buildSchema();
      container.setRecordCount(6);


      VectorAccessibleFlatBufSerializable writeSerializable = new VectorAccessibleFlatBufSerializable(container, allocator);

      Configuration conf = new Configuration();
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

      VectorContainer readContainer = new VectorContainer(allocator);
      readContainer.setRecordCount(container.getRecordCount());
      readContainer.addSchema(container.getSchema());
      readContainer.buildSchema();

      try (final FileSystem fs = FileSystem.get(conf)) {
        final File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        final Path path = new Path(tempDir.getAbsolutePath(), "dremioSerializable");
        try (final FSDataOutputStream out = fs.create(path)) {
          writeSerializable.writeToStream(out);
        }

        VectorAccessibleFlatBufSerializable readSerializable = new VectorAccessibleFlatBufSerializable(readContainer, allocator);
        try (final FSDataInputStream in = fs.open(path)) {
          readSerializable.readFromStream(in);
        }
      }

      ArrayList<ArrayList<Integer>> expectedOutPut = new ArrayList<ArrayList<Integer>>();
      expectedOutPut.add(new ArrayList<Integer>(Arrays.asList(0, 0, 0, 0, 0, null)));
      expectedOutPut.add(new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4, null)));
      expectedOutPut.add(new ArrayList<Integer>(Arrays.asList(0, 2, 4, 6, 8, null)));
      expectedOutPut.add(new ArrayList<Integer>(Arrays.asList(null, null, null, null, null, null)));
      expectedOutPut.add(new ArrayList<Integer>(Arrays.asList(0, null, 2, null, 4, null)));

      ArrayList<Integer> nullList = new ArrayList<>();
      nullList.add(null);
      expectedOutPut.add(nullList);

      Assert.assertEquals(6, readContainer.getRecordCount());
      for (VectorWrapper<?> w : readContainer) {
        try (ListVector vv = (ListVector)w.getValueVector()) {
          for (int i = 0; i < listVector.getValueCount(); i++) {
            Assert.assertTrue(listVector.getObject(i).equals(expectedOutPut.get(i)));
          }
        }
      }

    }
  }
}
