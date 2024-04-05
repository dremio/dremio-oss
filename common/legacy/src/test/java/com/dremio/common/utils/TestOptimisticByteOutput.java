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
package com.dremio.common.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import com.dremio.common.SuppressForbidden;
import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Test;

/** Test {@link ByteOutput} implementation API usage. */
@SuppressForbidden
public class TestOptimisticByteOutput {
  private static final byte[] smallData = new byte[] {0, 1, 2, 3, 0, 127};

  @Test
  public void testWriteLazyArray() throws IOException {
    // Arrange

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length);
    byteOutput.writeLazy(smallData, 0, smallData.length);
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertSame(smallData, outputData);
    assertArrayEquals(smallData, outputData);
  }

  @Test
  public void testWriteArray() throws IOException {
    // Arrange

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length);
    byteOutput.write(smallData, 0, smallData.length);
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertNotSame(smallData, outputData);
    assertArrayEquals(smallData, outputData);
  }

  @Test
  public void testWriteLazyByteBuffer() throws IOException {
    // Arrange
    final ByteBuffer buffer = ByteBuffer.allocate(smallData.length);
    buffer.put(smallData);
    buffer.flip();

    // Act
    final OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length);
    byteOutput.writeLazy(buffer);
    final byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertSame(buffer.array(), outputData);
    assertArrayEquals(smallData, outputData);

    assertEquals(smallData.length, buffer.limit());
    assertEquals(smallData.length, buffer.position());
    assertEquals(smallData.length, buffer.capacity());
    assertEquals(0, buffer.remaining());
  }

  @Test
  public void testWriteLazyDirectByteBuffer() throws IOException {
    // Arrange
    final ByteBuffer buffer = ByteBuffer.allocateDirect(smallData.length);
    buffer.put(smallData);
    buffer.flip();

    // Act
    final OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length);
    byteOutput.writeLazy(buffer);
    final byte[] outputData = byteOutput.toByteArray();

    // Assert
    byte[] compare = new byte[smallData.length];
    buffer.flip();
    buffer.get(compare);
    assertNotSame(compare, outputData);
    assertArrayEquals(smallData, outputData);

    assertEquals(smallData.length, buffer.limit());
    assertEquals(smallData.length, buffer.position());
    assertEquals(smallData.length, buffer.capacity());
    assertEquals(0, buffer.remaining());
  }

  @Test
  public void testWriteByteBuffer() throws IOException {
    // Arrange
    ByteBuffer buffer = ByteBuffer.allocate(smallData.length);
    buffer.put(smallData);
    buffer.flip();

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length);
    byteOutput.write(buffer);
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertNotSame(buffer.array(), outputData);
    assertArrayEquals(smallData, outputData);

    assertEquals(smallData.length, buffer.limit());
    assertEquals(smallData.length, buffer.position());
    assertEquals(smallData.length, buffer.capacity());
    assertEquals(0, buffer.remaining());
  }

  @Test
  public void testWriteByteBufferMultipleTimes() throws IOException {
    // Arrange
    int size = smallData.length * 4;
    ByteBuffer originalBuffer = ByteBuffer.allocate(smallData.length);
    originalBuffer.put(smallData);
    originalBuffer.flip();
    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(size);
    for (int offset = 0; offset < size; offset += smallData.length) {
      ByteBuffer buffer = originalBuffer.slice();

      byteOutput.write(buffer);

      // Early Assert
      assertEquals(smallData.length, buffer.limit());
      assertEquals(smallData.length, buffer.position());
      assertEquals(smallData.length, buffer.capacity());
      assertEquals(0, buffer.remaining());
    }
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    ByteBuffer fourTimes = ByteBuffer.allocate(size);
    fourTimes.put(smallData);
    fourTimes.put(smallData);
    fourTimes.put(smallData);
    fourTimes.put(smallData);
    byte[] biggerData = fourTimes.array();
    assertNotSame(biggerData, outputData);
    assertArrayEquals(biggerData, outputData);
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteByteBufferMultipleTimesReadEarly() throws IOException {
    // Arrange
    int size = smallData.length * 4;
    ByteBuffer buffer = ByteBuffer.allocate(smallData.length);
    buffer.put(smallData);
    buffer.flip();

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(size);
    for (int offset = 0; offset < smallData.length; offset += smallData.length) {
      byteOutput.write(buffer);
    }

    // Assert
    assertEquals(smallData.length, buffer.limit());
    assertEquals(smallData.length, buffer.position());
    assertEquals(smallData.length, buffer.capacity());
    byte[] outputData = byteOutput.toByteArray();
  }

  @Test
  public void testWrite() throws IOException {
    // Arrange

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length);
    for (byte b : smallData) {
      byteOutput.write(b);
    }
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertNotSame(smallData, outputData);
    assertArrayEquals(smallData, outputData);
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteReadEarly() {
    // Arrange

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length);
    byteOutput.write(smallData[0]);
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertNotSame(smallData, outputData);
    assertArrayEquals(smallData, outputData);
  }

  @Test
  public void testWriteArrayMultipleTimes() throws IOException {
    // Arrange
    int size = smallData.length * 4;
    ByteBuffer fourTimes = ByteBuffer.allocate(size);
    fourTimes.put(smallData);
    fourTimes.put(smallData);
    fourTimes.put(smallData);
    fourTimes.put(smallData);
    byte[] biggerData = fourTimes.array();

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(size);
    for (int offset = 0; offset < size; offset += smallData.length) {
      byteOutput.write(biggerData, offset, smallData.length);
    }
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertNotSame(biggerData, outputData);
    assertArrayEquals(biggerData, outputData);
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteArrayMultipleTimesReadEarly() throws IOException {
    // Arrange
    int size = smallData.length * 4;
    ByteBuffer fourTimes = ByteBuffer.allocate(size);
    fourTimes.put(smallData);
    fourTimes.put(smallData);
    fourTimes.put(smallData);
    fourTimes.put(smallData);
    byte[] biggerData = fourTimes.array();

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(size);
    for (int offset = 0; offset < size / 2; offset += smallData.length) {
      byteOutput.write(biggerData, offset, smallData.length);
    }
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertNotSame(biggerData, outputData);
    assertArrayEquals(biggerData, outputData);
  }

  @Test
  public void testWriteEmptyArray() throws IOException {
    // Arrange

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(0);
    byteOutput.write(new byte[5], 4, 0);
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertArrayEquals(new byte[0], outputData);
  }

  @Test
  public void testWriteEmptyBuffer() throws IOException {
    // Arrange

    // Act
    OptimisticByteOutput byteOutput = new OptimisticByteOutput(0);
    byteOutput.write(ByteBuffer.allocate(0));
    byte[] outputData = byteOutput.toByteArray();

    // Assert
    assertArrayEquals(new byte[0], outputData);
  }

  @Test
  public void testLiteralByteString() throws Exception {
    ByteString literal = ByteString.copyFrom(smallData);

    OptimisticByteOutput byteOutput = new OptimisticByteOutput(literal.size());
    UnsafeByteOperations.unsafeWriteTo(literal, byteOutput);

    byte[] array = (byte[]) FieldUtils.readField(literal, "bytes", true);
    assertArrayEquals(smallData, byteOutput.toByteArray());
    assertSame(array, byteOutput.toByteArray());
  }

  @Test
  public void testRopeByteString() throws Exception {
    ByteString literal = ByteString.copyFrom(smallData);

    ByteString data = literal;
    for (int i = 0; i < 3; i++) {
      data = data.concat(literal);
    }

    final byte[] expected;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      for (int i = 0; i < 4; i++) {
        baos.write(smallData);
      }
      expected = baos.toByteArray();
    }

    OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length * 4);
    UnsafeByteOperations.unsafeWriteTo(data, byteOutput);

    assertArrayEquals(expected, byteOutput.toByteArray());
  }

  private static final Method NEW_ROPE_BYTE_STRING_INSTANCE;

  static {
    try {
      Class<?> clazz = Class.forName("com.google.protobuf.RopeByteString");
      Method method =
          clazz.getDeclaredMethod("newInstanceForTest", ByteString.class, ByteString.class);
      method.setAccessible(true);
      NEW_ROPE_BYTE_STRING_INSTANCE = method;
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testRopeByteStringWithZeroOnLeft() throws Exception {
    ByteString literal = ByteString.copyFrom(smallData);

    ByteString data =
        (ByteString) NEW_ROPE_BYTE_STRING_INSTANCE.invoke(null, ByteString.EMPTY, literal);

    OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length);
    UnsafeByteOperations.unsafeWriteTo(data, byteOutput);

    byte[] array = (byte[]) FieldUtils.readField(literal, "bytes", true);
    assertArrayEquals(smallData, byteOutput.toByteArray());
    assertSame(array, byteOutput.toByteArray());
  }

  @Test
  public void testRopeByteStringWithZeroOnRight() throws Exception {
    ByteString literal = ByteString.copyFrom(smallData);

    ByteString data =
        (ByteString) NEW_ROPE_BYTE_STRING_INSTANCE.invoke(null, literal, ByteString.EMPTY);

    OptimisticByteOutput byteOutput = new OptimisticByteOutput(smallData.length);
    UnsafeByteOperations.unsafeWriteTo(data, byteOutput);

    byte[] array = (byte[]) FieldUtils.readField(literal, "bytes", true);
    assertArrayEquals(smallData, byteOutput.toByteArray());
    assertSame(array, byteOutput.toByteArray());
  }
}
