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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotSame;

import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test {@link ByteOutput} implementation with various payload sizes through UnsafeByteOperations.
 */
@RunWith(Parameterized.class)
public class TestOptimisticByteOutputUnsafeByteOperations {

  private final byte[] data;

  public TestOptimisticByteOutputUnsafeByteOperations(int size) {
    data = getData(size);
  }

  private byte[] getData(int size) {
    byte[] bytes = new byte[size];
    new Random(1230).nextBytes(bytes);
    return bytes;
  }

  @Parameterized.Parameters(name = "TestOptimisticByteOutputUnsafeByteOperations-Payload-{0}")
  public static Collection<Integer> sizes() {
    return asList(0, 1, 2, 3, 100, 255, 512, 1024, 256000, 1000000);
  }

  @Test
  public void testUnsafeByteOperations() throws IOException {
    // Arrange
    final ByteString bytes = ByteString.copyFrom(data);
    final OptimisticByteOutput byteOutput = new OptimisticByteOutput(bytes.size());

    // Act
    UnsafeByteOperations.unsafeWriteTo(bytes, byteOutput);

    // Assert
    assertNotSame(data, byteOutput.toByteArray());
    assertArrayEquals(data, byteOutput.toByteArray());
  }
}
