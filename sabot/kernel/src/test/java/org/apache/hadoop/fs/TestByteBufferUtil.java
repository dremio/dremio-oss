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
package org.apache.hadoop.fs;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.ByteBufferPool;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for the locally fixed ByteBufferUtil
 */
public class TestByteBufferUtil {
  private static final int TEST_DATA_SIZE = ByteBufferUtil.MAX_BUFFERED_READ * 7 / 3;

  private static byte[] testData = new byte[TEST_DATA_SIZE];

  @BeforeClass
  public static void populateData() {
    for (int i = 0; i < TEST_DATA_SIZE; i++) {
      testData[i] = (byte)(i & 0x7f);
    }
  }

  public class LimitedByteArrayInputStream extends ByteArrayInputStream {
    private final int limit;

    public LimitedByteArrayInputStream(byte[] buf, int limit) {
      super(buf);
      this.limit = limit;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      return super.read(b, off, Math.min(len, limit));
    }
  }

  private void testReadHelper(final int readSize, final int readLimit) throws Exception {
    ByteArrayInputStream inputStream = new LimitedByteArrayInputStream(testData, readLimit);
    ByteBufferPool adapterPool = new TestByteBufferPool();
    int currOffset = 0;
    while (currOffset < TEST_DATA_SIZE) {
      ByteBuffer byteBuffer = ByteBufferUtil.fallbackRead(inputStream, adapterPool, readSize);
      final int length = byteBuffer.remaining();
      for (int i = 0; i < length; i++) {
        assertEquals(testData[currOffset + i], byteBuffer.get());
      }
      adapterPool.putBuffer(byteBuffer);
      currOffset += length;
    }
  }

  @Test
  public void testReadManySmall() throws Exception {
    final int smallReadSize = 17;
    testReadHelper(smallReadSize, smallReadSize);
  }

  @Test
  public void testReadOneLarge() throws Exception {
    testReadHelper(TEST_DATA_SIZE, TEST_DATA_SIZE);
  }

  @Test
  public void testReadChunkedLarge() throws Exception {
    // Single read that will internally be served as several small reads
    testReadHelper(TEST_DATA_SIZE, TEST_DATA_SIZE * 7 / 117);
  }

  private static class TestByteBufferPool implements ByteBufferPool {

    @Override
    public ByteBuffer getBuffer(boolean direct, int length) {
      return direct ? ByteBuffer.allocateDirect(length) : ByteBuffer.allocate(length);
    }

    @Override
    public void putBuffer(ByteBuffer buffer) {
      // NB: Garbage collection frees the underlying buffer memory
    }
  }
}
