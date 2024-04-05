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
package com.dremio.exec.store.parquet;

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit test for the simple wrappers in the BulkInputStream */
public class TestBulkInputStream {
  private static final int TEST_DATA_SIZE = 15 * 1024;

  private static byte[] testData = new byte[TEST_DATA_SIZE];

  @BeforeClass
  public static void populateData() {
    for (int i = 0; i < TEST_DATA_SIZE; i++) {
      testData[i] = (byte) (i & 0x7f);
    }
  }

  /**
   * Compare testData[testDataOffset..testDataOffset+length) against
   * data[dataOffset..dataOffset+length)
   */
  private static void compareData(byte[] data, int dataOffset, int testDataOffset, int dataLength) {
    for (int i = 0; i < dataLength; i++) {
      assertEquals(testData[testDataOffset + i], data[dataOffset + i]);
    }
  }

  /**
   * Same as above, only the data being compared is in a ByteBuf with the readerIndex set
   * appropriately, and having at least 'dataLength' bytes
   */
  private static void compareData(ByteBuf data, int testDataOffset, int dataLength) {
    for (int i = 0; i < dataLength; i++) {
      assertEquals(testData[testDataOffset + i], data.readByte());
    }
  }

  private static class TestSeekableInputStream extends DelegatingSeekableInputStream {
    final ByteArrayInputStream inputStream;
    final long inputCount;

    TestSeekableInputStream(byte[] data) {
      this(data, new ByteArrayInputStream(data));
    }

    private TestSeekableInputStream(byte[] data, ByteArrayInputStream inputStream) {
      super(inputStream);
      this.inputStream = inputStream;
      this.inputCount = data.length;
    }

    @Override
    public long getPos() {
      return inputCount - inputStream.available();
    }

    @Override
    public void seek(long newPos) {
      inputStream.reset();
      inputStream.skip(newPos);
    }
  }

  @Test
  public void testWrap() throws Exception {
    BulkInputStream bis = BulkInputStream.wrap(new TestSeekableInputStream(testData));

    int streamPos = 0;
    assertEquals(streamPos, bis.getPos());

    // Read some bytes from the start
    final ByteBuf buf = Unpooled.directBuffer(1000);
    bis.readFully(buf, 88);
    compareData(buf, streamPos, 88);
    streamPos += 88;
    assertEquals(streamPos, bis.getPos());

    // Skip some, then read
    streamPos += 50; // skip 50 bytes
    bis.seek(streamPos);
    bis.readFully(buf, 37);
    compareData(buf, streamPos, 37);
    streamPos += 37;
    assertEquals(streamPos, bis.getPos());

    // skip to near the end, then read
    streamPos = TEST_DATA_SIZE - 100;
    bis.seek(streamPos);
    bis.readFully(buf, 100);
    compareData(buf, streamPos, 100);
    streamPos += 100;
    assertEquals(streamPos, bis.getPos());
  }

  private void testSeekableStream(SeekableInputStream inputStream) throws IOException {
    int streamPos = 0;
    assertEquals(streamPos, inputStream.getPos());

    // Read some bytes from the start
    final byte[] buf = new byte[1000];
    inputStream.readFully(buf, 0, 88);
    compareData(buf, 0, streamPos, 88);
    streamPos += 88;
    assertEquals(streamPos, inputStream.getPos());

    final byte[] shortBuf = new byte[17];
    inputStream.readFully(shortBuf);
    compareData(shortBuf, 0, streamPos, 17);
    streamPos += 17;
    assertEquals(streamPos, inputStream.getPos());

    // test ByteBuffer interfaces
    final ByteBuffer shortByteBuf = ByteBuffer.allocate(25);
    inputStream.read(shortByteBuf);
    compareData(shortByteBuf.array(), 0, streamPos, 25);
    streamPos += 25;
    assertEquals(streamPos, inputStream.getPos());

    final ByteBuffer shortByteBuf2 = ByteBuffer.allocateDirect(71);
    inputStream.read(shortByteBuf2);
    final ByteBuf compareBuf = Unpooled.directBuffer(100);
    shortByteBuf2.flip();
    compareBuf.writeBytes(shortByteBuf2);
    compareData(compareBuf, streamPos, 71);
    streamPos += 71;
    assertEquals(streamPos, inputStream.getPos());

    final ByteBuffer shortByteBuf3 = ByteBuffer.allocate(66);
    inputStream.readFully(shortByteBuf3);
    compareData(shortByteBuf3.array(), 0, streamPos, 66);
    streamPos += 66;
    assertEquals(streamPos, inputStream.getPos());

    // Test plain old read interface
    buf[0] = (byte) inputStream.read();
    buf[1] = (byte) inputStream.read();
    buf[2] = (byte) inputStream.read();
    compareData(buf, 0, streamPos, 3);
    streamPos += 3;
    assertEquals(streamPos, inputStream.getPos());

    // Skip some, then read
    streamPos += 50; // skip 50 bytes
    inputStream.seek(streamPos);
    inputStream.readFully(buf, 0, 37);
    compareData(buf, 0, streamPos, 37);
    streamPos += 37;
    assertEquals(streamPos, inputStream.getPos());

    // skip to near the end, then read
    streamPos = TEST_DATA_SIZE - 100;
    inputStream.seek(streamPos);
    inputStream.readFully(buf, 0, 100);
    compareData(buf, 0, streamPos, 100);
    streamPos += 100;
    assertEquals(streamPos, inputStream.getPos());
  }

  @Test
  public void testAsSeekableIdentity() throws Exception {
    testSeekableStream(
        BulkInputStream.wrap(new TestSeekableInputStream(testData)).asSeekableInputStream());
  }

  @Test
  public void testAsSeekable() throws Exception {
    testSeekableStream(
        new BulkInputStream() {
          private long pos = 0;
          private byte[] data = testData;

          @Override
          public void seek(long offset) {
            pos = offset;
          }

          @Override
          public long getPos() {
            return pos;
          }

          @Override
          public void readFully(ByteBuf buf, int length) {
            buf.writeBytes(data, (int) pos, length);
            pos += length;
          }

          @Override
          public void close() {}
        }.asSeekableInputStream());
  }
}
