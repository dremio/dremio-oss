/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.io.SeekableInputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * An custom input stream with minimal methods and designed for bulk reads.
 */
public interface BulkInputStream extends Closeable {

  void seek(long offset)  throws IOException;
  void readFully(ByteBuf buf, int length) throws IOException;
  long getPos() throws IOException;

  default void readFully(byte[] dst, int dstOffset, int dstLen)  throws IOException {
    final ByteBuf buf1 = Unpooled.buffer(dstLen);
    try {
      readFully(buf1, dstLen);
      buf1.getBytes(0, dst, dstOffset, dstLen);
    } finally {
      buf1.release();
    }
  }

  default long skip(long len) throws IOException {
    seek(getPos() + len);
    return len;
  }

  public static BulkInputStream wrap(SeekableInputStream is) {
    return new SeekableBulkInputStream(is);
  }

  static class SeekableBulkInputStream extends SeekableInputStream implements BulkInputStream {

    private final SeekableInputStream is;

    public SeekableBulkInputStream(SeekableInputStream is) {
      super();
      this.is = is;
    }

    @Override
    public void close() throws IOException {
      is.close();
    }

    @Override
    public void seek(long offset) throws IOException {
      is.seek(offset);
    }

    @Override
    public void readFully(ByteBuf buffer, int length) throws IOException {
      final int initialWriterIndex = buffer.writerIndex();
      ByteBuffer bb = buffer.nioBuffer(buffer.writerIndex(), length);
      is.readFully(bb);
      buffer.writerIndex(initialWriterIndex + length);
    }

    @Override
    public long getPos() throws IOException {
      return is.getPos();
    }

    @Override
    public int read() throws IOException {
      return is.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
      return is.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return is.read(b, off, len);
    }

    // Overridden SeekableInputStream methods
    @Override
    public void readFully(byte[] b) throws IOException {
      is.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int start, int len) throws IOException {
      is.readFully(b, start, len);
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      return is.read(buf);
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      is.readFully(buf);
    }
  }

  default SeekableInputStream asSeekableInputStream() {
    if(this instanceof SeekableBulkInputStream) {
      return (SeekableInputStream) this;
    }
    return new SeekableInputStream() {
      @Override
      public long getPos() throws IOException {
        return BulkInputStream.this.getPos();
      }

      @Override
      public void seek(long newPos) throws IOException {
        BulkInputStream.this.seek(newPos);
      }

      @Override
      public void readFully(byte[] bytes) throws IOException {
        BulkInputStream.this.readFully(bytes, 0, bytes.length);
      }

      @Override
      public void readFully(byte[] bytes, int start, int len) throws IOException {
        BulkInputStream.this.readFully(bytes, start, len);
      }

      @Override
      public int read(ByteBuffer buf) throws IOException {
        final int remainingBytes = buf.remaining();
        final ByteBuf buf1 = Unpooled.buffer(remainingBytes);
        BulkInputStream.this.readFully(buf1, remainingBytes);
        buf.put(buf1.nioBuffer());
        // TODO: Question: how do we indicate end of stream?
        buf1.release();
        return remainingBytes - buf.remaining();
      }

      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        int remainingBytes = buf.remaining();
        int bytesRead;
        do {
          bytesRead = read(buf);
          if (bytesRead > 0) {
            remainingBytes -= bytesRead;
          }
        } while (remainingBytes > 0 && bytesRead >= 0);
      }

      private final ByteBuf singleByte = Unpooled.buffer(1);
      @Override
      public int read() throws IOException {
        BulkInputStream.this.readFully(singleByte, 1);
        int value = singleByte.getByte(0);
        singleByte.clear();
        return (value & 0xff);
      }

      @Override
      public void close() {
        singleByte.release();
      }
    };
  }
}
