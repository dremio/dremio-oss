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
package com.dremio.exec.store.hive.exec.apache;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ByteBufferPool;


/**
 * Wrapper around FSDataInputStream to capture {@code FSError}.
 */
class FSDataInputStreamWrapper extends FSDataInputStream {
  private FSDataInputStream underlyingIs;

  public FSDataInputStreamWrapper(FSDataInputStream in) throws IOException {
    super(new WrappedInputStream(in));
    underlyingIs = in;
  }

  protected FSDataInputStreamWrapper(FSDataInputStream in, WrappedInputStream wrapped) throws IOException {
    super(wrapped);
    underlyingIs = in;
  }

  protected FSDataInputStream getUnderlyingStream() {
    return underlyingIs;
  }

  @Override
  public synchronized void seek(long desired) throws IOException {
    try {
      underlyingIs.seek(desired);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public long getPos() throws IOException {
    try {
      return underlyingIs.getPos();
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    try {
      return underlyingIs.read(position, buffer, offset, length);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    try {
      underlyingIs.readFully(position, buffer, offset, length);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    try {
      underlyingIs.readFully(position, buffer);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    try {
      return underlyingIs.seekToNewSource(targetPos);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  @LimitedPrivate({"HDFS"})
  public InputStream getWrappedStream() {
    return underlyingIs.getWrappedStream();
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    try {
      return underlyingIs.read(buf);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public FileDescriptor getFileDescriptor() throws IOException {
    try {
      return underlyingIs.getFileDescriptor();
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
    try {
      underlyingIs.setReadahead(readahead);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public void setDropBehind(Boolean dropBehind) throws IOException, UnsupportedOperationException {
    try {
      underlyingIs.setDropBehind(dropBehind);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts) throws IOException, UnsupportedOperationException {
    try {
      return underlyingIs.read(bufferPool, maxLength, opts);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public void releaseBuffer(ByteBuffer buffer) {
    underlyingIs.releaseBuffer(buffer);
  }

  @Override
  public int read() throws IOException {
    try {
      return underlyingIs.read();
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    try {
      return underlyingIs.skip(n);
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public int available() throws IOException {
    try {
      return underlyingIs.available();
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (underlyingIs != null) {
      try {
        underlyingIs.close();
        underlyingIs = null;
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }
  }

  @Override
  public void mark(int readlimit) {
    underlyingIs.mark(readlimit);
  }

  @Override
  public void reset() throws IOException {
    try {
      underlyingIs.reset();
    } catch(FSError e) {
      throw HadoopFileSystemWrapper.propagateFSError(e);
    }
  }

  @Override
  public boolean markSupported() {
    return underlyingIs.markSupported();
  }

  @Override
  public void unbuffer() {
    underlyingIs.unbuffer();
  }

  /**
   * We need to wrap the FSDataInputStream inside a InputStream, because read() method in InputStream is
   * overridden in FilterInputStream (super class of FSDataInputStream) as final, so we can not override in
   * FSDataInputStreamWrapper.
   */
  static class WrappedInputStream extends InputStream implements Seekable, PositionedReadable {
    private FSDataInputStream is;

    protected WrappedInputStream(FSDataInputStream is) {
      this.is = is;
    }

    /**
     * Most of the read are going to be block reads which use {@link #read(byte[], int,
     * int)}. So not adding stats for single byte reads.
     */
    @Override
    public int read() throws IOException {
      try {
        return is.read();
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      try {
        return is.read(b, off, len);
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public int read(byte[] b) throws IOException {
      try {
        return is.read(b);
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public long skip(long n) throws IOException {
      try {
        return is.skip(n);
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public int available() throws IOException {
      try{
        return is.available();
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public void close() throws IOException {
      if (is != null) {
        try{
          is.close();
          is = null;
        } catch(FSError e) {
          throw HadoopFileSystemWrapper.propagateFSError(e);
        }
      }
    }

    @Override
    public synchronized void mark(int readlimit) {
      is.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
      try {
        is.reset();
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public boolean markSupported() {
      return is.markSupported();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      try {
        return is.read(position, buffer, offset, length);
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      try {
        is.readFully(position, buffer, offset, length);
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      try {
        is.readFully(position, buffer);
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public void seek(long pos) throws IOException {
      try {
        is.seek(pos);
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public long getPos() throws IOException {
      try {
        return is.getPos();
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      try {
        return is.seekToNewSource(targetPos);
      } catch(FSError e) {
        throw HadoopFileSystemWrapper.propagateFSError(e);
      }
    }
  }
}
