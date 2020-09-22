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
package com.dremio.exec.store;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import io.netty.util.internal.PlatformDependent;

/**
 * This class provides a Syncable local extension of the hadoop FileSystem
 */
public class LocalSyncableFileSystem extends FileSystem {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalSyncableFileSystem.class);

  @Override
  public URI getUri() {
    try {
      return new URI("dremio-local:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FSDataInputStream open(Path path, int i) throws IOException {
    return new FSDataInputStream(new LocalInputStream(localize(path)));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i2, long l, Progressable progressable) throws IOException {
    return new FSDataOutputStream(new LocalSyncableOutputStream(localize(path)), null);
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    throw new IOException("Append is not supported in LocalSyncableFilesystem");
  }

  @Override
  public boolean rename(Path path, Path path2) throws IOException {
    throw new IOException("Rename not supported");
  }

  @Override
  public boolean delete(Path path) throws IOException {
    File file = new File(localize(path).toString());
    return file.delete();
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    path = localize(path);
    File file = new File(path.toString());
    if (b) {
      if (file.isDirectory()) {
        FileUtils.deleteDirectory(file);
      } else {
        file.delete();
      }
    } else if (file.isDirectory()) {
      throw new IOException("Cannot delete directory");
    }
    file.delete();
    return true;
  }

  private static final FileStatus[] NO_FILE_STATUSES = {};
  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    String localizedPath = localize(path).toString();
    File file = new File(localizedPath);
    if (!file.exists() || !file.isDirectory()) {
      return NO_FILE_STATUSES;
    }
    String[] files = file.list();
    FileStatus[] statuses = new FileStatus[files.length];
    for (int i = 0; i < files.length; i++) {
      statuses[i] = getFileStatus(new Path(path, files[i]));
    }

    return statuses;
  }

  @Override
  public void setWorkingDirectory(Path path) {
  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    return new File(localize(path).toString()).mkdirs();
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    String localizedPath = localize(path).toString();
    File file = new File(localizedPath);
    if (file.exists()) {
      return new FileStatus(file.length(), file.isDirectory(), 1, 0, file.lastModified(), path);
    }

    throw new FileNotFoundException(localizedPath);
  }

  /**
   * Ability to write directly from arrow buf.
   */
  public static interface WritesArrowBuf {
    public int write(ArrowBuf buf) throws IOException;
  }

  /**
   * Ability to read directly into arrow buf.
   */
  public static interface ReadsArrowBuf {
    public void readFully(ArrowBuf buf, int length) throws IOException;
  }

  /**
   * Outputstream used by local filesystem.
   */
  public static final class LocalSyncableOutputStream extends OutputStream implements Syncable, WritesArrowBuf {
    private final FileOutputStream fos;
    private final BufferedOutputStream output;

    private LocalSyncableOutputStream(Path path) throws FileNotFoundException {
      File dir = new File(path.getParent().toString());
      if (!dir.exists()) {
        boolean success = dir.mkdirs();
        if (!success) {
          throw new FileNotFoundException("failed to create parent directory");
        }
      }
      fos = new FileOutputStream(path.toString());
      output = new BufferedOutputStream(fos, 64*1024);
    }

    @Override
    public int write(ArrowBuf buf) throws IOException {
      ByteBuffer nioBuffer = PlatformDependent.directBuffer(buf.memoryAddress(), (int) buf.readableBytes());
      return fos.getChannel().write(nioBuffer);
    }

    // Compatibility with Hadoop 2.x. Was removed with Hadoop 3.0
    public void sync() throws IOException {
      output.flush();
      fos.getFD().sync();
    }

    @Override
    public void hsync() throws IOException {
      output.flush();
      fos.getFD().sync();
    }

    @Override
    public void flush() throws IOException {
      output.flush();
    }

    @Override
    public void hflush() throws IOException {
      output.flush();
      fos.getFD().sync();
    }

    @Override
    public void write(int b) throws IOException {
      output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      output.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      try {
        output.close(); // closes "fos"
      } finally {
        super.close();
      }
    }
  }

  private static final class LocalInputStream extends InputStream implements Seekable, PositionedReadable, ByteBufferReadable, ReadsArrowBuf {

    private static final int BUFFER_SIZE = 64*1024;
    private final RandomAccessFile file;
    private final String path;
    private long position = 0;

    private BufferedInputStream input;

    public LocalInputStream(Path path)  throws IOException {
      this.path = path.toString();
      this.file = new RandomAccessFile(path.toString(), "r");
      input = new BufferedInputStream(new FileInputStream(file.getFD()), BUFFER_SIZE);
    }

    @Override
    public int read(long l, byte[] bytes, int i, int i2) throws IOException {
      throw new IOException("unsupported operation");
    }

    @Override
    public void readFully(long l, byte[] bytes, int i, int i2) throws IOException {
      throw new IOException("unsupported operation");
    }

    @Override
    public void readFully(long l, byte[] bytes) throws IOException {
      throw new IOException("unsupported operation");
    }

    @Override
    public void seek(long l) throws IOException {
      file.seek(l);
      input = new BufferedInputStream(new FileInputStream(file.getFD()), 1024*1024);
      position = l;
    }

    @Override
    public long getPos() throws IOException {
      return position;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
      throw new IOException("seekToNewSource not supported");
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      buf.reset();

      if(buf.hasArray()){
        int read = read(buf.array(), buf.arrayOffset(), buf.capacity());
        buf.limit(read);
        return read;
      }else{
        byte[] b = new byte[buf.capacity()];
        int read = read(b);
        buf.put(b);
        return read;
      }

    }

    // Compatibility with Hadoop 2.x. Was added in Hadoop 3.2
    public int read(long position, ByteBuffer dst) throws IOException {
      throw new IOException("positional read with ByteBuffer not supported");
    }

    @Override
    public int read(byte[] b) throws IOException {
      return input.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return input.read(b, off, len);
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      input.read(b);
      position++;
      return b[0] & 0xFF;
    }

    @Override
    public void close() throws IOException {
      try {
        input.close();
      } finally {
        super.close();
      }
    }

    @Override
    public void readFully(ArrowBuf buf, int length) throws IOException {
      ByteBuffer nioBuffer = PlatformDependent.directBuffer(buf.memoryAddress(), length);
      while(length > 0) {
        int read = file.getChannel().read(nioBuffer);
        if(read == -1) {
          throw new EOFException();
        }
        length -= read;
      }
      buf.writerIndex(length);
    }
  }

  private static Path localize(Path path) {
    return Path.getPathWithoutSchemeAndAuthority(path);
  }
}
