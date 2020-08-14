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
package com.dremio.sabot.op.sort.external;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.cache.VectorAccessibleFlatBufSerializable;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.InputStreamWithStats;
import com.dremio.exec.store.LocalSyncableFileSystem.LocalSyncableOutputStream;
import com.dremio.exec.store.LocalSyncableFileSystem.WritesArrowBuf;
import com.dremio.exec.store.OutputStreamWithStats;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.spill.SpillDirectory;
import com.dremio.service.spill.SpillService;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

/**
 * Distribute spills across given list of directories.
 * Monitor disk space left and stop using disks which are running low on free space.
 * Monitoring is disabled for spill directories on non local filesystems.
 */
public class SpillManager implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpillManager.class);

  static final String DREMIO_LOCAL_IMPL_STRING = "fs.dremio-local.impl";

  private final String id;
  private final SpillService spillService;
  private final String caller;
  private final OperatorStats stats;
  private final boolean useDirectWritePathIfPossible;
  private long appWriteBytes;
  private long appReadBytes;
  private long ioReadBytes;
  private long ioWriteBytes;
  private long compressionNanos;
  private long decompressionNanos;
  private long ioReadWaitNanos;
  private long ioWriteWaitNanos;

  public SpillManager(SabotConfig sabotConfig, OptionManager optionManager, String id, Configuration hadoopConf,
      SpillService spillService, String caller, OperatorStats stats)  {
    final List<String> directories = new ArrayList<>(sabotConfig.getStringList(ExecConstants.SPILL_DIRS));
    if (directories.isEmpty()) {
      throw UserException.dataWriteError().message("No spill locations specified.").build(logger);
    }

    this.stats = stats;
    this.id  = id;
    this.caller = caller;
    this.spillService = spillService;
    // load options
    if (optionManager != null) {
      this.useDirectWritePathIfPossible = optionManager.getOption(ExecConstants.EXTERNAL_SORT_DIRECT_WRITE);
    } else {
      this.useDirectWritePathIfPossible = ExecConstants.EXTERNAL_SORT_DIRECT_WRITE.getDefault().getBoolVal();
    }

    try {
      spillService.makeSpillSubdirs(id);
    } catch (UserException e) {
      throw UserException.dataWriteError(e)
        .addContext("Caller", caller)
        .build(logger);
    }
  }

  public SpillFile getSpillFile(String fileName) throws RuntimeException {
    try {
      final SpillDirectory spillDirectory = spillService.getSpillSubdir(id);
      return new SpillFile(spillDirectory.getFileSystem(), new Path(spillDirectory.getSpillDirPath(), fileName));
    } catch (UserException e) {
      throw UserException.dataWriteError(e)
        .addContext("for %s spill id %s", caller, id)
        .addContext("Caller", caller)
        .build(logger);
    }
  }

  @Override
  public void close() throws Exception {
    spillService.deleteSpillSubdirs(id);
  }

  final public class SpillFile implements AutoCloseable {
    private final FileSystem fs;
    private final Path path;

    SpillFile(FileSystem fs, Path path) {
      this.fs = fs;
      this.path = path;
    }

    public SpillOutputStream create(boolean compressed) throws IOException {
      return createSpillOutputStream(this, compressed);
    }

    public SpillInputStream open(boolean compressed) throws IOException {
      return createSpillInputStream(this, compressed);
    }

    public FSDataOutputStream create() throws IOException {
      return fs.create(path);
    }

    public FSDataOutputStream append() throws IOException {
      return fs.append(path);
    }

    public FSDataInputStream open() throws IOException {
      return fs.open(path);
    }

    private void delete() throws IOException {
      fs.delete(path, true);
    }

    @Override
    public void close() throws Exception {
      delete();
    }

    public FileStatus getFileStatus() throws IOException {
      return fs.getFileStatus(path);
    }

    public Path getPath() {
      return path;
    }
  }

  private static class ABOutputStreamWithStats extends OutputStreamWithStats implements WritesArrowBuf {

    public ABOutputStreamWithStats(OutputStream out) {
      super(out);
    }

    public int write(ArrowBuf buf) throws IOException {
      write.start();
      try {
        return ((WritesArrowBuf) out).write(buf);
      } finally {
        write.stop();
      }

    }

  }

  private SpillOutputStream createSpillOutputStream(SpillFile file, boolean compressed) throws IOException {
    FSDataOutputStream output = file.fs.create(file.path);
    OutputStream actualOutput = output;
    try {
      OutputStream inner = output.getWrappedStream();
      if(inner instanceof LocalSyncableOutputStream) {
        actualOutput = inner;
      }
    } catch (Exception ex) {
      logger.debug("Failed to get inner wrapped stream, using fallback.", ex);
    }
    ABOutputStreamWithStats base = new ABOutputStreamWithStats(actualOutput);
    ABOutputStreamWithStats top = compressed ? new ABOutputStreamWithStats(new LZ4BlockOutputStream(base)) : base;
    boolean useDirectWrite = useDirectWritePathIfPossible && !compressed && actualOutput instanceof WritesArrowBuf;
    return new SpillOutputStream(top, base, file, compressed, useDirectWrite);
  }

  public class SpillOutputStream extends FilterOutputStream {

    private final byte[] heapMoveBuffer = new byte[64*1024];
    private final ABOutputStreamWithStats top;
    private final ABOutputStreamWithStats base;
    private final SpillFile file;
    private boolean compressed;
    private boolean writeDirect;

    private SpillOutputStream(
        ABOutputStreamWithStats top,
        ABOutputStreamWithStats base,
        SpillFile file,
        boolean compressed,
        boolean writeDirect) {
      super(top);
      this.top = top;
      this.base = base;
      this.file = file;
      this.compressed = compressed;
      this.writeDirect = writeDirect;
    }

    public boolean isCompressed() {
      return compressed;
    }

    public Path getPath() {
      return file.getPath();
    }

    public long getWriteBytes() {
      return top.getWriteBytes();
    }

    public long getIOBytes() {
      return base.getWriteBytes();
    }

    public long getIOTime() {
      return base.getWriteNanos() + base.getCloseNanos();
    }

    public long getCompressionTime() {
      if(!compressed) {
        return 0;
      }

      return top.getWriteNanos() + top.getCloseNanos() - getIOTime();
    }

    public long writeBatch(VectorContainer outgoing) throws IOException {
      VectorAccessibleFlatBufSerializable serializable = new VectorAccessibleFlatBufSerializable(outgoing, null);
      serializable.setWriteDirect(writeDirect);
      serializable.writeToStream(top);
      return serializable.getBytesWritten();
    }

    @Override
    public void close() throws IOException {
      super.close();
      if(stats != null) {
        stats.moveProcessingToWait(getIOTime());
      }
      ioWriteWaitNanos += getIOTime();
      appWriteBytes += getWriteBytes();
      ioWriteBytes += getIOBytes();
      compressionNanos += getCompressionTime();
    }

  }

  private SpillInputStream createSpillInputStream(SpillFile file, boolean compressed) throws IOException {
    InputStream output = file.fs.open(file.path);
    InputStreamWithStats base = new InputStreamWithStats(output);
    InputStreamWithStats top = compressed ? new InputStreamWithStats(new LZ4BlockInputStream(base)) : base;
    return new SpillInputStream(top, base, file, compressed);
  }

  public class SpillInputStream extends FilterInputStream {

    private final InputStreamWithStats top;
    private final InputStreamWithStats base;
    private final SpillFile file;
    private boolean compressed;

    private SpillInputStream(
        InputStreamWithStats top,
        InputStreamWithStats base,
        SpillFile file,
        boolean compressed) {
      super(top);
      this.top = top;
      this.base = base;
      this.file = file;
      this.compressed = compressed;
    }

    public boolean isCompressed() {
      return compressed;
    }

    public Path getPath() {
      return file.getPath();
    }

    public long getDecompressedBytes() {
      return top.getReadBytes();
    }

    public long getIOBytes() {
      return base.getReadBytes();
    }

    public long getIOTime() {
      return base.getReadNanos();
    }

    public long getDeompressionTime() {
      if(!compressed) {
        return 0;
      }

      return top.getReadNanos() - getIOTime();
    }

    public void load(VectorContainer container, BufferAllocator allocator) throws IOException {
      VectorAccessibleFlatBufSerializable serializable = new VectorAccessibleFlatBufSerializable(container, allocator);
      serializable.readFromStream(top);
    }

    @Override
    public void close() throws IOException {
      super.close();
      if(stats != null) {
        stats.moveProcessingToWait(getIOTime());
      }
      ioReadWaitNanos += getIOTime();
      appReadBytes += getDecompressedBytes();
      ioReadBytes += getIOBytes();
      decompressionNanos += getDeompressionTime();
    }
  }

  public long getAppWriteBytes() {
    return appWriteBytes;
  }

  public long getAppReadBytes() {
    return appReadBytes;
  }

  public long getIOReadBytes() {
    return ioReadBytes;
  }

  public long getIOWriteBytes() {
    return ioWriteBytes;
  }

  public long getCompressionNanos() {
    return compressionNanos;
  }

  public long getDecompressionNanos() {
    return decompressionNanos;
  }

  public long getIOReadWait() {
    return ioReadWaitNanos;
  }

  public long getIOWriteWait() {
    return ioWriteWaitNanos;
  }
}
