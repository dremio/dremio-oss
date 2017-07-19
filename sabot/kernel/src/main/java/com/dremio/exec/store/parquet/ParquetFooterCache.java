/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class ParquetFooterCache {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetFooterCache.class);

  private static final int DEFAULT_READ_SIZE = 64*1024;
  private static final int FOOTER_LENGTH_SIZE = 4;
  private static final int FOOTER_METADATA_SIZE = FOOTER_LENGTH_SIZE + ParquetFileWriter.MAGIC.length;
  private static final int MAGIC_LENGTH = ParquetFileWriter.MAGIC.length;
  private static final int MIN_FILE_SIZE = ParquetFileWriter.MAGIC.length + FOOTER_METADATA_SIZE;

  private final LoadingCache<FileStatus, ParquetMetadata> footers;
  private final LoadingCache<BlockLocationKey, BlockLocation[]> blocks;
  private final FileSystem fs;

  public ParquetFooterCache(final FileSystem fs, final int maxSize, boolean useNoSeekReader){
    this.fs = fs;
    footers = CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .build(useNoSeekReader ? new NonSeekingLoader() : new SeekingLoader());

    blocks = CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .refreshAfterWrite(1, TimeUnit.DAYS)
        .build(new CacheLoader<BlockLocationKey, BlockLocation[]>() {
          @Override
          public BlockLocation[] load(BlockLocationKey key) throws Exception {
            return fs.getFileBlockLocations(key.status, key.start, key.end);
          }
        });
  }

  public ParquetMetadata getFooter(FileStatus status) {
    try{
      return footers.get(status);
    } catch(ExecutionException ex){
      throw UserException.dataReadError(ex.getCause())
        .message("Unable to read Parquet footer for file %s.", status.getPath())
        .build(logger);
    }
  }

  public BlockLocation[] getBlockLocations(FileStatus status, long start, long end) throws IOException {
    try{
      return blocks.get(new BlockLocationKey(status, start, end));
    }catch(ExecutionException ex){
      Throwable e = ex.getCause();
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  public ParquetMetadata getFooter(String path) {
    return getFooter(new Path(path));
  }

  public ParquetMetadata getFooter(Path path) {
    try{
      final FileStatus status = fs.getFileStatus(path);
      return getFooter(status);
    } catch(IOException ex){
      throw UserException.dataReadError(ex)
      .message("Unable to read Parquet footer for file %s.", path)
      .build(logger);
    }
  }


  private class SeekingLoader extends CacheLoader<FileStatus, ParquetMetadata> {

    @Override
    public ParquetMetadata load(FileStatus key) throws Exception {

      return readFooterWithSeek(fs, key, ParquetMetadataConverter.NO_FILTER);
    }

  }

  private class NonSeekingLoader extends CacheLoader<FileStatus, ParquetMetadata> {

    @Override
    public ParquetMetadata load(FileStatus key) throws Exception {
      return readFooterNoSeek(fs, key, ParquetMetadataConverter.NO_FILTER).getParquetMetadata();
    }

  }

  /**
   * Code taken from Parquet to allow injection of FileSystem. Read last eight
   * bytes of file, then seek backwards and read data.
   *
   * @param fileSystem
   * @param file
   * @param filter
   * @return
   * @throws IOException
   */
  private static final ParquetMetadata readFooterWithSeek(
      final FileSystem fileSystem,
      FileStatus file,
      ParquetMetadataConverter.MetadataFilter filter) throws IOException {

    FSDataInputStream f = fileSystem.open(file.getPath());

    try {
      long l = file.getLen();
      if (logger.isDebugEnabled()) {
        logger.debug("File length " + l);
      }
      int FOOTER_LENGTH_SIZE = 4;
      if (l < ParquetFileWriter.MAGIC.length + FOOTER_LENGTH_SIZE + ParquetFileWriter.MAGIC.length) {
        throw new RuntimeException(file.getPath() + " is not a Parquet file (too small)");
      }
      long footerLengthIndex = l - FOOTER_LENGTH_SIZE - ParquetFileWriter.MAGIC.length;
      if (logger.isDebugEnabled()) {
        logger.debug("reading footer index at " + footerLengthIndex);
      }

      f.seek(footerLengthIndex);
      int footerLength = BytesUtils.readIntLittleEndian(f);
      byte[] magic = new byte[ParquetFileWriter.MAGIC.length];
      f.readFully(magic);
      if (!(Arrays.equals(ParquetFileWriter.MAGIC, magic))) {
        throw new RuntimeException(file.getPath() + " is not a Parquet file. expected magic number at tail "
            + Arrays.toString(ParquetFileWriter.MAGIC) + " but found " + Arrays.toString(magic));
      }
      long footerIndex = footerLengthIndex - footerLength;
      if (logger.isDebugEnabled()) {
        logger.debug("read footer length: " + footerLength + ", footer index: " + footerIndex);
      }
      if ((footerIndex < ParquetFileWriter.MAGIC.length) || (footerIndex >= footerLengthIndex)) {
        throw new RuntimeException("corrupted file: the footer index is not within the file");
      }
      f.seek(footerIndex);
      ParquetMetadata localParquetMetadata = ParquetFormatPlugin.parquetMetadataConverter.readParquetMetadata(f, filter);

      return localParquetMetadata;
    } finally {
      f.close();
    }
  }

  /**
   * An updated footer reader that tries to read the entire footer without knowing the length.
   * This should reduce the amount of seek/read roundtrips in most workloads.
   * @param fs
   * @param status
   * @return
   * @throws IOException
   */
  private static Footer readFooterNoSeek(
      final FileSystem fs,
      final FileStatus status,
      ParquetMetadataConverter.MetadataFilter filter) throws IOException {
    try(FSDataInputStream file = fs.open(status.getPath())) {

      final long fileLength = status.getLen();
      Preconditions.checkArgument(fileLength >= MIN_FILE_SIZE, "%s is not a Parquet file (too small)", status.getPath());

      int len = (int) Math.min( fileLength, (long) DEFAULT_READ_SIZE);
      byte[] footerBytes = new byte[len];
      readFully(file, fileLength - len, footerBytes, 0, len);

      checkMagicBytes(status, footerBytes, footerBytes.length - ParquetFileWriter.MAGIC.length);
      final int size = BytesUtils.readIntLittleEndian(footerBytes, footerBytes.length - FOOTER_METADATA_SIZE);

      if(size > footerBytes.length - FOOTER_METADATA_SIZE){
        // if the footer is larger than our initial read, we need to read the rest.
        byte[] origFooterBytes = footerBytes;
        int origFooterRead = origFooterBytes.length - FOOTER_METADATA_SIZE;

        footerBytes = new byte[size];

        readFully(file, fileLength - size - FOOTER_METADATA_SIZE, footerBytes, 0, size - origFooterRead);
        System.arraycopy(origFooterBytes, 0, footerBytes, size - origFooterRead, origFooterRead);
      }else{
        int start = footerBytes.length - (size + FOOTER_METADATA_SIZE);
        footerBytes = ArrayUtils.subarray(footerBytes, start, start + size);
      }

      ParquetMetadata metadata = ParquetFormatPlugin.parquetMetadataConverter.readParquetMetadata(new ByteArrayInputStream(footerBytes), filter);
      Footer footer = new Footer(status.getPath(), metadata);
      return footer;
    }
  }

  private static final void readFully(FSDataInputStream stream, long start, byte[] output, int offset, int len) throws IOException{
    int bytesRead = 0;
    while(bytesRead > -1 && bytesRead < len){
      bytesRead += stream.read(start+bytesRead, output, offset + bytesRead, len-bytesRead);
    }
  }

  private static void checkMagicBytes(FileStatus status, byte[] data, int offset) throws IOException {
    for(int i =0, v = offset; i < MAGIC_LENGTH; i++, v++){
      if(ParquetFileWriter.MAGIC[i] != data[v]){
        byte[] magic = ArrayUtils.subarray(data, offset, offset + MAGIC_LENGTH);
        throw new IOException(status.getPath() + " is not a Parquet file. expected magic number at tail " + Arrays.toString(ParquetFileWriter.MAGIC) + " but found " + Arrays.toString(magic));
      }
    }
  }

  private static class BlockLocationKey {
    //BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, start, length);
    private final FileStatus status;
    private final long start;
    private final long end;

    public BlockLocationKey(FileStatus status, long start, long end) {
      super();
      this.status = status;
      this.start = start;
      this.end = end;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (end ^ (end >>> 32));
      result = prime * result + (int) (start ^ (start >>> 32));
      result = prime * result + ((status == null) ? 0 : status.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      BlockLocationKey other = (BlockLocationKey) obj;
      if (end != other.end) {
        return false;
      }
      if (start != other.start) {
        return false;
      }
      if (status == null) {
        if (other.status != null) {
          return false;
        }
      } else if (!status.equals(other.status)) {
        return false;
      }
      return true;
    }


  }

}
