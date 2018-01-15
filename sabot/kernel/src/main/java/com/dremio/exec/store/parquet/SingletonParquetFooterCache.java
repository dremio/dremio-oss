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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.google.common.base.Preconditions;

/**
 * Single object cache that holds the parquet footer for last file.
 */
public class SingletonParquetFooterCache {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingletonParquetFooterCache.class);

  private static final int DEFAULT_READ_SIZE = 64*1024;
  private static final int FOOTER_LENGTH_SIZE = 4;
  private static final int FOOTER_METADATA_SIZE = FOOTER_LENGTH_SIZE + ParquetFileWriter.MAGIC.length;
  private static final int MAGIC_LENGTH = ParquetFileWriter.MAGIC.length;
  private static final int MIN_FILE_SIZE = ParquetFileWriter.MAGIC.length + FOOTER_METADATA_SIZE;

  private ParquetMetadata footer;
  private Path lastFile;

  public SingletonParquetFooterCache() {
    this.footer = null;
    this.lastFile = null;
  }

  public ParquetMetadata getFooter(FileSystemWrapper fs, Path file) {
    if (footer == null || !lastFile.equals(file)) {
      try {
        footer = readFooter(fs, file, ParquetMetadataConverter.NO_FILTER);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to read parquet footer for file " + file, ioe);
      }
      lastFile = file;
    }
    return footer;
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

  public static ParquetMetadata readFooter(final FileSystem fs, final Path file, ParquetMetadataConverter.MetadataFilter filter) throws IOException  {
    return readFooter(fs, fs.getFileStatus(file), filter);
  }

  /**
   * An updated footer reader that tries to read the entire footer without knowing the length.
   * This should reduce the amount of seek/read roundtrips in most workloads.
   * @param fs
   * @param status
   * @return
   * @throws IOException
   */
  public static ParquetMetadata readFooter(
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

      return ParquetFormatPlugin.parquetMetadataConverter.readParquetMetadata(new ByteArrayInputStream(footerBytes), filter);
    }
  }
}
