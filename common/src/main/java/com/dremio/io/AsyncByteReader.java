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
package com.dremio.io;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.dremio.io.file.Path;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * A simplified asynchronous data reading interface.
 */
public interface AsyncByteReader extends AutoCloseable {
  CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);

  /**
   * Read data into the provided dst buffer. Attempts to do so offheap.
   * @param offset The offset in the underlying data.
   * @param dst The ArrowBuf to read into
   * @param dstOffset The offset to read into.
   * @param len The amount of bytes to read.
   * @return A CompletableFuture that will be informed when the read is completed.
   */
  CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len);

  /**
   * Checks if the version of the file being read has changed. Note that
   * this should create a singleton instance of the Future to checkVersion. This API
   * is called repeatedly as part of {@link #versionedReadFully} and would cause a
   * performance degradation if the version check is done multiple times.
   *
   * @param version The expected version of the file
   *
   * @return A CompletableFuture that will be informed when the check is complete
   */
  default CompletableFuture<Void> checkVersion(String version) {
    return completedFuture;
  }

  /**
   * Verifies that the object's version has not changed and reads the object
   * @return
   */
  default CompletableFuture<Void> versionedReadFully(String version, long offset, ByteBuf dst, int dstOffset, int len) {
    return CompletableFuture
        .allOf(checkVersion(version), readFully(offset, dst, dstOffset, len))
        .whenComplete((v, e) -> {
          if (e != null) {
            Throwable cause = e.getCause();
            if (cause instanceof FileNotFoundException) {
              throw new CompletionException(cause);
            }
            throw new CompletionException(e);
          }
        });
  }

  /**
   * Read data and return as a byte array.
   * @param offset File offset to read from
   * @param len Number of bytes to read
   * @return A CompletableFuture that will be carry the byte[] result when the read is completed
   */
  default CompletableFuture<byte[]> readFully(long offset, int len) {
    final ByteBuf buf = Unpooled.directBuffer(len);
    CompletableFuture<Void> innerFuture = readFully(offset, buf, 0, len);
    return innerFuture.thenApply((v) -> {
      byte[] bytes = new byte[len];
      buf.getBytes(0, bytes, 0, len);
      return bytes;
    }).whenComplete((a,b) -> buf.release());
  }

  @Override
  default void close() throws Exception {
  }

  default List<ReaderStat> getStats() {
    return Collections.emptyList();
  }

  /**
   * Interface to fetch statistics for this async-reader, the actual statistics and values returned in the
   * list are implementation specific.
   */
  class ReaderStat {
    String name;
    double value;

    public ReaderStat(String name, double value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public double getValue() {
      return value;
    }

    public void setValue(double newValue) { value = newValue;}
  }

  /**
   * Interface to exchange all properties associated with the file or object for which the async reader
   * is required.  The full path, version, and file type should completely qualify this file or object
   * in the underlying layer that implements the async reader.
   */
  interface FileKey {
    /**
     * Enum list of file-types that support the async-reader interface.
     */
    enum FileType { PARQUET, ORC, OTHER }

    List<String> getDatasetKey();
    Path getPath();
    String getVersion();
    FileType getFileType();

    static FileKey of(Path path, String version, FileType fileType) {
      return of(path, version, fileType, null);
    }

    static FileKey of(Path path, String version, FileType fileType, List<String> dataset) {
      Objects.requireNonNull(path, "path is required");
      Objects.requireNonNull(version, "version is required");
      Objects.requireNonNull(fileType, "file type is required");

      return new FileKey() {
        private final List<String> datasetKey = dataset != null ? ImmutableList.copyOf(dataset) : null;

        @Override
        public Path getPath() {
          return path;
        }

        @Override
        public String getVersion() {
          return version;
        }

        @Override
        public FileType getFileType() {
          return fileType;
        }

        @Override
        public List<String> getDatasetKey() {
          return datasetKey;
        }
      };
    }
  }
}
