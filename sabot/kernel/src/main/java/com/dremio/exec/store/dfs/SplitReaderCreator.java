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
package com.dremio.exec.store.dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.arrow.util.Preconditions;

import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;

/**
 * Abstract class whose implementations are used in {@link PrefetchingIterator} to create a parquet split's reader
 */
public abstract class SplitReaderCreator implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplitReaderCreator.class);

  protected Path path;
  protected SplitReaderCreator next;
  protected List<List<String>> tablePath;
  protected InputStreamProvider inputStreamProvider;
  protected ParquetProtobuf.ParquetDatasetSplitScanXAttr splitXAttr;

  public SplitReaderCreator() {
  }

  public SplitReaderCreator(InputStreamProvider inputStreamProvider) {
    this.inputStreamProvider = inputStreamProvider;
  }

  /**
   * Creates reader to read current parquet split
   * @return
   */
  public abstract RecordReader createRecordReader(MutableParquetMetadata footer);

  /**
   * Initializes InputStreamProvider to be used by split reader
   * @param lastPath
   * @param lastInputStreamProvider
   */
  public abstract void createInputStreamProvider(InputStreamProvider lastInputStreamProvider, MutableParquetMetadata lastFooter);

  /**
   * Strictly abstract - all extending classes should close all closeables including inputStreamProvider
   *
   * @throws Exception
   */
  @Override
  public abstract void close() throws Exception;

  /**
   * Get current split's parquet footer
   * @return
   */
  public MutableParquetMetadata getFooter() {
    Preconditions.checkNotNull(inputStreamProvider);
    return handleEx(() -> inputStreamProvider.getFooter());
  }

  public InputStreamProvider getInputStreamProvider() {
    Preconditions.checkNotNull(inputStreamProvider);
    return inputStreamProvider;
  }

  /**
   * Get current split's parquet file location
   * @return
   */
  public Path getPath() {
    Preconditions.checkNotNull(path);
    return path;
  }

  /**
   * Adds the row groups that will be read by this split
   */
  public abstract void addRowGroupsToRead(Set<Integer> rowGroupList);

  /**
   * Iterates over the splits and identifies all row groups that are going
   * to be read from the given filePath
   */
  public void getRowGroupsFromSameFile(Path filePath, Set<Integer> rowGroupList) {
    if (!filePath.equals(path)) {
      return;
    }

    // this split shares the same file
    if (next != null) {
      next.getRowGroupsFromSameFile(filePath, rowGroupList);
    }

    addRowGroupsToRead(rowGroupList);
  }

  /**
   * Sets next SplitReaderCreator
   * @param next
   */
  public void setNext(SplitReaderCreator next) {
    this.next = next;
  }

  /**
   * Interface to allow a runnable that throws IOException.
   */
  public interface RunnableIO<T> {
    T run() throws IOException;
  }

  protected <T> T handleEx(RunnableIO<T> r) {
    Preconditions.checkNotNull(splitXAttr);
    Preconditions.checkNotNull(tablePath);
    try {
      return r.run();
    } catch (FileNotFoundException e) {
      throw UserException.invalidMetadataError(e)
          .addContext("Parquet file not found")
          .addContext("File", splitXAttr.getPath())
          .setAdditionalExceptionContext(new InvalidMetadataErrorContext(tablePath))
          .build(logger);
    } catch (IOException e) {
      throw UserException.dataReadError(e)
          .addContext("Failure opening parquet file")
          .addContext("File", splitXAttr.getPath())
          .build(logger);
    }
  }

}
