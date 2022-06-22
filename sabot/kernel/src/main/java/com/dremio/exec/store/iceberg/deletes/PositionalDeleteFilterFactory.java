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
package com.dremio.exec.store.iceberg.deletes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

/**
 * Factory for creating PositionalDeleteFilter instances.  This factory will try to optimize the use of delete file
 * reader instances across a ScanTableFunction batch.
 *
 * For each data file, a PositionalDeleteFilter instance is cached.  This instance will be reused for subsequent
 * row groups scanned for that data file in the batch.  These row groups may either be from separate block splits,
 * or multiple row groups expanded from a single block split.
 *
 * Each PositionalDeleteFilter associated with a data file may read from multiple delete files.  These delete file
 * instances are reused across multiple data files in the batch, such that there is only one reader instantiated per
 * delete file per batch.  For this to work, data files within the batch must be processed in sorted order.
 * ParquetSplitReaderCreatorIterator guarantees this for Parquet-based Iceberg table scans.
 *
 * Both PositionalDeleteFilter and PositionalDeleteFileReader instances are reference counted so they can be closed
 * immediately when they are no longer needed.
 *  - PositionalDeleteFilter's reference count is the count of row groups to be scanned for the data file the filter
 *    is associated with.  The PositionalDeleteFilter is closed once the last row group is scanned.
 *  - PositionalDeleteFileReader's reference count is the number of data files that need to use that delete file as
 *    part of their filter.  The PositionalDeleteFileReader is closed once the last PositionalDeleteFilter that
 *    depends on it is closed.
 *
 * The call sequence to PositionalDeleteFilterFactory is as follows:
 *
 *  - setDataFileInfoForBatch() is called at the start of each batch with the following information for each data file
 *    in the batch:
 *     - The list of applicable delete file paths
 *     - The number of splits in the batch for the data file - assuming 1 row group per split
 *  - As the ParquetScanTableFunction/ParquetSplitReaderCreatorIterator handles each split, if the split expands into
 *    more than one rowgroup, it must call adjustRowGroupCount() for that data file.  This is necessary so the
 *    refcount on the associated PositionalDeleteFilter for that file can be increased.
 *  - If ParquetSplitReaderCreatorIterator filters out an entire split for an input data file, it will call
 *    adjustRowGroupCount with a negative delta to decrement reference counts on the associated filter and delete files.
 *  - For each data file rowgroup to be scanned, create() is called to get the PositionalDeleteFilter for that data
 *    file.  For rowgroups after the 1st, this just returns the cached filter.
 */
public class PositionalDeleteFilterFactory implements AutoCloseable {

  private final OperatorContext context;
  private final PositionalDeleteFileReaderFactory readerFactory;
  private final Map<String, List<String>> dataFilesByDeleteFile = new HashMap<>();
  private final Map<String, PositionalDeleteFileReader> deleteFileReaders = new HashMap<>();
  private final Map<String, PositionalDeleteFilter> positionalDeleteFilters = new HashMap<>();

  private Map<String, DataFileInfo> dataFileInfo;

  public PositionalDeleteFilterFactory(OperatorContext context, PositionalDeleteFileReaderFactory readerFactory) {
    this.context = Preconditions.checkNotNull(context);
    this.readerFactory = Preconditions.checkNotNull(readerFactory);
  }

  public PositionalDeleteFilter create(String dataFilePath) {
    PositionalDeleteFilter positionalDeleteFilter = null;
    if (dataFileInfo != null && dataFileInfo.containsKey(dataFilePath)) {
      List<String> deleteFiles = dataFileInfo.get(dataFilePath).getDeleteFiles();
      if (deleteFiles != null && !deleteFiles.isEmpty()) {
        positionalDeleteFilter = getOrCreateFilter(dataFilePath, deleteFiles);
      }
    }

    return positionalDeleteFilter;
  }

  public void adjustRowGroupCount(String dataFilePath, int delta) {
    if (dataFileInfo != null && dataFileInfo.containsKey(dataFilePath)) {
      DataFileInfo info = dataFileInfo.get(dataFilePath);
      info.addRowGroups(delta);

      // adjust the refcount on the PositionalDeleteFilter for this data file, if we have one, so that the
      // lifetime is updated to reflect the adjusted rowgroup count
      PositionalDeleteFilter filter = positionalDeleteFilters.get(dataFilePath);
      if (filter != null) {
        if (delta > 0) {
          filter.retain(delta);
        } else if (delta < 0) {
          filter.release(-delta);
        }
      }

      // if the rowgroup count adjustment results in the data file being skipped completely, decrement
      // refcounts on associated delete files, and then remove the data file from all maps
      if (info.getNumRowGroups() == 0) {
        for (String deleteFile : info.getDeleteFiles()) {
          if (deleteFileReaders.containsKey(deleteFile)) {
            PositionalDeleteFileReader reader = deleteFileReaders.get(deleteFile);
            reader.release();
            if (reader.refCount() == 0) {
              deleteFileReaders.remove(deleteFile);
            }
          }
        }

        dataFileInfo.remove(dataFilePath);
        positionalDeleteFilters.remove(dataFilePath);
      }
    }
  }

  public void setDataFileInfoForBatch(Map<String, DataFileInfo> dataFileInfo) {
    this.dataFileInfo = dataFileInfo;

    // We can't share filters/open delete file readers across batches, so clear the respective maps for each.
    // The objects themselves will get closed when dependent data file readers are complete - on start of a new
    // batch the last reader from the previous batch still has not executed, so we can't arbitrarily close filters or
    // delete file readers here.
    deleteFileReaders.clear();
    positionalDeleteFilters.clear();

    // build an inverse mapping from delete file path to a sorted list of data file paths
    dataFilesByDeleteFile.clear();
    dataFileInfo.keySet().stream().sorted().forEachOrdered(dataFile ->
        dataFileInfo.get(dataFile).getDeleteFiles().forEach(deleteFile ->
            dataFilesByDeleteFile.computeIfAbsent(deleteFile, k -> new ArrayList<>()).add(dataFile)));
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(RuntimeException.class, deleteFileReaders.values());
  }

  private PositionalDeleteFileReader getOrCreateReader(String deleteFilePath) {
    Preconditions.checkState(dataFilesByDeleteFile.containsKey(deleteFilePath));
    return deleteFileReaders.computeIfAbsent(deleteFilePath, p -> {
      PositionalDeleteFileReader reader = readerFactory.create(context, Path.of(p), dataFilesByDeleteFile.get(p));
      try {
        reader.setup();
        return reader;
      } catch (ExecutionSetupException e) {
        AutoCloseables.close(RuntimeException.class, reader);
        throw new RuntimeException(e);
      }
    });
  }

  private PositionalDeleteFilter getOrCreateFilter(String dataFilePath, List<String> deleteFiles) {
    return positionalDeleteFilters.computeIfAbsent(dataFilePath, path -> {
      // Fetch the iterator creators for each delete file outside of the supplier... this serves two purposes:
      //  - it will start prefetching of the delete files, and
      //  - for the last data file in a batch, it will capture the creator instances as part of the supplier
      //    lambda & keep them alive, since the new batch will clear the deleteFileIterators map and create new
      //    instances
      List<PositionalDeleteFileReader> readers = deleteFiles.stream()
          .map(this::getOrCreateReader)
          .collect(Collectors.toList());

      // The Supplier pattern is used here so that the DeleteFileIterator can be lazily advanced when the corresponding
      // data file reader is setup, not when it is created for prefetching purposes.
      // IMPORTANT: This supplier CANNOT access any internal object state directly, as a supplier for the last data
      // file from the previous batch may be called after internal state has been reset to handle the next batch.
      Supplier<PositionalDeleteIterator> supplier = () -> {
        List<PositionalDeleteIterator> iterators = readers.stream()
            .map(r -> r.createIteratorForDataFile(path))
            .collect(Collectors.toList());
        return MergingPositionalDeleteIterator.merge(iterators);
      };

      int initialRefCount = dataFileInfo.get(path).getNumRowGroups();
      return new PositionalDeleteFilter(supplier, initialRefCount);
    });
  }

  /**
   * Information about data files that needs to be passed from ParquetScanTableFunction to
   * PositionalDeleteFilterFactory.
   */
  public static class DataFileInfo {

    private final String dataFile;
    private final List<String> deleteFiles;
    private int numRowGroups;

    public DataFileInfo(String dataFile, List<String> deleteFiles, int numRowGroups) {
      this.dataFile = dataFile;
      this.deleteFiles = deleteFiles;
      this.numRowGroups = numRowGroups;
    }

    public String getDataFile() {
      return dataFile;
    }

    public List<String> getDeleteFiles() {
      return deleteFiles;
    }

    public int getNumRowGroups() {
      return numRowGroups;
    }

    public void addRowGroups(int additionalRowGroups) {
      numRowGroups += additionalRowGroups;
    }
  }
}
