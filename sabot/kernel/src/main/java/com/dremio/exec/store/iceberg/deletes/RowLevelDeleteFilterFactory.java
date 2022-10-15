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

import static com.dremio.sabot.op.scan.ScanOperator.Metric.PARQUET_BYTES_READ;
import static com.dremio.sabot.op.tablefunction.TableFunctionOperator.Metric.NUM_DELETE_FILE_READERS;
import static com.dremio.sabot.op.tablefunction.TableFunctionOperator.Metric.PARQUET_DELETE_FILE_BYTES_READ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.iceberg.FileContent;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.DelegatingOperatorContext;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.google.common.base.Preconditions;

/**
 * Factory for creating {@link PositionalDeleteFilter} and {@link EqualityDeleteFilter} instances.  This factory will
 * try to optimize the use of filters and delete file reader instances across a ScanTableFunction batch.
 *
 * <p><h3>Positional delete handling</h3>
 *
 * <p>For each data file, a {@link PositionalDeleteFilter} instance is cached.  This instance will be reused for
 * subsequent row groups scanned for that data file in the batch.  These row groups may either be from separate block
 * splits, or multiple row groups expanded from a single block split.
 *
 * <p>Each {@link PositionalDeleteFilter} associated with a data file may read from multiple delete files.  These
 * delete file instances are reused across multiple data files in the batch, such that there is only one reader
 * instantiated per delete file per batch.  For this to work, data files within the batch must be processed in sorted
 * order. {@link com.dremio.exec.store.parquet.ParquetSplitReaderCreatorIterator ParquetSplitReaderCreatorIterator}
 * guarantees this for Parquet-based Iceberg table scans.
 *
 * <p>Both {@link PositionalDeleteFilter} and {@link PositionalDeleteFileReader} instances are reference counted so
 * they can be closed immediately when they are no longer needed.
 * <ul>
 *   <li>The filter's reference count is the count of row groups to be scanned for the data file the filter
 *    is associated with.  The filter is closed once the last row group is scanned.</li>
 *   <li>The file reader's reference count is the number of data files that need to use that delete file as
 *    part of their filter.  The reader is closed once the last filter that depends on it is closed.</li>
 * </ul>
 *
 * <p><h3>Equality delete handling</h3>
 *
 * <p>{@link EqualityDeleteFilter} instances are handled similarly.  For each data file, a filter instance is cached.
 * This instance will be used for subsequent row groups scanned for that data file in the batch.
 *
 * <p>For equality deletes, neither filters nor the {@link EqualityDeleteHashTable} instances they wrap are shared
 * across data files.  This is currently done to minimize memory use, as well as to keep the implementation simple.
 *
 * <p><h3>Call sequence</h3>
 *
 * <p>The call sequence to {@link RowLevelDeleteFilterFactory} is as follows:
 * <ul>
 *   <li>setDataFileInfoForBatch() is called at the start of each batch with the following information for each data
 *   file in the batch:
 *     <ul>
 *       <li>The list of applicable delete file paths</li>
 *       <li>The number of splits in the batch for the data file - assuming 1 row group per split</li>
 *     </ul></li>
 *   <li>As the ParquetSplitReaderCreatorIterator handles each split, if the split expands into more than one rowgroup,
 *   it must call adjustRowGroupCount() for that data file.  This is necessary so the refcount on the associated filter
 *   for that file can be increased.</li>
 *   <li>If ParquetSplitReaderCreatorIterator filters out an entire split for an input data file, it will call
 *   adjustRowGroupCount with a negative delta to decrement reference counts on the associated filters and delete
 *   files.</li>
 *   <li>For each data file rowgroup to be scanned, createPositionalDeleteFilter() and createEqualityDeleteFilter()
 *   is called to get the respective filters, or null if no such filters exist, for that data file.  For rowgroups
 *   after the 1st, this just returns the cached filter if one exists.</li>
 * </ul>
 */
public class RowLevelDeleteFilterFactory implements AutoCloseable {

  private final OperatorContext context;
  private final OperatorStats baseStats;
  private final RowLevelDeleteFileReaderFactory readerFactory;
  private final Map<String, List<String>> dataFilesByDeleteFile = new HashMap<>();
  private final Map<String, PositionalDeleteFileReader> positionalDeleteFileReaders = new HashMap<>();
  private final Map<String, PositionalDeleteFilter> positionalDeleteFilters = new HashMap<>();
  private final Map<String, EqualityDeleteFilter> equalityDeleteFilters = new HashMap<>();

  private Map<String, DataFileInfo> dataFileInfo;

  public RowLevelDeleteFilterFactory(OperatorContext context, RowLevelDeleteFileReaderFactory readerFactory) {
    // Create an OperatorContext which has an independent OperatorStats instance, and delegates all other calls to
    // the context that was provided.  This allows for isolating metrics recorded by Parquet readers for delete files.
    // Specific metrics that need to be preserved are transferred to the base OperatorStats instance when execution
    // is complete - see the updateBaseStats() method for metrics that are preserved.
    this.context = createDeleteFileStatsContext(context);
    this.baseStats = context.getStats();
    this.readerFactory = Preconditions.checkNotNull(readerFactory);
  }

  public PositionalDeleteFilter createPositionalDeleteFilter(String dataFilePath) {
    PositionalDeleteFilter positionalDeleteFilter = null;
    if (dataFileInfo != null && dataFileInfo.containsKey(dataFilePath)) {
      List<DeleteFileInfo> deleteFiles = dataFileInfo.get(dataFilePath).getPositionalDeleteFiles();
      if (deleteFiles != null && !deleteFiles.isEmpty()) {
        positionalDeleteFilter = getOrCreatePositionalDeleteFilter(dataFilePath, deleteFiles);
      }
    }

    return positionalDeleteFilter;
  }

  public EqualityDeleteFilter createEqualityDeleteFilter(String dataFilePath,
      List<IcebergProtobuf.IcebergSchemaField> icebergColumnIds) {
    EqualityDeleteFilter equalityDeleteFilter = null;
    if (dataFileInfo != null && dataFileInfo.containsKey(dataFilePath)) {
      List<DeleteFileInfo> deleteFiles = dataFileInfo.get(dataFilePath).getEqualityDeleteFiles();
      if (deleteFiles != null && !deleteFiles.isEmpty()) {
        equalityDeleteFilter = getOrCreateEqualityDeleteFilter(dataFilePath, deleteFiles, icebergColumnIds);
      }
    }

    return equalityDeleteFilter;
  }

  public void adjustRowGroupCount(String dataFilePath, int delta) {
    if (dataFileInfo != null && dataFileInfo.containsKey(dataFilePath)) {
      DataFileInfo info = dataFileInfo.get(dataFilePath);
      info.addRowGroups(delta);

      // adjust the refcount on the PositionalDeleteFilters/EqualityDeleteFilters for this data file, if we have any,
      // so that the lifetime is updated to reflect the adjusted rowgroup count
      PositionalDeleteFilter positionalDeleteFilter = positionalDeleteFilters.get(dataFilePath);
      if (positionalDeleteFilter != null) {
        if (delta > 0) {
          positionalDeleteFilter.retain(delta);
        } else if (delta < 0) {
          positionalDeleteFilter.release(-delta);
        }
      }

      EqualityDeleteFilter equalityDeleteFilter = equalityDeleteFilters.get(dataFilePath);
      if (equalityDeleteFilter != null) {
        if (delta > 0) {
          equalityDeleteFilter.retain(delta);
        } else if (delta < 0) {
          equalityDeleteFilter.release(-delta);
        }
      }

      // if the rowgroup count adjustment results in the data file being skipped completely, decrement
      // refcounts on associated positional delete files, and then remove the data file from all maps
      if (info.getNumRowGroups() == 0) {
        for (DeleteFileInfo deleteFile : info.getDeleteFiles()) {
          if (deleteFile.getContent() == FileContent.POSITION_DELETES &&
              positionalDeleteFileReaders.containsKey(deleteFile.getPath())) {
            PositionalDeleteFileReader reader = positionalDeleteFileReaders.get(deleteFile.getPath());
            reader.release();
            if (reader.refCount() == 0) {
              positionalDeleteFileReaders.remove(deleteFile.getPath());
            }
          }
        }

        dataFileInfo.remove(dataFilePath);
        positionalDeleteFilters.remove(dataFilePath);
        equalityDeleteFilters.remove(dataFilePath);
      }
    }
  }

  public void setDataFileInfoForBatch(Map<String, DataFileInfo> dataFileInfo) {
    this.dataFileInfo = dataFileInfo;

    // We can't share filters/open delete file readers across batches, so clear the respective maps for each.
    // The objects themselves will get closed when dependent data file readers are complete - on start of a new
    // batch the last reader from the previous batch still has not executed, so we can't arbitrarily close filters or
    // delete file readers here.
    positionalDeleteFileReaders.clear();
    positionalDeleteFilters.clear();
    equalityDeleteFilters.clear();

    // build an inverse mapping from delete file path to a sorted list of data file paths
    dataFilesByDeleteFile.clear();
    dataFileInfo.keySet().stream().sorted().forEachOrdered(dataFile ->
        dataFileInfo.get(dataFile).getDeleteFiles().forEach(deleteFile ->
            dataFilesByDeleteFile.computeIfAbsent(deleteFile.getPath(), k -> new ArrayList<>()).add(dataFile)));
  }

  @Override
  public void close() throws Exception {
    updateBaseStats();
    AutoCloseables.close(RuntimeException.class, positionalDeleteFileReaders.values());
  }

  private PositionalDeleteFileReader getOrCreatePositionalDeleteReader(DeleteFileInfo deleteFile) {
    Preconditions.checkArgument(deleteFile.getContent() == FileContent.POSITION_DELETES);
    Preconditions.checkState(dataFilesByDeleteFile.containsKey(deleteFile.getPath()));
    return positionalDeleteFileReaders.computeIfAbsent(deleteFile.getPath(), p -> {
      PositionalDeleteFileReader reader = readerFactory.createPositionalDeleteFileReader(context, Path.of(p),
          dataFilesByDeleteFile.get(p));
      try {
        reader.setup();
        baseStats.addLongStat(NUM_DELETE_FILE_READERS, 1);
        return reader;
      } catch (ExecutionSetupException e) {
        AutoCloseables.close(RuntimeException.class, reader);
        throw new RuntimeException(e);
      }
    });
  }

  private PositionalDeleteFilter getOrCreatePositionalDeleteFilter(String dataFilePath,
      List<DeleteFileInfo> deleteFiles) {
    return positionalDeleteFilters.computeIfAbsent(dataFilePath, path -> {
      // Fetch the iterator creators for each delete file outside of the supplier... this serves two purposes:
      //  - it will start prefetching of the delete files, and
      //  - for the last data file in a batch, it will capture the creator instances as part of the supplier
      //    lambda & keep them alive, since the new batch will clear the deleteFileIterators map and create new
      //    instances
      List<PositionalDeleteFileReader> readers = deleteFiles.stream()
          .map(this::getOrCreatePositionalDeleteReader)
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
      return new PositionalDeleteFilter(supplier, initialRefCount, baseStats);
    });
  }

  private EqualityDeleteFileReader getOrCreateEqualityDeleteReader(DeleteFileInfo deleteFile,
      List<IcebergProtobuf.IcebergSchemaField> icebergColumnIds) {
    Preconditions.checkArgument(deleteFile.getContent() == FileContent.EQUALITY_DELETES);
    EqualityDeleteFileReader reader = readerFactory.createEqualityDeleteFileReader(context,
        Path.of(deleteFile.getPath()), deleteFile.getRecordCount(), deleteFile.getEqualityIds(), icebergColumnIds);
    try {
      reader.setup();
      baseStats.addLongStat(NUM_DELETE_FILE_READERS, 1);
      return reader;
    } catch (ExecutionSetupException e) {
      AutoCloseables.close(RuntimeException.class, reader);
      throw new RuntimeException(e);
    }
  }

  private EqualityDeleteFilter getOrCreateEqualityDeleteFilter(String dataFilePath,
      List<DeleteFileInfo> deleteFiles, List<IcebergProtobuf.IcebergSchemaField> icebergColumnIds) {
    return equalityDeleteFilters.computeIfAbsent(dataFilePath, path -> {
      // Fetch the readers for each equality delete file outside of the supplier below so that prefetching of
      // the files can start immediately
      List<EqualityDeleteFileReader> readers = deleteFiles.stream()
          .map(f -> getOrCreateEqualityDeleteReader(f, icebergColumnIds))
          .collect(Collectors.toList());

      // LazyEqualityDeleteTableSupplier is used here so that the hash tables for each equality delete file are not
      // built until EqualityDeleteFilter.setup is called once the associated data file has started processing
      int initialRefCount = dataFileInfo.get(path).getNumRowGroups();
      return new EqualityDeleteFilter(context.getAllocator(), new LazyEqualityDeleteTableSupplier(readers),
          initialRefCount, baseStats);
    });
  }

  private void updateBaseStats() {
    OperatorStats deleteStats = context.getStats();
    long bytesRead = deleteStats.getLongStat(PARQUET_BYTES_READ);
    if (bytesRead > 0) {
      baseStats.setLongStat(PARQUET_DELETE_FILE_BYTES_READ, bytesRead);
    }
  }

  private static OperatorContext createDeleteFileStatsContext(OperatorContext context) {
    // Create an OperatorContext with a new OperatorStats instance to support delete file stats isolation.  All
    // other calls are delegated to the wrapped context.
    OperatorStats deleteFileStats = new OperatorStats(context.getStats(), true);
    return new DelegatingOperatorContext(context) {

      @Override
      public OperatorStats getStats() {
        return deleteFileStats;
      }
    };
  }

  /**
   * Information about data files that needs to be passed from ParquetScanTableFunction to
   * PositionalDeleteFilterFactory.
   */
  public static class DataFileInfo {

    private final String dataFile;
    private final List<DeleteFileInfo> deleteFiles;
    private int numRowGroups;

    public DataFileInfo(String dataFile, List<DeleteFileInfo> deleteFiles, int numRowGroups) {
      this.dataFile = dataFile;
      this.deleteFiles = deleteFiles;
      this.numRowGroups = numRowGroups;
    }

    public String getDataFile() {
      return dataFile;
    }

    public List<DeleteFileInfo> getDeleteFiles() {
      return deleteFiles;
    }

    public List<DeleteFileInfo> getPositionalDeleteFiles() {
      return deleteFiles.stream().filter(f -> f.content == FileContent.POSITION_DELETES).collect(Collectors.toList());
    }

    public List<DeleteFileInfo> getEqualityDeleteFiles() {
      return deleteFiles.stream().filter(f -> f.content == FileContent.EQUALITY_DELETES).collect(Collectors.toList());
    }

    public int getNumRowGroups() {
      return numRowGroups;
    }

    public void addRowGroups(int additionalRowGroups) {
      numRowGroups += additionalRowGroups;
    }
  }

  public static class DeleteFileInfo {

    private final String path;
    private final FileContent content;
    private final long recordCount;
    private final List<Integer> equalityIds;

    public DeleteFileInfo(String path, FileContent content, long recordCount, List<Integer> equalityIds) {
      this.path = path;
      this.content = content;
      this.recordCount = recordCount;
      this.equalityIds = equalityIds;
    }

    public String getPath() {
      return path;
    }

    public FileContent getContent() {
      return content;
    }

    public long getRecordCount() {
      return recordCount;
    }

    public List<Integer> getEqualityIds() {
      return equalityIds;
    }
  }
}
