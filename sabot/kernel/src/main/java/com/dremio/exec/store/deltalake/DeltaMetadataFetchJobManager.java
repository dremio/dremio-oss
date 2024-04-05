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

package com.dremio.exec.store.deltalake;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for creating a list of {@link DeltaLogSnapshot} having metadata info
 * like schema, dataset size etc required for planning a query on a Delta dataset.
 *
 * <p>Creates and manages all the {@link DeltaMetadataFetchJob} each of which may create one {@link
 * DeltaLogSnapshot}. {@link DeltaMetadataFetchJob } are created in batches using {@link
 * DeltaMetadataFetchJobProducer} and are submitted to the thread pool {@link
 * DeltaMetadataFetchPool} in batches. All the snapshots created in a batch are then collected in a
 * snapshots list.
 *
 * <p>Final list returned by method getListOfSnapshots. Call to this method is blocks the calling
 * thread until the fetch is complete.
 *
 * <p>Initialisations possible are: 1) travelRequest == null then will return the list of snapshots
 * required for reading the latest version of the delta table 2) If a travelRequest is supplied then
 * will return the list of snapshots required for reading that particular version.
 */
@NotThreadSafe
public class DeltaMetadataFetchJobManager {

  private static final Logger logger = LoggerFactory.getLogger(DeltaMetadataFetchJobManager.class);

  private final FileSystem fs;
  private final SabotContext context;
  private final String selectionRoot;
  private final DeltaVersion startVersion;

  private final ThreadPoolExecutor threadPool = DeltaMetadataFetchPool.getPool();
  private final List<DeltaLogSnapshot> snapshots = new ArrayList<>();
  private boolean fetchedMetadata = false;
  private final Path metadataDir;

  private final BatchReader batchReader;

  public DeltaMetadataFetchJobManager(
      SabotContext context,
      FileSystem fs,
      FileSelection fileSelection,
      TimeTravelOption.TimeTravelRequest travelRequest) {
    this(context, fs, fileSelection.getSelectionRoot(), travelRequest);
  }

  public DeltaMetadataFetchJobManager(
      SabotContext context,
      FileSystem fs,
      String selectionRoot,
      TimeTravelOption.TimeTravelRequest travelRequest) {
    this.fs = fs;
    this.context = context;
    this.selectionRoot = selectionRoot;
    this.metadataDir = Path.of(selectionRoot).resolve(DeltaConstants.DELTA_LOG_DIR);

    DeltaVersionResolver resolver = new DeltaVersionResolver(fs, metadataDir);
    this.startVersion = resolver.resolve(travelRequest);

    DeltaMetadataFetchJobProducer producer =
        new DeltaMetadataFetchJobProducer(context, fs, metadataDir, startVersion);
    this.batchReader = new BatchReader(threadPool, producer);
  }

  public List<DeltaLogSnapshot> getListOfSnapshots() {
    if (fetchedMetadata) {
      return snapshots;
    }

    logger.debug(
        "Starting metadata fetch for delta dataset {}. Manager State {}",
        selectionRoot,
        this.toString());

    while (batchReader.readNextBatch()) {
      List<DeltaLogSnapshot> batch = batchReader.readBatch();
      snapshots.addAll(batch);
    }

    logger.debug(
        "Finished metadata fetch for delta dataset {}. Manager State {}",
        selectionRoot,
        this.toString());
    fetchedMetadata = true;

    return snapshots;
  }

  public boolean isReadLatest() {
    return startVersion.isCheckpoint();
  }

  public void setBatchSize(int batchSize) {
    batchReader.setBatchesSize(batchSize);
  }

  public int getBatchesRead() {
    return batchReader.getBatchesRead();
  }

  @Override
  public String toString() {
    return "DeltaMetadataFetchJobManager{"
        + "fs="
        + fs
        + ", selectionRoot="
        + selectionRoot
        + ", startVersion="
        + startVersion
        + ", threadPool="
        + threadPool
        + ", snapshots="
        + snapshots
        + ", fetchedMetadata="
        + fetchedMetadata
        + ", metadataDir="
        + metadataDir
        + '}';
  }

  private final class BatchReader {

    private final ThreadPoolExecutor threadPool;
    private final DeltaMetadataFetchJobProducer producer;

    private int batchesRead = 0;
    private final AtomicBoolean readNextBatch = new AtomicBoolean(true);
    private int batchSize = 10;

    private BatchReader(ThreadPoolExecutor threadPool, DeltaMetadataFetchJobProducer producer) {
      this.threadPool = threadPool;
      this.producer = producer;
    }

    private List<DeltaLogSnapshot> readBatch() {

      logger.debug(
          "Reading batch {} of size {} of metadata data files for delta dataset at {}. Manager State {}",
          batchesRead,
          batchSize,
          selectionRoot,
          this.toString());
      List<CompletableFuture<DeltaLogSnapshot>> futures = startAsyncReads();

      if (futures.isEmpty()) {
        return Collections.emptyList();
      }

      // collect the results from all the futures.
      List<DeltaLogSnapshot> snapshotList =
          futures.stream().map(x -> x.join()).filter(Objects::nonNull).collect(Collectors.toList());

      logger.debug(
          "Batch {} of size {} of metadata data files for delta dataset at {} was read successfully. Manager State {}",
          batchesRead,
          batchSize,
          selectionRoot,
          this.toString());

      if (!startVersion.isCheckpoint()) {
        logger.debug(
            "Stopping read of delta dataset at path {} as a checkpoint file was found. Manager State {}",
            selectionRoot,
            this.toString());
        stopIfCheckpointFound(snapshotList);
      }

      batchesRead++;
      return snapshotList;
    }

    private void stopIfCheckpointFound(List<DeltaLogSnapshot> snapshots) {
      boolean checkpointFound = snapshots.stream().anyMatch(x -> x.containsCheckpoint());
      if (checkpointFound) {
        stopRead();
      }
    }

    private List<CompletableFuture<DeltaLogSnapshot>> startAsyncReads() {
      Integer jobsProduced = 0;
      List<DeltaMetadataFetchJob> jobs = new ArrayList<>(batchSize);

      while (producer.hasNext() && jobsProduced++ < batchSize) {
        jobs.add(producer.next());
      }

      if (!producer.hasNext()) {
        stopRead();
      }

      // submit the jobs to the pool and get a list of futures
      return jobs.stream().map(job -> submit(job)).collect(Collectors.toList());
    }

    private CompletableFuture<DeltaLogSnapshot> submit(DeltaMetadataFetchJob job) {
      return CompletableFuture.supplyAsync(job, threadPool)
          .exceptionally(
              e -> {
                CompletionException exp = (CompletionException) e;
                if (exp.getCause() instanceof DeltaMetadataFetchJob.InvalidFileException) {
                  logger.debug(
                      "Metadata read completed for Deltatable {}. Cause {}. Manager State {}",
                      selectionRoot,
                      exp.getCause().getMessage(),
                      this.toString());
                  stopRead();
                  return null;
                }

                if (exp.getCause() instanceof IOException) {
                  logger.error("Unexpected error occurred in DeltaMetadataFetchJob. Error", exp);

                  throw UserException.dataReadError(exp.getCause())
                      .message("Failed to read metadata for delta dataset %s", selectionRoot)
                      .build(logger);
                }

                return null;
              });
    }

    private void stopRead() {
      readNextBatch.set(false);
    }

    private int getBatchesRead() {
      return batchesRead;
    }

    private boolean readNextBatch() {
      return readNextBatch.get();
    }

    private void setBatchesSize(int batchSize) {
      this.batchSize = batchSize;
    }
  }
}
