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
package com.dremio.service.jobs;

import static com.dremio.common.perf.Timer.time;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.IndexedStore;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.Service;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Stores and manages job results for max 30 days (default).
 * Each node stores job results on local disk.
 */
public class JobResultsStore implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobResultsStore.class);

  private static final FinalizableReferenceQueue FINALIZABLE_REFERENCE_QUEUE = new FinalizableReferenceQueue();

  private final String storageName;
  private final Path jobStoreLocation;
  private final FileSystem dfs;
  private final BufferAllocator allocator;
  private final Set<FinalizableReference> jobResultReferences = Sets.newConcurrentHashSet();
  private final LoadingCache<JobId, JobData> jobResults;
  private final IndexedStore<JobId, JobResult> store;

  public JobResultsStore(final FileSystemPlugin plugin, final IndexedStore<JobId, JobResult> store,
      final BufferAllocator allocator) throws IOException {
    this.storageName = plugin.getName();
    this.dfs = plugin.getSystemUserFS();
    this.jobStoreLocation = plugin.getConfig().getPath();
    this.dfs.mkdirs(jobStoreLocation);
    this.store = store;
    this.allocator = allocator;

    this.jobResults = CacheBuilder.newBuilder()
        .maximumSize(100)
        .expireAfterAccess(15, TimeUnit.MINUTES)
        .build(
            new CacheLoader<JobId, JobData>() {
              @Override
              public JobData load(JobId key) throws Exception {
                // CountDownLatch(0) as jobs are completed and metadata should be already collected
                final JobDataImpl jobDataImpl = new JobDataImpl(new LateJobLoader(key), key, new CountDownLatch(0));
                return newJobDataReference(jobDataImpl);
              }
            });
  }


  /**
   * Get the output table path for the given id
   */
  private List<String> getOutputTablePath(final JobId jobId) {
    // Get the information from the store or fallback to using job id as the table name
    Optional<JobResult> jobResult = Optional.ofNullable(store.get(jobId));
    return jobResult
        .map(result -> getLastAttempt(result).getOutputTableList())
        .orElse(Arrays.asList(storageName, jobId.toString()));
  }

  /** Helper method to get the job output directory */
  private Path getJobOutputDir(final JobId jobId) {
    List<String> outputTablePath = getOutputTablePath(jobId);

    return jobStoreLocation.resolve(Iterables.getLast(outputTablePath));
  }

  public boolean cleanup(JobId jobId) {
    final Path jobOutputDir = getJobOutputDir(jobId);
    try {
      if (dfs.exists(jobOutputDir)) {
        dfs.delete(jobOutputDir, true);
        logger.debug("Deleted job output directory : {}", jobOutputDir);
      }
      return true;
    } catch (IOException e) {
      logger.warn("Could not delete job output directory : " + jobOutputDir, e);
      return false;
    }
  }

  @VisibleForTesting
  public boolean jobOutputDirectoryExists(JobId jobId) {
    final Path jobOutputDir = getJobOutputDir(jobId);
    try {
      return dfs.exists(jobOutputDir);
    } catch (IOException e) {
      return false;
    }
  }

  private JobData newJobDataReference(final JobData delegate) {
    final JobData result = new JobDataWrapper(delegate);
    FinalizableReference ref = new FinalizablePhantomReference<JobData>(result, FINALIZABLE_REFERENCE_QUEUE) {
      @Override
      public void finalizeReferent() {
        jobResultReferences.remove(this);
        try {
          delegate.close();
        } catch (Exception e) {
          logger.warn(String.format("Failed to close the data object for job %s", delegate.getJobId()), e);
        }
      }
    };

    jobResultReferences.add(ref);

    return result;
  }

  JobData cacheNewJob(JobId jobId, JobData data){
    // put this in cache so that others who want it, won't try to read it before it is done running.
    final JobData jobDataRef = newJobDataReference(data);
    jobResults.put(jobId, jobDataRef);

    return jobDataRef;
  }

  private static JobInfo getLastAttempt(JobResult jobResult) {
    return jobResult.getAttemptsList().get(jobResult.getAttemptsList().size() - 1).getInfo();
  }

  public String getJobResultsTableName(JobId jobId) {
    //
    return String.format("TABLE(%s(type => 'arrow'))",
        PathUtils.constructFullPath(getOutputTablePath(jobId)));
  }

  public RecordBatches loadJobData(JobId jobId, JobResult job, int offset, int limit){
    try (TimedBlock b = time("getJobResult")) {

      final List<JobAttempt> attempts = job.getAttemptsList();
      if(attempts.size() > 0) {
        final JobAttempt mostRecentJob = attempts.get(job.getAttemptsList().size() - 1);
        if (mostRecentJob.getState() == JobState.CANCELED) {
          throw UserException.dataReadError()
            .message("Could not load results as the query was canceled")
            .build(logger);
        }
      }

      final Path jobOutputDir = getJobOutputDir(jobId);
      if (!dfs.isDirectory(jobOutputDir)) {
        throw UserException.dataReadError()
            .message("Job '%s' output doesn't exist", jobId.getId())
            .build(logger);
      }

      // Find out the list of batches that contain the given record ranges
      final List<ArrowFileMetadata> resultMetadata = getLastAttempt(job).getResultMetadataList();

      if (resultMetadata == null || resultMetadata.isEmpty()) {
        throw UserException.dataReadError()
            .message("Job " + jobId.getId() + " has no results")
            .build(logger);
      }

      final List<ArrowFileMetadata> resultFilesToRead = Lists.newArrayList();
      int runningFileRecordCount = 0;
      for(ArrowFileMetadata fileMetadata : resultMetadata) {
        if (offset < runningFileRecordCount + fileMetadata.getRecordCount()) {
          resultFilesToRead.add(fileMetadata);

          if (resultFilesToRead.size() == 1) {
            // update the given offset to start from the current file
            offset -= runningFileRecordCount;
          }

          // Check if the offset + limit falls within the current file
          if (offset + limit <= runningFileRecordCount + fileMetadata.getRecordCount()) {
            break;
          }
        }

        runningFileRecordCount += fileMetadata.getRecordCount();
      }

      final List<RecordBatchHolder> batchHolders = Lists.newArrayList();
      if (resultFilesToRead.isEmpty()) {
        // when the query returns no results at all or the requested range is invalid, return an empty record batch
        // for metadata purposes.
        try (ArrowFileReader fileReader = new ArrowFileReader(dfs, jobOutputDir, resultMetadata.get(0), allocator)) {
          batchHolders.addAll(fileReader.read(0, 0));
        }
      } else {
        runningFileRecordCount = 0;
        int remaining = limit;
        for(ArrowFileMetadata file : resultFilesToRead) {

          // Find the starting record index in file
          final long fileOffset = Math.max(0, offset - runningFileRecordCount);

          // Find how many records to read from file.
          // Min of remaining records in file or remaining records in total to read.
          final long fileLimit = Math.min(file.getRecordCount() - fileOffset, remaining);

          try (ArrowFileReader fileReader = new ArrowFileReader(dfs, jobOutputDir, file, allocator)) {
            batchHolders.addAll(fileReader.read(fileOffset, fileLimit));
            remaining -= fileLimit;
          }

          runningFileRecordCount += file.getRecordCount();
        }
      }

      return new RecordBatches(batchHolders);
    } catch(IOException ex){
      throw UserException.dataReadError(ex)
          .message("Failed to load results for job %s", jobId.getId())
          .build(logger);
    }
  }

  public JobData get(JobId jobId) {
    try{
      return jobResults.get(jobId);
    }catch(ExecutionException ex){
      throw Throwables.propagate(ex.getCause());
    }
  }

  private final class LateJobLoader implements JobLoader {

    private final JobId jobId;

    public LateJobLoader(JobId jobId) {
      super();
      this.jobId = jobId;
    }

    @Override
    public RecordBatches load(int offset, int limit) {
      return loadJobData(jobId, store.get(jobId), offset, limit);
    }

    @Override
    public void waitForCompletion() {
      // no-op as we are reading the results from an already completed job.
    }

    @Override
    public String getJobResultsTable() {
      return getJobResultsTableName(jobId);
    }
  }

  @Override
  public void start() throws Exception {
    // TODO reclaim space
  }

  @Override
  public void close() throws Exception {
    // TODO
    // wait for current operations
    // reclaim space


    jobResults.invalidateAll();
    jobResults.cleanUp();

    // Closing open references
    Iterator<FinalizableReference> iterator = jobResultReferences.iterator();
    while(iterator.hasNext()) {
      FinalizableReference ref = iterator.next();

      ref.finalizeReferent();
    }
  }
}
