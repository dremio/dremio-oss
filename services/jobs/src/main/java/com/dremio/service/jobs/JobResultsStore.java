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
package com.dremio.service.jobs;

import static com.dremio.common.perf.Timer.time;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.IndexedStore;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.service.Service;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Stores and manages job results for max 30 days (default).
 * Each node stores job results on local disk.
 */
public class JobResultsStore implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobResultsStore.class);

  private final String storageName;
  private final Path jobStoreLocation;
  private final FileSystem dfs;
  private final BufferAllocator allocator;
  private final LoadingCache<JobId, JobDataImpl> jobResults;
  private final IndexedStore<JobId, JobResult> store;

  public JobResultsStore(final FileSystemPlugin plugin, final IndexedStore<JobId, JobResult> store,
      final BufferAllocator allocator) throws IOException {
    this.storageName = plugin.getStorageName();
    this.dfs = plugin.getFS(ImpersonationUtil.getProcessUserName());
    this.jobStoreLocation = new Path(plugin.getConfig().getPath());
    this.dfs.mkdirs(jobStoreLocation);
    this.store = store;
    this.allocator = allocator;

    this.jobResults = CacheBuilder.newBuilder()
        .maximumSize(100)
        .removalListener(new RemovalListener<JobId, JobDataImpl>() {
          @Override
          public void onRemoval(RemovalNotification<JobId, JobDataImpl> notification) {
            try {
              notification.getValue().close();
            } catch (Exception e) {
              logger.warn(String.format("Failed to close the data object for job %s", notification.getValue().getJobId()), e);
            }
          }
        })
        .expireAfterAccess(15, TimeUnit.MINUTES)
        .build(
            new CacheLoader<JobId, JobDataImpl>() {
              @Override
              public JobDataImpl load(JobId key) throws Exception {
                return new JobDataImpl(new LateJobLoader(key), key);
              }
            });
  }

  /** Helper method to get the job output directory */
  private Path getJobOutputDir(final JobId jobId) {
    return new Path(jobStoreLocation, jobId.getId());
  }

  public boolean cleanup(JobId jobId) {
    final Path jobOutputDir = getJobOutputDir(jobId);
    try {
      dfs.delete(jobOutputDir, true);
      logger.info("Deleted job output directory : " + jobOutputDir);
      return true;
    } catch (IOException e) {
      logger.warn("Could not delete job output directory : " + jobOutputDir, e);
      return false;
    }
  }

  public boolean isOld(JobResult jobResult, long cutOffTime) {
    JobInfo jobInfo = getLastAttempt(jobResult);
    return jobInfo.getFinishTime() < cutOffTime;
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

  void cacheNewJob(JobId jobId, JobDataImpl data){
    // put this in cache so that others who want it, won't try to read it before it is done running.
    jobResults.put(jobId, data);
  }

  private static JobInfo getLastAttempt(JobResult jobResult) {
    return jobResult.getAttemptsList().get(jobResult.getAttemptsList().size() - 1).getInfo();
  }

  public String getJobResultsTableName(JobId jobId) {
    return String.format("TABLE(%s(type => 'arrow'))",
        PathUtils.constructFullPath(ImmutableList.of(storageName, jobId.getId())));
  }

  public RecordBatches loadJobData(JobId jobId, JobResult job, int offset, int limit){
    try (TimedBlock b = time("getJobResult")) {
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

  public JobDataImpl get(JobId jobId) {
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
  }
}
