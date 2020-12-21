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
import static com.dremio.exec.store.easy.arrow.ArrowFileReader.fromBean;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.exec.store.JobResultsStoreConfig;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadataValidator;
import com.dremio.exec.store.easy.arrow.ArrowFileReader;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.Service;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Stores and manages job results for max 30 days (default).
 * Each executor node stores job results on local disk.
 */
public class JobResultsStore implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobResultsStore.class);

  private final String storageName;
  private final Path jobStoreLocation;
  private final FileSystem dfs;
  private final BufferAllocator allocator;
  private final LegacyIndexedStore<JobId, JobResult> store;

  public JobResultsStore(
      final JobResultsStoreConfig resultsStoreConfig,
      final LegacyIndexedStore<JobId, JobResult> store,
      final BufferAllocator allocator
  ) throws IOException {
    this.storageName = resultsStoreConfig.getStorageName();
    this.dfs = resultsStoreConfig.getFileSystem();
    this.jobStoreLocation = resultsStoreConfig.getStoragePath();
    dfs.mkdirs(jobStoreLocation);

    this.store = store;
    this.allocator = allocator;
  }

  /**
   * Get the output table path for the given id
   */
  public List<String> getOutputTablePath(final JobId jobId) {
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
      if (doesQueryResultsDirExists(jobOutputDir, jobId)) {
        deleteQueryResults(jobOutputDir, true, jobId);
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
      return doesQueryResultsDirExists(jobOutputDir, jobId);
    } catch (IOException e) {
      return false;
    }
  }

  protected static JobInfo getLastAttempt(JobResult jobResult) {
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
        } else if (mostRecentJob.getState() == JobState.FAILED) {
          String failureMessage = mostRecentJob.getInfo().getDetailedFailureInfo().getErrorsList().get(0).getMessage();
          throw UserException.dataReadError()
            .message(failureMessage)
            .build(logger);
        }
      }

      final Path jobOutputDir = getJobOutputDir(jobId);
      if (!doesQueryResultsDirExists(jobOutputDir, jobId)) {
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
      int remainingRecords = limit;
      for(ArrowFileMetadata fileMetadata : resultMetadata) {
        if (offset < runningFileRecordCount + fileMetadata.getRecordCount()) {
          resultFilesToRead.add(fileMetadata);

          // offset within current file to read records
          long fileOffset = 0; // fileOffset will be 0 from second file to be read for records.
          if (resultFilesToRead.size() == 1) {
            // update the given offset to start from the current file
            offset -= runningFileRecordCount;
            fileOffset = offset; // fileOffset will be "offset" for the first file to be read for records.
          }

          // Find how many records to read from current file.
          // Min of remaining records in file or remaining records in total to read.
          long fileLimit = Math.min(fileMetadata.getRecordCount() - fileOffset, remainingRecords);
          remainingRecords -= fileLimit;

          // stop including files if there are no remainingRecords to be included.
          if (remainingRecords <=0) {
            break;
          }
        }

        runningFileRecordCount += fileMetadata.getRecordCount();
      }

      final List<RecordBatchHolder> batchHolders = Lists.newArrayList();
      if (resultFilesToRead.isEmpty()) {
        // when the query returns no results at all or the requested range is invalid, return an empty record batch
        // for metadata purposes.
        batchHolders.addAll(getQueryResults(jobOutputDir, resultMetadata.get(0), allocator, 0, 0));

      } else {
        runningFileRecordCount = 0;
        int remaining = limit;
        for(ArrowFileMetadata file : resultFilesToRead) {

          // Find the starting record index in file
          final long fileOffset = Math.max(0, offset - runningFileRecordCount);

          // Find how many records to read from file.
          // Min of remaining records in file or remaining records in total to read.
          final long fileLimit = Math.min(file.getRecordCount() - fileOffset, remaining);

          batchHolders.addAll(getQueryResults(jobOutputDir, file, allocator, fileOffset, fileLimit));
          remaining -= fileLimit;

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

  private boolean shouldSkipJobResults(JobId jobId) {
    final JobResult job = store.get(jobId);
    final List<ArrowFileMetadata> resultMetadata = getLastAttempt(job).getResultMetadataList();
    return resultMetadata != null && !resultMetadata.isEmpty() &&
      resultMetadata.stream().anyMatch(ArrowFileMetadataValidator::hasInvalidUnions);
  }

  protected List<RecordBatchHolder> getQueryResults(Path jobOutputDir,
                                                    ArrowFileMetadata arrowFileMetadata,
                                                    BufferAllocator allocator,
                                                    long fileOffset,
                                                    long fileLimit) throws IOException {
    try(ArrowFileReader fileReader = new ArrowFileReader(dfs, jobOutputDir, arrowFileMetadata, allocator)) {
      return fileReader.read(fileOffset, fileLimit);
    }
  }

  /**
   * Check if query results directory exists, optionally using jobId
   *
   * @param jobOutputDir query results directory
   * @param jobId might be used in derived class.
   * @return
   * @throws IOException
   */
  protected boolean doesQueryResultsDirExists(Path jobOutputDir, JobId jobId) throws IOException {
    if (shouldSkipJobResults(jobId)) {
      return false;
    }

    Set<NodeEndpoint> nodeEndpoints = getNodeEndpoints(jobId);
    if (nodeEndpoints == null || nodeEndpoints.isEmpty()) {
      logger.debug("There are no nodeEndpoints where query results dir existence need to be checked." +
                   "For eg: for jdbc queries, results are not stored on executors.");
      return false;
    }
    return dfs.exists(jobOutputDir);
  }

  /**
   * Delete the path recursively optionally using jobId
   *
   * @param jobOutputDir
   * @param recursive
   * @param jobId Used in derived class.
   * @return
   * @throws IOException
   */
  protected boolean deleteQueryResults(Path jobOutputDir, boolean recursive, JobId jobId) throws IOException {
    return dfs.delete(jobOutputDir, recursive);
  }

  public JobData get(JobId jobId) {
    return new JobDataImpl(new LateJobLoader(jobId), jobId);
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
  }

  protected Set<NodeEndpoint> getNodeEndpoints(JobId jobId) {
    JobResult jobResult = store.get(jobId);
    List<ArrowFileMetadata> arrowFileMetadataList = getLastAttempt(jobResult).getResultMetadataList();

    Set<NodeEndpoint> nodeEndpoints = Sets.newHashSet();

    if (arrowFileMetadataList == null || arrowFileMetadataList.isEmpty()) {
      return nodeEndpoints;
    }

    for(ArrowFileMetadata afm: arrowFileMetadataList) {
      com.dremio.exec.proto.beans.NodeEndpoint screenNodeEndpoint = afm.getScreenNodeEndpoint();

      if (screenNodeEndpoint != null) {
        nodeEndpoints.add(fromBean(screenNodeEndpoint));
      }
    }
    return nodeEndpoints;
  }
}
