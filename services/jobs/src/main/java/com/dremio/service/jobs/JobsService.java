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

import java.util.List;

import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.service.Service;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.MaterializationSummary;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;

/**
 * Job Service interface. Submit job, maintain job states
 */
public interface JobsService extends Service {

  /**
   * Submit job to execution engine.
   * @param query sql query to be executed.
   * @param queryType type of query.
   * @param datasetPath dataset path job will run on.
   * @param version version of dataset job will run on.
   * @param statusListener a listener to notify for change of status. Must not be null.
   * @return Job associated for given query and dataset.
   */
  Job submitJob(SqlQuery query, QueryType queryType, NamespaceKey datasetPath, DatasetVersion version, JobStatusListener statusListener);

  /**
   * Submit job to execution engine instructing it to exclude given materializations from acceleration.
   *
   * @param query sql query to be executed.
   * @param queryType type of query.
   * @param datasetPath dataset path job will run on.
   * @param version version of dataset job will run on.
   * @param exclusions list of materialization identifiers that shall be excluded from acceleration.
   * @param statusListener a listener to notify for change of status. Must not be null.
   * @param materializationSummary information related to materialization like materializationId and layoutId
   * @return Job associated for given query and dataset.
   */
  Job submitJobWithExclusions(SqlQuery query, QueryType queryType, NamespaceKey datasetPath, DatasetVersion version, List<String> exclusions, JobStatusListener statusListener, MaterializationSummary materializationSummary);

  /**
   * Run sql query directly.
   * @param query sql query.
   * @param type type of query.
   * @return Job associated with sql query.
   */
  Job submitExternalJob(final SqlQuery query, QueryType type);

  /**
   * Submit a download job.
   * @param query sql query.
   * @param downloadId download id used to find output of job.
   * @param fileName filename to use when data is downloaded.
   * @return Job associated with sql query.
   */
  Job submitDownloadJob(final SqlQuery query, String downloadId, String fileName);

  /**
   * Get job information.
   * @param jobId job id.
   * @return Current Job state for given id, {@code null} if job id is not found in current running jobs.
   */
  Job getJob(JobId jobId);


  /**
   * Get job information.
   * @param jobId job id.
   * @return Current Job state for given id, {@code null} if job id is not found in current running jobs.
   */
  Job getJobChecked(JobId jobId) throws JobNotFoundException;

  /**
   * Get JobData for completed job
   * @param jobId job id
   * @param offset result offset
   * @param limit result limit
   * @return
   */
  JobDataFragment getJobData(JobId jobId, int limit);

  /**
   * Get all jobs including running jobs.
   * @param filterString How to filter jobs.
   * @param sortColumn Column to sort the results on.
   * @param sortOrder Order for sorting.
   * @param offset start listing jobs from offset
   * @param limit number of jobs to return
   * @param userName filter jobs for given user
   * @param isAdmin if user has admin privileges.
   * @return Jobs associated with the conditions in the expected.
   */
  Iterable<Job> getAllJobs(final String filterString, final String sortColumn, final SortOrder sortOrder, int offset, int limit, String userName);

  /**
   * Get the number of jobs run for a given path and version.
   * @param datasetPath Path of Dataset (any version)
   * @return The count of jobs.
   */
  int getJobsCount(final NamespaceKey datasetPath);

  /**
   * Get the number of jobs run for given datasets.
   * @param datasetPaths list of dataset paths.
   * @return list of counts
   */
  List<Integer> getJobsCount(final List<NamespaceKey> datasetPaths);

  /**
   * Get the number of jobs run for a given path and version.
   * @param datasetPath Path of Dataset
   * @param datasetVersion Version for Dataset (or null for all versions)
   * @return The count of jobs.
   */
  int getJobsCountForDataset(final NamespaceKey datasetPath, final DatasetVersion datasetVersion);

  /**
   * Get list of jobs run for a given path.
   * @param datasetPath Path of Dataset
   *
   * @return The list of jobs.
   */
  Iterable<Job> getJobsForDataset(final NamespaceKey datasetPath, int limit);

  /**
   * Get list of jobs run for a given path and version.
   *
   * Sorted in descending order by:
   * - START_TIME
   * - FINISH_TIME
   * - JOBID
   *
   * @param datasetPath Path of dataset
   * @param version Version for dataset (or null for all versions)
   * @return The list of jobs.
   */
  Iterable<Job> getJobsForDataset(final NamespaceKey datasetPath, final DatasetVersion version, int limit);

  /**
   * Get list of jobs that have the provided parent
   * @param datasetPath the path of the parent
   * @return the corresponding jobs
   */
  Iterable<Job> getJobsForParent(NamespaceKey datasetPath, int limit);


  /**
   * Retrieve the query profile of jobId and attempt
   * @param jobId
   * @param attempt attempt number
   * @return
   */
  QueryProfile getProfile(JobId jobId, int attempt);

  /**
   * Cancel the provided jobId as the provided user.
   * @param username The user causing the cancellation (to be used for security verification)
   * @param jobId The job id to cancel
   * @return The outcome of the cancellation attempt. (Cancellation is asynchronous.)
   */
  void cancel(String username, JobId jobId) throws JobException;

  /**
   * Register a listener that listens for events associated with a particular job.
   *
   * Throws exception if the requested JobId is not currently active.
   * @param jobId JobId to listen to.
   * @param listener The listener to be informed of job update events.
   */
  void registerListener(JobId jobId, ExternalStatusListener listener);
}


