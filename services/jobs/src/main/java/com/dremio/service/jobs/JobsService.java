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

import java.security.AccessControlException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.service.Service;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;

/**
 * Job Service interface. Submit job, maintain job states
 */
public interface JobsService extends Service {

  /**
   * Submit a job to the execution engine.
   *
   * @param id              external id
   * @param jobRequest      job request
   * @param statusListener  a listener to notify for change of status. Must not be null
   * @return {@link CompletableFuture} of the submitted Job
   */
  CompletableFuture<Job> submitJob(ExternalId id, JobRequest jobRequest, JobStatusListener statusListener);

  /**
   * Submit a job to the execution engine. Generates a random externalId for the job
   *
   * @param jobRequest      job request
   * @param statusListener  a listener to notify for change of status. Must not be null
   * @return {@link CompletableFuture} of the submitted Job
   */
  default CompletableFuture<Job> submitJob(JobRequest jobRequest, JobStatusListener statusListener) {
    return submitJob(ExternalIdHelper.generateExternalId(), jobRequest, statusListener);
  }

  /**
   * Get details of the job.
   *
   * @param jobId job id
   * @return details of the job for given id
   * @throws JobNotFoundException if job is not found
   */
  Job getJob(JobId jobId) throws JobNotFoundException;

  /**
   * Get details of the job, from the kvStore.
   * This is useful if we don't want the details to change while we are consuming them.
   *
   * @param jobId job id
   * @return details of the job for given id
   * @throws JobNotFoundException if job is not found
   */
  Job getJobFromStore(JobId jobId) throws JobNotFoundException;

  /**
   * Get details of the job.
   *
   * @param jobId    job id
   * @param userName username for permission check
   * @return details of the job for given id
   * @throws JobNotFoundException if job is not found
   * @throws AccessControlException if user does not have access to the job
   */
  Job getJob(JobId jobId, String userName) throws JobNotFoundException;

  /**
   * Get all jobs including running jobs.
   *
   * @param filterString How to filter jobs.
   * @param sortColumn   Column to sort the results on.
   * @param sortOrder    Order for sorting.
   * @param offset       start listing jobs from offset
   * @param limit        number of jobs to return
   * @param userName     filter jobs for given user
   * @return Jobs associated with the conditions in the expected.
   */
  Iterable<Job> getAllJobs(String filterString, String sortColumn, SortOrder sortOrder, int offset, int limit,
                           String userName);

  /**
   * Get the number of jobs run for a given path and version.
   *
   * @param datasetPath Path of Dataset (any version)
   * @return The count of jobs.
   */
  int getJobsCount(NamespaceKey datasetPath);

  /**
   * Get the number of jobs run for given datasets.
   *
   * @param datasetPaths list of dataset paths.
   * @return list of counts
   */
  List<Integer> getJobsCount(List<NamespaceKey> datasetPaths);

  /**
   * Get the number of jobs run for a given path and version.
   *
   * @param datasetPath    Path of Dataset
   * @param datasetVersion Version for Dataset (or null for all versions)
   * @return The count of jobs.
   */
  int getJobsCountForDataset(NamespaceKey datasetPath, DatasetVersion datasetVersion);


  /**
   * Get the number of jobs run sorted by job type given a date range
   *
   * @param startDate Start date (inclusive)
   * @param endDate   End date (inclusive)
   * @return The count of jobs.
   */
  List<JobTypeStats> getJobStats(long startDate, long endDate);

  /**
   * Get list of jobs run for a given path.
   *
   * @param datasetPath Path of Dataset
   * @return The list of jobs.
   */
  Iterable<Job> getJobsForDataset(NamespaceKey datasetPath, int limit);

  /**
   * Get list of jobs run for a given path and version.
   * <p>
   * Sorted in descending order by:
   * - START_TIME
   * - FINISH_TIME
   * - JOBID
   *
   * @param datasetPath Path of dataset
   * @param version     Version for dataset (or null for all versions)
   * @return The list of jobs.
   */
  Iterable<Job> getJobsForDataset(NamespaceKey datasetPath, DatasetVersion version, int limit);

  /**
   * Get list of jobs run for a given path and version and user
   *
   * @param datasetPath
   * @param version
   * @param user
   * @param limit
   * @return
   */
  Iterable<Job> getJobsForDataset(NamespaceKey datasetPath, DatasetVersion version, String user, int limit);

  /**
   * Get list of jobs that have the provided parent
   *
   * @param datasetPath the path of the parent
   * @return the corresponding jobs
   */
  Iterable<Job> getJobsForParent(NamespaceKey datasetPath, int limit);


  /**
   * Retrieve the query profile of jobId and attempt
   *
   * @param jobId
   * @param attempt attempt number
   * @return
   */
  QueryProfile getProfile(JobId jobId, int attempt) throws JobNotFoundException;

  /**
   * Cancel the provided jobId as the provided user.
   *
   * @param username The user causing the cancellation (to be used for security verification)
   * @param jobId    The job id to cancel
   * @param reason   Reason why job is cancelled
   * @return The outcome of the cancellation attempt. (Cancellation is asynchronous.)
   */
  void cancel(String username, JobId jobId, String reason) throws JobException;

  /**
   * Register a listener that listens for events associated with a particular job.
   * <p>
   * Throws exception if the requested JobId is not currently active.
   *
   * @param jobId    JobId to listen to.
   * @param listener The listener to be informed of job update events.
   */
  void registerListener(JobId jobId, ExternalStatusListener listener);
}


