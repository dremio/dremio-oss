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

import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.service.Service;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.CancelReflectionJobRequest;
import com.dremio.service.job.JobCounts;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobStats;
import com.dremio.service.job.JobStatsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsWithParentDatasetRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.ReflectionJobDetailsRequest;
import com.dremio.service.job.ReflectionJobProfileRequest;
import com.dremio.service.job.ReflectionJobSummaryRequest;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.SearchReflectionJobsRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;

/**
 * Job Service interface. Submit job, maintain job states
 */
public interface JobsService extends Service {

  /**
   * Submit a job to the execution engine. Generates a random externalId for the job
   *
   * @param jobRequest      job request
   * @param statusListener  a listener to notify for change of status. Must not be null
   * @return {@link JobId} of submitted job
   */
  JobId submitJob(SubmitJobRequest jobRequest, JobStatusListener statusListener);

  /**
   * Get details of a job.
   * @param jobDetailsRequest JobDetails Request
   * @return                  Job for given request
   * @throws JobNotFoundException if job is not found
   */
  JobDetails getJobDetails(JobDetailsRequest jobDetailsRequest) throws JobNotFoundException;

  /**
   * Get job summary for the job
   *
   * @param jobSummaryRequest JobSummaryRequest
   * @return              JobSummary for a given request
   * @throws JobNotFoundException if job is not found
   */
  JobSummary getJobSummary(JobSummaryRequest jobSummaryRequest) throws JobNotFoundException;

  /**
   * Get the number of jobs run for the given request.
   *
   * @param request job counts request
   * @return number of jobs run
   */
  JobCounts getJobCounts(JobCountsRequest request);

  /**
   * Get the number of jobs run sorted by job type given a date range.
   *
   * @param request job stats request
   * @return job stats
   */
  JobStats getJobStats(JobStatsRequest request);

  /**
   * Search jobs.
   *
   * @param request request
   * @return jobs that match
   */
  Iterable<JobSummary> searchJobs(SearchJobsRequest request);

  /**
   * Get list of jobs that have the provided parent
   *
   * @param jobsForParentRequest
   * @return the corresponding jobs
   */
  Iterable<JobDetails> getJobsForParent(JobsWithParentDatasetRequest jobsForParentRequest);


  /**
   * Retrieve the query profile of jobId and attempt
   *
   * @param queryProfileRequest request for QueryProfile
   * @return
   */
  QueryProfile getProfile(QueryProfileRequest queryProfileRequest) throws JobNotFoundException;

  /**
   * Cancel the provided jobId as the provided user.
   *
   * Cancellation is asynchronous.
   *
   * @param request cancellation request
   */
  void cancel(CancelJobRequest request) throws JobException;

  /**
   * Register a listener that listens for events associated with a particular job.
   * <p>
   * Throws exception if the requested JobId is not currently active.
   *
   * @param jobId    JobId to listen to.
   * @param listener The listener to be informed of job update events.
   */
  void registerListener(JobId jobId, ExternalStatusListener listener);

  /**
   * Get the Jobs client currently attached to this JobsService.
   * @return Jobs Client
   */
  JobsClient getJobsClient();

  /**
   * Get job summary for the job
   *
   * @param jobSummaryRequest JobSummaryRequest
   * @return              JobSummary for a given request
   * @throws JobNotFoundException if job is not found
   * @throws ReflectionJobValidationException if reflectionId doesn't belong to the job.
   */
  JobSummary getReflectionJobSummary(ReflectionJobSummaryRequest jobSummaryRequest) throws JobNotFoundException, ReflectionJobValidationException;


  /**
   * Get details of a job.
   * @param request ReflectionJobDetails Request
   * @return                  Job for given request
   * @throws JobNotFoundException if job is not found
   * @throws ReflectionJobValidationException if reflectionId doesn't belong to the job.
   */
  JobDetails getReflectionJobDetails(ReflectionJobDetailsRequest request) throws JobNotFoundException, ReflectionJobValidationException;


  /**
   * Cancel the provided jobId with reflectionId as the provided user.
   *
   * Cancellation is asynchronous.
   *
   * @param request cancellation request
   */
  void cancelReflectionJob(CancelReflectionJobRequest request) throws JobException;


  /**
   * Register a listener that listens for events associated with a particular job with given reflectionId.
   * <p>
   * Throws exception if the requested JobId is not currently active.
   *
   * @param jobId job to listen to
   * @param userName user issuing the request
   * @param reflectionId reflectionId associated with the job
   * @param listener The listener to be informed of job update events.
   */
  void registerReflectionJobListener(JobId jobId, String userName, String reflectionId, ExternalStatusListener listener);


  /**
   * Search jobs.
   *
   * @param request request
   * @return jobs that match
   */
  Iterable<JobSummary> searchReflectionJobs(SearchReflectionJobsRequest request);


  /**
   * Retrieve the query profile of jobId and attempt
   *
   * @param request for QueryProfile
   * @return
   */
  QueryProfile getReflectionJobProfile(ReflectionJobProfileRequest request) throws JobNotFoundException, ReflectionJobValidationException;
}


