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
package com.dremio.dac.resource;

import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.resource.NotificationResponse.ResponseType;
import com.dremio.dac.service.datasets.DatasetDownloadManager;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.JobResourceNotFoundException;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.GetJobRequest;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobWarningException;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsServiceUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * Resource for getting single job summary/overview/details
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/job/{jobId}")
public class JobResource {
  private static final Logger logger = LoggerFactory.getLogger(JobResource.class);
  private static final Set<QueryType> JOB_TYPES_TO_DOWNLOAD = ImmutableSet.of(QueryType.UI_RUN,
    QueryType.UI_PREVIEW, QueryType.UI_INTERNAL_RUN, QueryType.UI_INTERNAL_PREVIEW, QueryType.UI_EXPORT);

  private final JobsService jobsService;
  private final DatasetVersionMutator datasetService;
  private final SecurityContext securityContext;

  @Inject
  public JobResource(
      JobsService jobsService,
      DatasetVersionMutator datasetService,
      @Context SecurityContext securityContext
      ) {
    this.jobsService = jobsService;
    this.datasetService = datasetService;
    this.securityContext = securityContext;
  }

  /**
   * Get job overview.
   * @param jobId job id
   */
  @GET
  @Produces(APPLICATION_JSON)
  public JobUI getJob(@PathParam("jobId") String jobId) throws JobResourceNotFoundException {
    try {
      GetJobRequest request = GetJobRequest.newBuilder()
        .setJobId(new JobId(jobId))
        .setUserName(securityContext.getUserPrincipal().getName())
        .build();
      return new JobUI(jobsService.getJob(request));
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }
  }

  @POST
  @Path("cancel")
  @Produces(APPLICATION_JSON)
  public NotificationResponse cancel(@PathParam("jobId") String jobId) throws JobResourceNotFoundException {
    try {
      final String username = securityContext.getUserPrincipal().getName();
      jobsService.cancel(username, new JobId(jobId), String.format("Query cancelled by user '%s'", username));
      return new NotificationResponse(ResponseType.OK, "Job cancellation requested");
    } catch(JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    } catch(JobWarningException e) {
      return new NotificationResponse(ResponseType.WARN, e.getMessage());
    } catch(JobException e) {
      return new NotificationResponse(ResponseType.ERROR, e.getMessage());
    }
  }

  // Get details of job
  @GET
  @Path("/details")
  @Produces(APPLICATION_JSON)
  public JobDetailsUI getJobDetail(@PathParam("jobId") String jobId) throws JobResourceNotFoundException {
    final Job job;
    try {
      GetJobRequest request = GetJobRequest.newBuilder()
        .setJobId(new JobId(jobId))
        .setUserName(securityContext.getUserPrincipal().getName())
        .build();
      job = jobsService.getJob(request);
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }

    return JobDetailsUI.of(job);
  }

  public static String getPaginationURL(JobId jobId) {
    return String.format("/job/%s/data", jobId.getId());
  }

  @GET
  @Path("/data")
  @Produces(APPLICATION_JSON)
  public JobDataFragment getDataForVersion(
      @PathParam("jobId") JobId jobId,
      @QueryParam("limit") int limit,
      @QueryParam("offset") int offset) throws JobResourceNotFoundException {

    Preconditions.checkArgument(limit > 0, "Limit should be greater than 0");
    Preconditions.checkArgument(offset >= 0, "Limit should be greater than or equal to 0");

    final Job job;
    try {
      GetJobRequest request = GetJobRequest.newBuilder()
        .setJobId(jobId)
        .setUserName(securityContext.getUserPrincipal().getName())
        .build();
      job = jobsService.getJob(request);
    } catch (JobNotFoundException e) {
      logger.warn("job not found: {}", jobId);
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }

    // job results in pagination requests.
    return new JobUI(job).getData().range(offset, limit);
  }

  @GET
  @Path("/r/{rowNum}/c/{columnName}")
  @Produces(APPLICATION_JSON)
  public Object getCellFullValue(
      @PathParam("jobId") JobId jobId,
      @PathParam("rowNum") int rowNum,
      @PathParam("columnName") String columnName) throws JobResourceNotFoundException {
    Preconditions.checkArgument(rowNum >= 0, "Row number shouldn't be negative");
    Preconditions.checkNotNull(columnName, "Expected a non-null column name");

    final Job job;
    try {
      GetJobRequest request = GetJobRequest.newBuilder()
        .setJobId(jobId)
        .setUserName(securityContext.getUserPrincipal().getName())
        .build();
      job = jobsService.getJob(request);
    } catch (JobNotFoundException e) {
      logger.warn("job not found: {}", jobId);
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }

    return new JobUI(job).getData().range(rowNum, 1).extractValue(columnName, 0);
  }

  public static String getDownloadURL(Job job) {
    final JobInfo jobInfo = job.getJobAttempt().getInfo();
    if (jobInfo.getQueryType() == QueryType.UI_EXPORT) {
      return format("/job/%s/download", job.getJobId().getId());
    }
    return null;
  }

  /**
   * Export data for job id as a file
   *
   * @param previewJobId
   * @param downloadFormat - a format of output file. Also defines a file extension
   * @return
   * @throws IOException
   * @throws JobResourceNotFoundException
   * @throws JobNotFoundException
   */
  @GET
  @Path("download")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response download(@PathParam("jobId") JobId previewJobId,
    @QueryParam("downloadFormat")
      DownloadFormat downloadFormat)
    throws IOException, JobResourceNotFoundException, JobNotFoundException {

    final String currentUser = securityContext.getUserPrincipal().getName();

    //first check that current user has access to preview data
    final GetJobRequest previewJobRequest = GetJobRequest.newBuilder()
      .setJobId(previewJobId)
      .setUserName(currentUser)
      .build();

    // ensure that we could access to the job.
    Job previewJob = jobsService.getJob(previewJobRequest);

    final JobInfo previewJobInfo = previewJob.getJobAttempt().getInfo();
    final List<String> datasetPath = previewJobInfo.getDatasetPathList();

    if (!JOB_TYPES_TO_DOWNLOAD.contains(previewJobInfo.getQueryType())) {
      logger.error("Not supported job type: {} for job '{}'. Supported job types are: {}",
        previewJobInfo.getQueryType(), previewJobId,
        String.join(", ", JOB_TYPES_TO_DOWNLOAD.stream()
          .map(queryType -> String.valueOf(queryType.getNumber()))
          .collect(Collectors.toList())));
      throw new IllegalArgumentException("Data for the job could not be downloaded");
    }

    final String outputFileName;
    // job id is already a download job. So just extract a filename from it
    if (previewJobInfo.getQueryType() == QueryType.UI_EXPORT) {
      outputFileName = previewJobInfo.getDownloadInfo().getFileName();
    } else {
      // must use DatasetDownloadManager.getDownloadFileName. If a naming convention is changed, the change should go to
      // DatasetDownloadManager.getDownloadFileName method.
      outputFileName = DatasetDownloadManager.getDownloadFileName(previewJobId, downloadFormat);
    }

    final StreamingOutput streamingOutput = new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException {
        try {
          JobsServiceUtil.waitForJobCompletion(CompletableFuture.completedFuture(previewJob));

          //read current job state
          final Job previewJob = jobsService.getJob(previewJobRequest);
          checkJobCompletionState(previewJob);


          Job downloadJob = previewJob;
          if (previewJobInfo.getQueryType() != QueryType.UI_EXPORT) {
            DatasetDownloadManager manager = datasetService.downloadManager();

            downloadJob = manager.scheduleDownload(datasetPath, previewJobInfo.getSql(),
              downloadFormat, previewJobInfo.getContextList(), -1 /* no limit */, currentUser, previewJobId);

            JobsServiceUtil.waitForJobCompletion(CompletableFuture.completedFuture(downloadJob));
            //get the final state of a job after completion
            downloadJob = jobsService.getJob(GetJobRequest.newBuilder()
              .setUserName(currentUser)
              .setJobId(downloadJob.getJobId())
              .build());
            checkJobCompletionState(downloadJob);
          }

          final DownloadDataResponse downloadDataResponse =
            datasetService.downloadData(downloadJob.getJobAttempt().getInfo().getDownloadInfo(), currentUser);

          IOUtils.copyBytes(downloadDataResponse.getInput(), output, 4096, true);
        } catch (JobNotFoundException e) {
          throw new WebApplicationException(e);
        }
      }
    };

    return Response.ok(streamingOutput, MediaType.APPLICATION_OCTET_STREAM)
      .header("Content-Disposition", "attachment; filename=\"" + outputFileName + "\"").build();

  }

  private static void checkJobCompletionState(final Job job) throws ForbiddenException {
    final JobState jobState = job.getJobAttempt().getState();
    final JobInfo jobInfo = job.getJobAttempt().getInfo();

    // requester should check that job state is COMPLETED before downloading, but return nicely
    switch (jobState) {
      case COMPLETED:
        // job is completed. Do nothing
        break;
      case FAILED:
        throw new ForbiddenException(jobInfo.getFailureInfo());
      case CANCELED:
        if (jobInfo.getCancellationInfo() != null) {
          throw new ForbiddenException(jobInfo.getCancellationInfo().getMessage());
        }
        throw new ForbiddenException("Download job was canceled, but further information is unavailable");
      default:
        throw new ForbiddenException(format("Could not download results (job: %s, state: %s)", job.getJobId(), jobState));
    }
  }
}
