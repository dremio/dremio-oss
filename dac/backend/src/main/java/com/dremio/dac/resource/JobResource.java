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

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.annotations.TemporaryAccess;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.resource.NotificationResponse.ResponseType;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.datasets.DatasetDownloadManager;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.InvalidReflectionJobException;
import com.dremio.dac.service.errors.JobResourceNotFoundException;
import com.dremio.dac.util.DownloadUtil;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.CancelReflectionJobRequest;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.ReflectionJobDetailsRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobWarningException;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.ReflectionJobValidationException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Resource for getting single job summary/overview/details
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/job/{jobId}")
public class JobResource extends BaseResourceWithAllocator {
  private static final Logger logger = LoggerFactory.getLogger(JobResource.class);

  private final JobsService jobsService;
  private final DatasetVersionMutator datasetService;
  private final SecurityContext securityContext;

  @Inject
  public JobResource(
    JobsService jobsService,
    DatasetVersionMutator datasetService,
    @Context SecurityContext securityContext,
    BufferAllocatorFactory allocatorFactory
  ) {
    super(allocatorFactory);
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
    return new JobUI(jobsService, new JobId(jobId), securityContext.getUserPrincipal().getName());
  }

  @POST
  @Path("cancel")
  @Produces(APPLICATION_JSON)
  public NotificationResponse cancel(@PathParam("jobId") String jobId) throws JobResourceNotFoundException {
    try {
      final String username = securityContext.getUserPrincipal().getName();
      jobsService.cancel(CancelJobRequest.newBuilder()
          .setUsername(username)
          .setJobId(JobsProtoUtil.toBuf(new JobId(jobId)))
          .setReason(String.format("Query cancelled by user '%s'", username))
          .build());
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
    final JobDetails jobDetails;
    try {
      JobDetailsRequest request = JobDetailsRequest.newBuilder()
          .setJobId(JobProtobuf.JobId.newBuilder().setId(jobId).build())
          .setUserName(securityContext.getUserPrincipal().getName())
          .setProvideResultInfo(true)
          .build();
      jobDetails = jobsService.getJobDetails(request);
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }

    return JobDetailsUI.of(jobDetails);
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

    try {
      jobsService.getJobSummary(JobSummaryRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(jobId))
        .setUserName(securityContext.getUserPrincipal().getName())
        .build());
    } catch (JobNotFoundException e) {
      logger.warn("job not found: {}", jobId);
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }

    JobDataClientUtils.waitForFinalState(jobsService, jobId);
    // job results in pagination requests.
    return new JobDataWrapper(jobsService, jobId, securityContext.getUserPrincipal().getName())
      .range(getOrCreateAllocator("getDataForVersion"), offset, limit);
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

    JobDataClientUtils.waitForFinalState(jobsService, jobId);
    try (final JobDataFragment dataFragment = new JobDataWrapper(jobsService, jobId, securityContext.getUserPrincipal().getName())
      .range(getOrCreateAllocator("getCellFullValue"), rowNum, 1)) {

      return dataFragment.extractValue(columnName, 0);
    }
  }

  public static String getDownloadURL(JobDetails jobDetails) {
    if (JobsProtoUtil.getLastAttempt(jobDetails).getInfo().getQueryType() == QueryType.UI_EXPORT) {
      return format("/job/%s/download", jobDetails.getJobId().getId());
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
  @TemporaryAccess
  public Response download(
    @PathParam("jobId") JobId previewJobId,
    @QueryParam("downloadFormat") DownloadFormat downloadFormat
  ) throws JobResourceNotFoundException, JobNotFoundException {
    return doDownload(previewJobId, downloadFormat);
  }

  // Get details of reflection job
  @GET
  @Path("/reflection/{reflectionId}/details")
  @Produces(APPLICATION_JSON)
  public JobDetailsUI getReflectionJobDetail(@PathParam("jobId") String jobId,
                                             @PathParam("reflectionId") String reflectionId) throws JobResourceNotFoundException {
    final JobDetails jobDetails;

    if (Strings.isNullOrEmpty(reflectionId)) {
      throw UserException.validationError()
        .message("reflectionId cannot be null or empty")
        .build();
    }

    try {
      JobDetailsRequest.Builder jobDetailsRequestBuilder = JobDetailsRequest.newBuilder()
        .setJobId(com.dremio.service.job.proto.JobProtobuf.JobId.newBuilder().setId(jobId).build())
        .setProvideResultInfo(true)
        .setUserName(securityContext.getUserPrincipal().getName());

      ReflectionJobDetailsRequest request = ReflectionJobDetailsRequest.newBuilder()
        .setJobDetailsRequest(jobDetailsRequestBuilder.build())
        .setReflectionId(reflectionId)
        .build();

      jobDetails = jobsService.getReflectionJobDetails(request);
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    } catch (ReflectionJobValidationException e) {
      throw new InvalidReflectionJobException(e.getJobId().getId(), e.getReflectionId());
    }

    return JobDetailsUI.of(jobDetails);
  }

  @POST
  @Path("/reflection/{reflectionId}/cancel")
  @Produces(APPLICATION_JSON)
  public NotificationResponse cancelReflectionJob(@PathParam("jobId") String jobId,
                                                  @PathParam("reflectionId") String reflectionId) throws JobResourceNotFoundException {
    if (Strings.isNullOrEmpty(reflectionId)) {
      throw UserException.validationError()
        .message("reflectionId cannot be null or empty")
        .build();
    }

    try {
      final String username = securityContext.getUserPrincipal().getName();


      CancelJobRequest cancelJobRequest = CancelJobRequest.newBuilder()
        .setUsername(username)
        .setJobId(JobsProtoUtil.toBuf(new JobId(jobId)))
        .setReason(String.format("Query cancelled by user '%s'", username))
        .build();

      CancelReflectionJobRequest cancelReflectionJobRequest = CancelReflectionJobRequest.newBuilder()
        .setCancelJobRequest(cancelJobRequest)
        .setReflectionId(reflectionId)
        .build();

      jobsService.cancelReflectionJob(cancelReflectionJobRequest);
      return new NotificationResponse(ResponseType.OK, "Job cancellation requested");
    } catch(JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    } catch (ReflectionJobValidationException e) {
      throw new InvalidReflectionJobException(e.getJobId().getId(), e.getReflectionId());
    } catch(JobWarningException e) {
      return new NotificationResponse(ResponseType.WARN, e.getMessage());
    } catch(JobException e) {
      return new NotificationResponse(ResponseType.ERROR, e.getMessage());
    }
  }

  protected Response doDownload(JobId previewJobId, DownloadFormat downloadFormat) throws JobResourceNotFoundException, JobNotFoundException {
    final String currentUser = securityContext.getUserPrincipal().getName();

    //first check that current user has access to preview data
    final JobDetailsRequest previewJobRequest = JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(previewJobId))
      .setUserName(currentUser)
      .build();

    final JobDetails jobDetails = jobsService.getJobDetails(previewJobRequest);

    final DownloadUtil downloadUtil = new DownloadUtil(jobsService, datasetService);
    final ChunkedOutput<byte[]> output = downloadUtil.startChunckedDownload(previewJobId, currentUser, downloadFormat, getDelay());

    final String contentType;
    if (downloadFormat != null) {
      switch (downloadFormat) {
        case JSON:
          contentType = APPLICATION_JSON;
          break;
        case CSV:
          contentType = "text/csv";
          break;
        default:
          contentType = MediaType.APPLICATION_OCTET_STREAM;
          break;
      }
    } else {
      contentType = MediaType.APPLICATION_OCTET_STREAM;
    }

    final JobInfo info = JobsProtoUtil.getLastAttempt(jobDetails).getInfo();

    final String outputFileName;
    // job id is already a download job. So just extract a filename from it
    if (info.getQueryType() == QueryType.UI_EXPORT) {
      outputFileName = info.getDownloadInfo().getFileName();
    } else {
      // must use DatasetDownloadManager.getDownloadFileName. If a naming convention is changed, the change should go to
      // DatasetDownloadManager.getDownloadFileName method.
      outputFileName = DatasetDownloadManager.getDownloadFileName(previewJobId, downloadFormat);
    }

    return Response.ok(output, contentType)
      .header("Content-Disposition", "attachment; filename=\"" + outputFileName + "\"")
      // stops the browser from trying to determine the type of the file based on the content.
      .header( "X-Content-Type-Options", "nosniff")
      .build();
  }

  protected long getDelay() {
    return 0L;
  }
}
