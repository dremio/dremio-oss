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
package com.dremio.dac.resource;

import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
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
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.resource.NotificationResponse.ResponseType;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.JobResourceNotFoundException;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobWarningException;
import com.dremio.service.jobs.JobsService;
import com.google.common.base.Preconditions;

/**
 * Resource for getting single job summary/overview/details
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/job/{jobId}")
public class JobResource {
  private static final Logger logger = LoggerFactory.getLogger(JobResource.class);

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
      return new JobUI(jobsService.getJobChecked(new JobId(jobId)));
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }
  }

  @POST
  @Path("cancel")
  @Produces(APPLICATION_JSON)
  public NotificationResponse cancel(@PathParam("jobId") String jobId) throws JobResourceNotFoundException {
    try {
      jobsService.cancel(securityContext.getUserPrincipal().getName(), new JobId(jobId));
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
    JobId id = new JobId(jobId);
    Job job = jobsService.getJob(id);
    if(job != null){
      return new JobDetailsUI(job);
    }else{
      throw new JobResourceNotFoundException(id);
    }
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

    final Job job =  jobsService.getJob(jobId);
    if (job == null) {
      logger.warn("job not found: {}", jobId);
      throw new JobResourceNotFoundException(jobId);
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

    final Job job =  jobsService.getJob(jobId);
    if (job == null) {
      logger.warn("job not found: {}", jobId);
      throw new JobResourceNotFoundException(jobId);
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

  @GET
  @Path("download")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response downloadData(@PathParam("jobId") JobId jobId) throws IOException, JobResourceNotFoundException {
    final Job job = jobsService.getJob(jobId);
    final JobInfo jobInfo = job.getJobAttempt().getInfo();

    if (jobInfo.getQueryType() == QueryType.UI_EXPORT) {
      final DownloadDataResponse downloadDataResponse = datasetService.downloadData(jobInfo.getDownloadInfo(), securityContext.getUserPrincipal().getName());
      final StreamingOutput streamingOutput = new StreamingOutput() {
        @Override
        public void write(OutputStream output) throws IOException, WebApplicationException {
          IOUtils.copyBytes(downloadDataResponse.getInput(), output, 4096, true);
        }
      };
      return Response.ok(streamingOutput, MediaType.APPLICATION_OCTET_STREAM)
        .header("Content-Disposition", "attachment; filename=\"" + downloadDataResponse.getFileName() + "\"").build();
    } else {
      throw new JobResourceNotFoundException(jobId, format("Job %s has no data that can not be downloaded, invalid type %s", jobId, jobInfo.getQueryType()));
    }
  }
}
