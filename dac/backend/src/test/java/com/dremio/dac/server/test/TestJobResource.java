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
package com.dremio.dac.server.test;

import java.io.IOException;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.annotations.RestResourceUsedForTesting;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.resource.JobResource;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.JobResourceNotFoundException;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsService;

/**
 * Job test resource
 */
@RestResourceUsedForTesting
@Secured
@RolesAllowed({"admin", "user"})
@Path("/testjob/{jobId}")
public class TestJobResource extends JobResource {
  @Inject
  public TestJobResource(
    JobsService jobsService,
    DatasetVersionMutator datasetService,
    @Context SecurityContext securityContext,
    BufferAllocatorFactory allocatorFactory
  ) {
    super(jobsService, datasetService, securityContext, allocatorFactory);
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
  public Response download(
    @PathParam("jobId") JobId previewJobId,
    @QueryParam("downloadFormat") DownloadFormat downloadFormat
  ) throws JobResourceNotFoundException, JobNotFoundException {
    return doDownload(previewJobId, downloadFormat);
  }

  @Override
  protected long getDelay() {
    return 500L;
  }
}
