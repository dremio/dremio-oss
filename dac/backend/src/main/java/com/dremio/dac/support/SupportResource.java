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
package com.dremio.dac.support;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;

import org.apache.hadoop.io.IOUtils;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.dac.service.errors.JobResourceNotFoundException;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.users.UserNotFoundException;

/**
 * Resource for uploading support information to Dremio.
 */
@RestResource
@Secured
@RolesAllowed({ "admin", "user" })
@Path("/support/{jobId}")
public class SupportResource {

  private final SupportService supportService;
  private final SecurityContext context;

  @Inject
  public SupportResource(SupportService supportService, @Context SecurityContext context) {
    super();
    this.supportService = supportService;
    this.context = context;
  }

  @POST
  @Produces(APPLICATION_JSON)
  public SupportResponse submit(@PathParam("jobId") JobId jobId) throws IOException, UserNotFoundException {
    return supportService.uploadSupportRequest(context.getUserPrincipal().getName(), jobId);
  }

  @POST
  @Path("download")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response downloadData(@PathParam("jobId") JobId jobId)
      throws IOException, UserNotFoundException, JobResourceNotFoundException {
    final DownloadDataResponse response;
    try {
      response = supportService.downloadSupportRequest(context.getUserPrincipal().getName(), jobId);
    } catch (JobNotFoundException e) {
      throw JobResourceNotFoundException.fromJobNotFoundException(e);
    }
    final StreamingOutput streamingOutput = new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException {
        IOUtils.copyBytes(response.getInput(), output, 4096, true);
      }
    };
    return Response.ok(streamingOutput, MediaType.APPLICATION_OCTET_STREAM)
      .header("Content-Disposition", "attachment; filename=\"" + response.getFileName() + "\"").build();
  }
}
