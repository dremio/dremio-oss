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

import static com.dremio.dac.support.QueryLogBundleService.BUFFER_SIZE;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.jersey.server.ChunkedOutput;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.annotations.TemporaryAccess;
import com.dremio.provision.service.ProvisioningHandlingException;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.users.UserNotFoundException;

/**
 * Resource for downloading query log support bundle.
 */
@APIResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/support-bundle/{jobId}")
public class QueryLogBundleResource {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryLogBundleResource.class);
  private final QueryLogBundleService queryLogBundleService;
  private final SecurityContext context;
  private static final ExecutorService executorService = Executors.newFixedThreadPool(
    1,
    r -> new Thread(r, "support-bundle-deliver"));

  @Inject
  public QueryLogBundleResource(QueryLogBundleService queryLogBundleService,  @Context SecurityContext context) {
    this.queryLogBundleService = queryLogBundleService;
    this.context = context;
  }

  /**
   * Download logs relevant to the query in the cluster.
   * Currently only support YARN cluster.
   * @return Response
   */
  @GET
  @Path("/download")
  @Consumes(MediaType.APPLICATION_JSON)
  @TemporaryAccess
  public Response getClusterLog(@PathParam("jobId") JobId jobId) {

    try {
      queryLogBundleService.validateUser(context.getUserPrincipal().getName());
      final ChunkedOutput<byte[]> output = doDownload(jobId.getId(), context.getUserPrincipal().getName());
      String outputFilename = "query_bundle_" + jobId.getId() + ".tar.gz";

      return Response.ok(output, MediaType.APPLICATION_OCTET_STREAM)
        .header("Content-Disposition", "attachment; filename=\""
          + outputFilename + "\"")
        .header("X-Content-Type-Options", "nosniff").build();
    } catch (UserNotFoundException | JobNotFoundException | ProvisioningHandlingException e) {
      return Response.status(Status.FORBIDDEN).entity(e.getLocalizedMessage()).build();
    } catch (NotSupportedException e) {
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(e.getLocalizedMessage()).build();
    } catch (IOException e) {
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getLocalizedMessage()).build();
    }
  }

  protected ChunkedOutput<byte[]> doDownload(String jobId, String userName)
    throws UserNotFoundException, JobNotFoundException, IOException, ProvisioningHandlingException, NotSupportedException {

    final ChunkedOutput<byte[]> output = new ChunkedOutput<>(byte[].class);
    BufferedInputStream pipeIs = new BufferedInputStream(queryLogBundleService.getClusterLog(jobId, userName));

    executorService.execute(() -> {
      try (ChunkedOutput toClose = output; BufferedInputStream toClose2 = pipeIs) {
        byte[] buf = new byte[BUFFER_SIZE];
        int len;
        while ((len = pipeIs.read(buf)) > -1) {
          if (len < BUFFER_SIZE) {
            output.write(Arrays.copyOf(buf, len));
          } else {
            output.write(buf);
          }
        }
      } catch (IOException e) {
        logger.error("Failed to write to output.", e);
      }
    });

    return output;
  }
}
