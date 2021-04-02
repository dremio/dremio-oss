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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.server.ChunkedOutput;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.admin.KVStoreReportService;
import com.dremio.dac.service.admin.KVStoreReportService.KVStoreNotSupportedException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;


/**
 * Resource for generating a report of splits entries in kvstore.
 */
@RestResource
@Secured
@RolesAllowed("admin")
@Path("/kvstore")
public class KVStoreReportResource {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KVStoreReportResource.class);
  private final KVStoreReportService kvStoreReportService;
  private ListeningExecutorService executorService;

  @Inject
  public KVStoreReportResource(
    KVStoreReportService kvStoreReportService,
    Provider<ExecutorService> executorServiceProvider
  ) {
    this.kvStoreReportService = kvStoreReportService;
    this.executorService = MoreExecutors.listeningDecorator(executorServiceProvider.get());
  }

  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/report")
  public Response downloadReport(@QueryParam("store") List<String> storeNames) {
    try {
      final ChunkedOutput<byte[]> output = doDownload(storeNames);
      String outputFilename = String.format("kvstore-report_%d.zip", System.currentTimeMillis());

      return Response.ok(output, MediaType.APPLICATION_OCTET_STREAM)
        .header("Content-Disposition", "attachment; filename=\""
          + outputFilename + "\"")
        .header("X-Content-Type-Options", "nosniff").build();
    } catch (KVStoreNotSupportedException e) {
      return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
    } catch (IOException e) {
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
    }
  }

  protected ChunkedOutput<byte[]> doDownload(List<String> storeNames) throws IOException, KVStoreNotSupportedException {
    final ChunkedOutput<byte[]> output = new ChunkedOutput<>(byte[].class);
    BufferedInputStream pipeIs = new BufferedInputStream(kvStoreReportService.getSplitReport(storeNames));

    ListenableFuture<Void> future = executorService.submit(() -> {
      try (ChunkedOutput<byte[]> toClose = output; BufferedInputStream toClose2 = pipeIs) {
        byte[] buf = new byte[KVStoreReportService.BUFFER_SIZE];
        int len;
        while ((len = pipeIs.read(buf)) > -1) {
          if (len < KVStoreReportService.BUFFER_SIZE) {
            output.write(Arrays.copyOf(buf, len));
          } else {
            output.write(buf);
          }
        }
      } catch (IOException e) {
        logger.error("Failed to write to output.", e);
      }
      return null;
    });
    return output;
  }

}
