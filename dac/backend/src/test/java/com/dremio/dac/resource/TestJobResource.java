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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ChunkedInput;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.users.SystemUser;

/**
 * Tests for JobsResource
 */
public class TestJobResource extends BaseTestServer {
  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();
  }

  @Test
  public void testDownloadSendsHeadersBeforeContent() {
    final SqlQuery query = new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME);

    final JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder().setSqlQuery(query).setQueryType(QueryType.UI_RUN).build()
    );

    final Invocation invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam("downloadFormat", "JSON"))
      .buildGet();

    final Response response = invocation.invoke();

    assertTrue(response.getHeaderString("Content-Disposition").startsWith("attachment;"));
    assertEquals("nosniff", response.getHeaderString("X-Content-Type-Options"));
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());

    final ChunkedInput<String> chunks = response.readEntity(new GenericType<ChunkedInput<String>>(){});
    String chunk;
    String readChunk = null;
    while ((chunk = chunks.read()) != null) {
      readChunk = chunk;
    }

    assertNotNull(readChunk);
  }
}
