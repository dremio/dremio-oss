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

import java.util.Collections;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;

import org.apache.http.HttpHeaders;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.dac.resource.TemporaryTokenResource.TempTokenResponse;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.users.SystemUser;

/**
 * Tests for TemporaryTokenResource and temporary token validation
 */
public class TestTemporaryTokenResource extends BaseTestServer {
  @Rule
  public final ExpectedException thrown = ExpectedException.none();
  private JobId jobId;

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();
  }

  @Before
  public void setup() {
    final SqlQuery query = new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME);

    jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder().setSqlQuery(query).setQueryType(QueryType.UI_RUN).build()
    );
  }

  @Test
  public void testTempTokenCreation() {
    Response tokenResponse = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 30)
      .queryParam("request", "/apiv2/test/?query=random"))
      .buildPost(null).invoke();
    assertEquals("Should pass.", 200, tokenResponse.getStatus());

    tokenResponse = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", -1)
      .queryParam("request", "/apiv2/test/?query=random"))
      .buildPost(null).invoke();
    assertEquals("Duration cannot be negative.", 400, tokenResponse.getStatus());

    tokenResponse = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("request", "/apiv2/test/?query=random"))
      .buildPost(null).invoke();
    assertEquals("Missing duration.", 400, tokenResponse.getStatus());

    tokenResponse = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 30))
      .buildPost(null).invoke();
    assertEquals("Missing request url.", 400, tokenResponse.getStatus());

    tokenResponse = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 30)
      .queryParam("request", "mailto:a@b.com"))
      .buildPost(null).invoke();
    assertEquals("Invalid request.", 400, tokenResponse.getStatus());
  }

  @Test
  public void testTempTokenValidation() {
    final Response tokenResponse = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 180)
      .queryParam("request", String.format("/apiv2/testjob/%s/download/?downloadFormat=%s", jobId.getId(), "JSON")))
      .buildPost(null).invoke();
    final String token = tokenResponse.readEntity(TempTokenResponse.class).getToken();

    // download should succeed with temp token in query param
    Invocation invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam(".token", token)
        .queryParam("downloadFormat", "JSON"))
      .header(HttpHeaders.AUTHORIZATION, null) // remove old session auth header
      .buildGet();
    Response response = invocation.invoke();
    assertEquals("Should pass.", 200, response.getStatus());
    assertEquals("Invalid referrer policy",
        "strict-origin-when-cross-origin,no-referrer", response.getHeaderString("Referrer-Policy"));

    // download should succeed with temp token in auth header
    invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam("downloadFormat", "JSON"))
      .header(HttpHeaders.AUTHORIZATION, null)
      .header(HttpHeaders.AUTHORIZATION, token)
      .buildGet();
    response = invocation.invoke();
    assertEquals("Should pass.", 200, response.getStatus());
    assertEquals("Invalid referrer policy",
        "strict-origin-when-cross-origin,no-referrer", response.getHeaderString("Referrer-Policy"));

    // download should fail with regular token in query param
    invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam("downloadFormat", "JSON")
        .queryParam(".token", getAuthHeaderValue()))
      .header(HttpHeaders.AUTHORIZATION, null)
      .buildGet();
    response = invocation.invoke();
    assertEquals("Temporary access resource should fail with regular token in query param", 401, response.getStatus());

    // missing temp token and auth header
    invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam("downloadFormat", "JSON"))
      .header(HttpHeaders.AUTHORIZATION, null)
      .buildGet();
    response = invocation.invoke();
    assertEquals("Missing temp token: Unauthorized.", 401, response.getStatus());
    assertEquals("Invalid referrer policy",
        "strict-origin-when-cross-origin,no-referrer", response.getHeaderString("Referrer-Policy"));

    // invalid temp token
    invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam(".token", "invalid-token")
        .queryParam("downloadFormat", "JSON"))
      .header(HttpHeaders.AUTHORIZATION, null)
      .buildGet();
    response = invocation.invoke();
    assertEquals("Invalid temp token: Unauthorized.", 401, response.getStatus());
    assertEquals("Invalid referrer policy",
        "strict-origin-when-cross-origin,no-referrer", response.getHeaderString("Referrer-Policy"));

  }

  @Test
  public void testRequestUrlMatch() {
    // slash at the end of the request url path
    final Response tokenResponse1 = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 30)
      .queryParam("request", String.format("apiv2/testjob/%s/download//?downloadFormat=%s", jobId.getId(), "JSON")))
      .buildPost(null).invoke();
    final String token1 = tokenResponse1.readEntity(TempTokenResponse.class).getToken();

    final Invocation invocation1 = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam(".token", token1)
        .queryParam("downloadFormat", "JSON"))
      .header(HttpHeaders.AUTHORIZATION, null) // remove old session auth header
      .buildGet();
    final Response response1 = invocation1.invoke();
    assertEquals("Paths match.", 200, response1.getStatus());

    // slash at the beginning of the request url path
    final Response tokenResponse2 = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 30)
      .queryParam("request", String.format("///apiv2/testjob/%s/download?downloadFormat=%s", jobId.getId(), "JSON")))
      .buildPost(null).invoke();
    final String token2 = tokenResponse2.readEntity(TempTokenResponse.class).getToken();

    final Invocation invocation2 = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam(".token", token2)
        .queryParam("downloadFormat", "JSON"))
      .header(HttpHeaders.AUTHORIZATION, null) // remove old session auth header
      .buildGet();
    final Response response2 = invocation2.invoke();
    assertEquals("Paths match.", 200, response2.getStatus());

    // multiple query params
    final Response tokenResponse3 = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 30)
      .queryParam("request", String.format("/apiv2/testjob/%s/download/?downloadFormat=%s&a=1&b=2&a=3", jobId.getId(), "JSON")))
      .buildPost(null).invoke();
    final String token3 = tokenResponse3.readEntity(TempTokenResponse.class).getToken();

    final Invocation invocation3 = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam(".token", token3)
        .queryParam("b", "2")
        .queryParam("a", "3")
        .queryParam("a", "1")
        .queryParam("downloadFormat", "JSON"))
      .header(HttpHeaders.AUTHORIZATION, null) // remove old session auth header
      .buildGet();
    final Response response3 = invocation3.invoke();
    assertEquals("Query params match.", 200, response3.getStatus());
  }

  @Test
  public void testRequestUrlNotMatch() {
    final Response tokenResponse = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 180)
      .queryParam("request", String.format("/apiv2/testjob/%s/download?downloadFormat=%s", jobId.getId(), "JSON")))
      .buildPost(null).invoke();
    final String token = tokenResponse.readEntity(TempTokenResponse.class).getToken();

    // query params not match
    Invocation invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam(".token", token)
        .queryParam("downloadFormat", "PARQUET"))
      .header(HttpHeaders.AUTHORIZATION, null) // remove old session auth header
      .buildGet();
    Response response = invocation.invoke();
    assertEquals("Query params do not match.", 401, response.getStatus());

    // missing query param
    invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam(".token", token))
      .header(HttpHeaders.AUTHORIZATION, null)
      .buildGet();
    response = invocation.invoke();
    assertEquals("Missing query param.", 401, response.getStatus());

    // path not match
    final SqlQuery query = new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME);

    JobId diffJobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder().setSqlQuery(query).setQueryType(QueryType.UI_RUN).build()
    );

    invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(diffJobId.getId())
        .path("download")
        .queryParam(".token", token))
      .header(HttpHeaders.AUTHORIZATION, null)
      .buildGet();
    response = invocation.invoke();
    assertEquals("Request paths do not match.", 401, response.getStatus());

    // extra query param
    final Response tokenResponse2 = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 30)
      .queryParam("request", String.format("/apiv2/testjob/%s/download", jobId.getId())))
      .buildPost(null).invoke();
    final String token2 = tokenResponse2.readEntity(TempTokenResponse.class).getToken();

    final Invocation invocation2 = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("download")
        .queryParam(".token", token2)
        .queryParam("downloadFormat", "JSON"))
      .header(HttpHeaders.AUTHORIZATION, null) // remove old session auth header
      .buildGet();
    final Response response2 = invocation2.invoke();
    assertEquals("Extra query param is not allowed.", 401, response2.getStatus());
  }

  @Test
  public void testRegularAPIUsingTempToken() {
    // non temporary access method use a temporary token
    final Response tokenResponse = getBuilder(getAPIv2()
      .path("temp-token")
      .queryParam("durationSeconds", 30)
      .queryParam("request", String.format("/apiv2/testjob/%s/test?", jobId.getId())))
      .buildPost(null).invoke();
    final String token = tokenResponse.readEntity(TempTokenResponse.class).getToken();

    // temp token in query param
    Invocation invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("test")
        .queryParam(".token", token))
      .header(HttpHeaders.AUTHORIZATION, null) // remove old session auth header
      .buildGet();
    Response response = invocation.invoke();
    assertEquals("Not allowed to use a temporary token for a non temporary access method.", 401, response.getStatus());

    // session auth token in query param
    invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("test")
        .queryParam(".token", getAuthHeaderValue())) // place session auth token in query param
      .header(HttpHeaders.AUTHORIZATION, null) // remove session auth header
      .buildGet();
    response = invocation.invoke();
    assertEquals("Not allowed to provide a regular token as a query param for a non temporary access method or resource.", 401, response.getStatus());

    // temp token in auth header
    invocation = getBuilder(
      getAPIv2()
        .path("testjob")
        .path(jobId.getId())
        .path("test"))
      .header(HttpHeaders.AUTHORIZATION, null) // remove old session auth header
      .header(HttpHeaders.AUTHORIZATION, token)
      .buildGet();
    response = invocation.invoke();
    assertEquals("Not allowed to use a temporary token for a non temporary access method.", 401, response.getStatus());
  }
}
