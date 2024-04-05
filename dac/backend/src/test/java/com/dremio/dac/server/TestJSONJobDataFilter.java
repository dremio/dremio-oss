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
package com.dremio.dac.server;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.dremio.dac.explore.model.DataJsonOutput;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterModifier;
import com.fasterxml.jackson.jaxrs.json.JsonEndpointConfig;
import java.io.IOException;
import java.util.Collection;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.test.util.server.ContainerRequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Unit tests for {@link JSONJobDataFilter} */
@RunWith(Parameterized.class)
public class TestJSONJobDataFilter {
  @Before
  public void setUp() {
    ObjectWriterInjector.getAndClear();
  }

  @Parameterized.Parameters(
      name = "Request header {0}, response media type = {1}, expected value = {2}")
  public static Collection<TestData> data() {
    return asList(
        new TestData(
            WebServer.X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS,
            WebServer.X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_SUPPORTED_VALUE,
            MediaType.APPLICATION_JSON_TYPE,
            true),
        new TestData(
            WebServer.X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS,
            "not supported header value",
            MediaType.APPLICATION_JSON_TYPE,
            false),
        new TestData(null, null, MediaType.APPLICATION_JSON_TYPE, false),
        new TestData(
            WebServer.X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS,
            WebServer.X_DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_SUPPORTED_VALUE,
            MediaType.MULTIPART_FORM_DATA_TYPE,
            false),
        new TestData(
            WebServer.X_DREMIO_HOSTNAME,
            "not supported header value",
            MediaType.MULTIPART_FORM_DATA_TYPE,
            false));
  }

  private static final class TestData {

    private final String headerKey;
    private final String headerValue;
    private final MediaType mediaType;
    private final boolean expectedToBeTouched;

    public TestData(String headerKey, String headerValue, MediaType mediaType, boolean isTouched) {
      this.headerKey = headerKey;
      this.headerValue = headerValue;
      this.mediaType = mediaType;
      this.expectedToBeTouched = isTouched;
    }

    public String getHeaderKey() {
      return headerKey;
    }

    public String getHeaderValue() {
      return headerValue;
    }

    public MediaType getMediaType() {
      return mediaType;
    }

    public boolean isExpectedToBeTouched() {
      return expectedToBeTouched;
    }
  }

  private final TestData testData;

  public TestJSONJobDataFilter(final TestData testData) {
    this.testData = testData;
  }

  @Test
  public void testJSONGeneratorUntouched() throws IOException {
    JSONJobDataFilter filter = new JSONJobDataFilter();
    ContainerRequestBuilder request =
        ContainerRequestBuilder.from("http://localhost/foo/bar", "GET", null);
    if (testData.getHeaderKey() != null) {
      request.header(testData.getHeaderKey(), testData.getHeaderValue());
    }

    ContainerResponse response =
        new ContainerResponse(request.build(), Response.ok().type(testData.getMediaType()).build());
    filter.filter(request.build(), response);

    ObjectWriterModifier modifier = ObjectWriterInjector.get();
    if (testData.isExpectedToBeTouched()) {
      assertNotNull(modifier);

      ObjectWriter writer = mock(ObjectWriter.class);
      modifier.modify(
          mock(JsonEndpointConfig.class),
          new MultivaluedHashMap<String, Object>(),
          new Object(),
          writer,
          mock(JsonGenerator.class));
      verify(writer)
          .withAttribute(DataJsonOutput.DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_ATTRIBUTE, true);
    } else {
      assertNull(modifier);
    }
  }
}
