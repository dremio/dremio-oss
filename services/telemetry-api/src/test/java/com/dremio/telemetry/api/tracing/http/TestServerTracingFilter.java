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
package com.dremio.telemetry.api.tracing.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.jupiter.api.Test;

public class TestServerTracingFilter extends JerseyTest {

  @Path("/dremio")
  public static class Dremio {

    @GET
    @Path("/source_code")
    public String getSourceCode() {
      return "source_code";
    }

    @GET
    @Path("/source_code_header")
    public Response getSourceCodeWithHeader() {
      return Response.ok()
          .header(ServerTracingFilter.REQUEST_ID_HEADER, "sample")
          .entity("test")
          .build();
    }
  }

  @Override
  protected Application configure() {
    InMemorySpanExporter spanExporter = InMemorySpanExporter.create();

    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();

    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder()
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .setTracerProvider(tracerProvider)
            .build();

    GlobalOpenTelemetry.resetForTest();
    GlobalOpenTelemetry.set(openTelemetry);

    return new ResourceConfig(Dremio.class).register(ServerTracingFilter.class);
  }

  @Test
  public void shouldPropagateRequestIdFromRequestToResponse() throws IOException {
    Response response =
        target("/dremio/source_code")
            .request()
            .header(ServerTracingFilter.REQUEST_ID_HEADER, "test")
            .get();

    assertEquals(response.getHeaderString(ServerTracingFilter.REQUEST_ID_HEADER), "test");
  }

  @Test
  public void shouldIgnoreRequestHeaderIfResponseAlreadyContainsIt() throws IOException {
    Response response =
        target("/dremio/source_code_header")
            .request()
            .header(ServerTracingFilter.REQUEST_ID_HEADER, "override")
            .get();

    assertEquals(response.getHeaderString(ServerTracingFilter.REQUEST_ID_HEADER), "sample");
  }

  @Test
  public void shouldCreateRequestIdIfNotExistsOnRequest() {
    Response response = target("/dremio/source_code").request().get();

    String customRequestId = response.getHeaderString(ServerTracingFilter.REQUEST_ID_HEADER);
    assertTrue(StringUtils.isNotBlank(customRequestId));
  }
}
