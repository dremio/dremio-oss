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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterModifier;
import com.fasterxml.jackson.jaxrs.json.JsonEndpointConfig;
import java.io.IOException;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.test.util.server.ContainerRequestBuilder;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link JSONPrettyPrintFilter} */
public class TestJSONPrettyPrintFilter {

  @Before
  public void setUp() {
    ObjectWriterInjector.getAndClear();
  }

  @Test
  public void testJSONGeneratorConfigured() throws IOException {
    JSONPrettyPrintFilter filter = new JSONPrettyPrintFilter();
    ContainerRequest request =
        ContainerRequestBuilder.from("http://localhost/foo/bar?pretty", "GET", null)
            .accept("random/media")
            .build();
    ContainerResponse response = new ContainerResponse(request, Response.ok().build());
    filter.filter(request, response);

    ObjectWriterModifier modifier = ObjectWriterInjector.get();
    assertNotNull(modifier);
    JsonGenerator g = mock(JsonGenerator.class);
    modifier.modify(
        mock(JsonEndpointConfig.class),
        new MultivaluedHashMap<String, Object>(),
        new Object(),
        mock(ObjectWriter.class),
        g);

    verify(g).useDefaultPrettyPrinter();
  }

  @Test
  public void testJSONGeneratorUntouched() throws IOException {
    JSONPrettyPrintFilter filter = new JSONPrettyPrintFilter();
    ContainerRequest request =
        ContainerRequestBuilder.from("http://localhost/foo/bar", "GET", null)
            .accept("random/media")
            .build();
    ContainerResponse response = new ContainerResponse(request, Response.ok().build());
    filter.filter(request, response);

    ObjectWriterModifier modifier = ObjectWriterInjector.get();
    assertNull(modifier);
  }
}
