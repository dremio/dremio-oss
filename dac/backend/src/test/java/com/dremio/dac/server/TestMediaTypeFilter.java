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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.PreMatching;

import org.glassfish.jersey.message.internal.AcceptableMediaType;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.test.util.server.ContainerRequestBuilder;
import org.junit.Test;

/**
 * Unit tests for {@link MediaTypeFilter}
 */
public class TestMediaTypeFilter {

  @Test
  public void testAnnotations() {
    // Check that the class is correctly annotated
    assertNotNull("@PreMatching annotation is required to modify headers", MediaTypeFilter.class.getAnnotation(PreMatching.class));
    Priority priority = MediaTypeFilter.class.getAnnotation(Priority.class);
    assertNotNull("@Priority annotation is required to modify headers", priority);

    assertTrue("priority should be higher than HEADER_DECORATOR", priority.value() <= Priorities.HEADER_DECORATOR);
  }

  @Test
  public void testHeaderChange() throws IOException {
    MediaTypeFilter filter = new MediaTypeFilter();
    ContainerRequest request = ContainerRequestBuilder.from("http://localhost/foo/bar?format=unit/test", "GET", null).accept("random/media").build();
    filter.filter(request);

    assertEquals(1, request.getAcceptableMediaTypes().size());
    assertEquals(new AcceptableMediaType("unit", "test"), request.getAcceptableMediaTypes().get(0));
  }

  @Test
  public void testHeaderIsUntouched() throws IOException {
    MediaTypeFilter filter = new MediaTypeFilter();
    ContainerRequest request = ContainerRequestBuilder.from("http://localhost/foo/bar", "GET", null).accept("random/media").build();
    filter.filter(request);

    assertEquals(1, request.getAcceptableMediaTypes().size());
    assertEquals(new AcceptableMediaType("random", "media"), request.getAcceptableMediaTypes().get(0));
  }

}
