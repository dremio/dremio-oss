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

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Tests {@link FunctionsListService} API
 */
public class TestFunctionsResource extends BaseTestServer {
  @Test
  public void testFunctions() throws Exception {
    final FunctionsListService.Response response = getFunctions();
    Assert.assertNotNull(response.getFunctions());
  }

  public FunctionsListService.Response getFunctions() throws Exception {
    final String endpoint = "/sql/functions";

    Response response = expectSuccess(
      getBuilder(getAPIv2().path(endpoint))
        .buildGet());
    String stringResponse = response.readEntity(String.class);

    ObjectMapper objectMapper = new ObjectMapper(
      new YAMLFactory()
        .disable(YAMLGenerator.Feature.SPLIT_LINES)
        .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
        .enable(YAMLGenerator.Feature.INDENT_ARRAYS))
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .registerModule(new JavaTimeModule())
      .registerModule(new GuavaModule())
      .registerModule(new Jdk8Module());

    return objectMapper.readValue(stringResponse, FunctionsListService.Response.class);
  }
}
