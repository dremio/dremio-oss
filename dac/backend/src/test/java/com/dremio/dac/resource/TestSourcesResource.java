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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.dac.server.BaseTestServer;
import org.junit.Test;

/** Tests {@link SourcesResource} API */
public class TestSourcesResource extends BaseTestServer {

  private static final String SOURCES_PATH = "/sources";

  @Test
  public void testGetSourcesWithIncludeDataCountParameter() {
    doc("includeDatasetCount is true");
    String sources =
        expectSuccess(
            getBuilder(getAPIv2().path(SOURCES_PATH).queryParam("includeDatasetCount", true))
                .buildGet(),
            String.class);

    assertTrue(
        "the numberOfDatasets field should be in the response, if includeDatasetCount is true ",
        sources.contains("numberOfDatasets"));
    assertTrue(
        "the datasetCountBounded field should be in the response, if includeDatasetCount is true",
        sources.contains("datasetCountBounded"));

    doc("includeDatasetCount is false");
    sources =
        expectSuccess(
            getBuilder(getAPIv2().path(SOURCES_PATH).queryParam("includeDatasetCount", false))
                .buildGet(),
            String.class);

    assertFalse(
        "the numberOfDatasets field should be in the response, if includeDatasetCount is false ",
        sources.contains("numberOfDatasets"));
    assertFalse(
        "the datasetCountBounded field should be in the response, if includeDatasetCount is false",
        sources.contains("datasetCountBounded"));

    doc("includeDatasetCount is not set");
    sources = expectSuccess(getBuilder(getAPIv2().path(SOURCES_PATH)).buildGet(), String.class);

    assertTrue(
        "the numberOfDatasets field should be in the response, if includeDatasetCount is not set",
        sources.contains("numberOfDatasets"));
    assertTrue(
        "the datasetCountBounded field should be in the response, if includeDatasetCount is not set",
        sources.contains("datasetCountBounded"));
  }
}
