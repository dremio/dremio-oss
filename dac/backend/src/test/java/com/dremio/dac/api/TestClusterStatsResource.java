/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.api;

import static com.dremio.exec.store.CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.namespace.source.proto.SourceConfig;


/**
 * Tests the {@link ClusterStatsResource} API
 */
public class TestClusterStatsResource extends BaseTestServer {
  private static final String PATH = "/cluster/stats";

  @Test
  public void testListSources() throws Exception {
    ClusterStatsResource.ClusterStats stats = expectSuccess(getBuilder(getPublicAPI(3).path(PATH)).buildGet(), ClusterStatsResource.ClusterStats.class);
    assertNotNull(stats);
    assertEquals(stats.getSources().size(), newSourceService().getSources().size());
  }

  @Test
  public void testSamplesS3() throws  Exception{

    List<SourceConfig> sources = new ArrayList();

    SourceConfig configS3Samples = new SourceConfig();
    configS3Samples.setMetadataPolicy(DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE);
    configS3Samples.setName("Samples");
    configS3Samples.setType("S3");

    SourceConfig configS3 = new SourceConfig();
    configS3.setMetadataPolicy(DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE);
    configS3.setName("SourceS3");
    configS3.setType("S3");

    sources.add(configS3Samples);
    sources.add(configS3);

    ClusterStatsResource.Stats result = ClusterStatsResource.getSources(sources,getSabotContext());

    assertTrue("Type is incorrect", "SamplesS3".equals(result.getAllSources().get(0).getType()));
    assertTrue("Type is incorrect", "S3".equals(result.getAllSources().get(1).getType()));

  }

}
