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
package com.dremio.dac.service.datasets;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.collect.Lists;

public class TestDatasetVersionMutator extends BaseTestServer {

  private final DatasetVersionMutator service = newDatasetVersionMutator();
  private LegacyKVStoreProvider provider;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    provider = l(LegacyKVStoreProvider.class);
  }

  @Test
  public void testDeleteDatasetVersion() throws NamespaceException {
    final InitialPreviewResponse showSchemasResponse = createDatasetFromSQL("SHOW SCHEMAS", emptyList());
    final List<String> path = showSchemasResponse.getDataset().getFullPath();
    final String version = showSchemasResponse.getDataset().getDatasetVersion().getVersion();
    final DatasetPath datasetPath = new DatasetPath(path);
    final DatasetVersion datasetVersion = new DatasetVersion(version);
    final VirtualDatasetUI versionBefore = service.getVersion(datasetPath, datasetVersion);
    assertNotNull("Dataset cannot be null after its creation", versionBefore);

    DatasetVersionMutator.deleteDatasetVersion(provider, path, version);

    exceptionRule.expect(DatasetVersionNotFoundException.class);
    exceptionRule.expectMessage(String.format("dataset %s version %s", String.join(".", path), version));
    service.getVersion(datasetPath, datasetVersion);
  }

  @Test
  public void testDeleteDatasetVersion_deleteOtherSamePath_willNotChangeHistory() {
    final InitialPreviewResponse showSchemasResponse = createDatasetFromSQL("SHOW SCHEMAS", emptyList());
    final List<String> path = showSchemasResponse.getDataset().getFullPath();
    final String version = showSchemasResponse.getDataset().getDatasetVersion().getVersion();
    final DatasetPath datasetPath = new DatasetPath(path);
    final DatasetVersion datasetVersion = new DatasetVersion(version);
    final VirtualDatasetUI versionBefore = service.getVersion(datasetPath, datasetVersion);
    assertNotNull("Dataset cannot be null after its creation", versionBefore);
    List<VirtualDatasetUI> allVersions = Lists.newArrayList(service.getAllVersions(datasetPath));
    assertEquals("Dataset must have just one version", 1, allVersions.size());

    DatasetVersionMutator.deleteDatasetVersion(provider, path, "00000000000");

    final VirtualDatasetUI versionAfter = service.getVersion(datasetPath, datasetVersion);
    assertEquals("Version must exist", datasetVersion, versionAfter.getVersion());
    allVersions = Lists.newArrayList(service.getAllVersions(datasetPath));
    assertEquals("Dataset must have just one version", 1, allVersions.size());
  }

}
