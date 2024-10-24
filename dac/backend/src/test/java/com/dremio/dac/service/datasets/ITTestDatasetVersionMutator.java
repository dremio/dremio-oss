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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class ITTestDatasetVersionMutator extends BaseTestServer {

  private final DatasetVersionMutator service = getDatasetVersionMutator();
  private LegacyKVStoreProvider provider;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    provider = l(LegacyKVStoreProvider.class);
  }

  @Test
  public void testDeleteDatasetVersion() {
    final InitialPreviewResponse showSchemasResponse =
        getHttpClient().getDatasetApi().createDatasetFromSQL("SHOW SCHEMAS", emptyList());
    final List<String> path = showSchemasResponse.getDataset().getFullPath();
    final String version = showSchemasResponse.getDataset().getDatasetVersion().getVersion();
    final DatasetPath datasetPath = new DatasetPath(path);
    final DatasetVersion datasetVersion = new DatasetVersion(version);
    final VirtualDatasetUI versionBefore = service.getVersion(datasetPath, datasetVersion);
    assertNotNull("Dataset cannot be null after its creation", versionBefore);

    DatasetVersionMutator.deleteDatasetVersion(provider, path, version);

    assertThatThrownBy(() -> service.getVersion(datasetPath, datasetVersion))
        .isInstanceOf(DatasetVersionNotFoundException.class)
        .hasMessageContaining(
            String.format("dataset %s version %s", String.join(".", path), version));
  }

  @Test
  public void testDeleteDatasetVersion_deleteOtherSamePath_willNotChangeHistory()
      throws NamespaceException {
    final InitialPreviewResponse showSchemasResponse =
        getHttpClient().getDatasetApi().createDatasetFromSQL("SHOW SCHEMAS", emptyList());
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

  @Test
  public void testGetDatasetVersionWithDifferentCase() throws NamespaceException, IOException {
    setSpace();
    final DatasetPath datasetPath = new DatasetPath("spacefoo.folderbar.tEsTcAsE");
    final DatasetUI datasetUI =
        getHttpClient()
            .getDatasetApi()
            .createDatasetFromSQLAndSave(datasetPath, "SHOW SCHEMAS", emptyList());
    final DatasetVersion datasetVersion = datasetUI.getDatasetVersion();
    final VirtualDatasetUI vds = service.getVersion(datasetPath, datasetVersion);
    assertNotNull("Dataset cannot be null after its creation", vds);

    // All upper case
    final List<String> upperCasePath =
        datasetPath.toPathList().stream().map(String::toUpperCase).collect(Collectors.toList());
    final DatasetPath uppercaseDatasetPath = new DatasetPath(upperCasePath);
    final VirtualDatasetUI vds1 = service.getVersion(uppercaseDatasetPath, datasetVersion);
    assertNotNull("Must be able to get Dataset version using all upper case path", vds1);

    // All lower case
    final List<String> lowerCasePath =
        datasetPath.toPathList().stream().map(String::toLowerCase).collect(Collectors.toList());
    final DatasetPath lowercaseDatasetPath = new DatasetPath(lowerCasePath);
    final VirtualDatasetUI vds2 = service.getVersion(lowercaseDatasetPath, datasetVersion);
    assertNotNull("Must be able to get Dataset version using all lower case path", vds2);
  }
}
