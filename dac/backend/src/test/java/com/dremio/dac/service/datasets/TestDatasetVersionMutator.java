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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.NameSpaceContainer;

@MockitoSettings(strictness = Strictness.WARN)
public class TestDatasetVersionMutator {
  @Mock
  private OptionManager optionManager;
  @Mock
  private NamespaceService namespaceService;
  @Mock
  private CatalogService catalogService;
  @Mock
  private InitializerRegistry initializerRegistry;
  @Mock
  private LegacyKVStoreProvider legacyKVStoreProvider;
  @Mock
  private JobsService jobsService;
  @Mock
  private ContextService contextService;
  @Mock
  private DataplanePlugin dataplanePlugin;

  private static final String versionedSourceName = "nessie";

  private DatasetVersionMutator datasetVersionMutator;

  @BeforeEach
  public void setup() throws Exception {
    datasetVersionMutator = new DatasetVersionMutator(
      initializerRegistry,
      legacyKVStoreProvider,
      namespaceService,
      jobsService,
      catalogService,
      optionManager,
      contextService);
  }

  @Test
  public void testRenameDatasetForVersionedSource() throws Exception {
    setupForVersionedSource();
    DatasetPath oldDatasetPath = new DatasetPath(new SourceName(versionedSourceName), new DatasetName("testTable"));
    DatasetPath newDatasetPath = new DatasetPath(new SourceName(versionedSourceName), new DatasetName("testMoveTable"));

    assertThatThrownBy(() -> datasetVersionMutator.renameDataset(oldDatasetPath, newDatasetPath))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("not allowed in Versioned source");
  }

  @Test
  public void testCopyFromDatasetForVersionedSource() throws Exception {
    setupForVersionedSource();
    DatasetPath datasetPath = new DatasetPath(new SourceName(versionedSourceName), new DatasetName("testTable"));

    assertThatThrownBy(() -> datasetVersionMutator.createDatasetFrom(datasetPath, datasetPath, "userName"))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("not allowed within Versioned source");
  }

  private void setupForVersionedSource() throws NamespaceException {
    NameSpaceContainer nameSpaceContainer = mock(NameSpaceContainer.class);
    when(namespaceService.getEntityByPath(new NamespaceKey(versionedSourceName))).thenReturn(nameSpaceContainer);
    when(nameSpaceContainer.getType()).thenReturn(NameSpaceContainer.Type.SOURCE);
    when(catalogService.getSource(versionedSourceName)).thenReturn(dataplanePlugin);
  }

}
