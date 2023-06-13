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
package com.dremio.dac.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.core.SecurityContext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.Dataset;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogEntityKey;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.test.DremioTest;

/**
 * Tests for catalog service helper for arctic
 */
public class TestCatalogServiceHelperForVersioned extends DremioTest {
  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.LENIENT);

  @Mock private ReflectionSettings reflectionSettings;
  @Mock private DataplanePlugin dataplanePlugin;
  @Mock private DremioTable dremioTable;

  @Mock private Catalog catalog;
  @Mock private SecurityContext securityContext;
  @Mock private SourceService sourceService;
  @Mock private NamespaceService namespaceService;
  @Mock private SabotContext sabotContext;
  @Mock private ReflectionServiceHelper reflectionServiceHelper;
  @Mock private HomeFileTool homeFileTool;
  @Mock private DatasetVersionMutator datasetVersionMutator;
  @Mock private SearchService searchService;

  private static final String datasetId = UUID.randomUUID().toString();

  @InjectMocks private CatalogServiceHelper catalogServiceHelper;

  @Before
  public void setup() {
    when(catalog.getSource(anyString())).thenReturn(dataplanePlugin);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);
    when(reflectionSettings.getStoredReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(Optional.empty());
  }

  @Test
  public void getCatalogEntityByPathWithNullReference() throws Exception {
    assertThatThrownBy(
            () ->
                catalogServiceHelper.getCatalogEntityByPath(
                    Arrays.asList("arctic", "table"), null, null, null, null))
        .isInstanceOf(ClientErrorException.class)
        .hasMessageContaining("Missing a versionType/versionValue");
  }

  @Test
  public void getCatalogEntityByPathNotFound() throws Exception {
    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
        .thenReturn(null);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("arctic", "table"), null, null, "BRANCH", "main");

    assertThat(catalogEntity.isPresent()).isFalse();
  }

  @Test
  public void getCatalogEntityByPathForTable() throws Exception {
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId(datasetId))
            .setFullPathList(Collections.singletonList("table"));

    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
        .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("arctic", "table"), null, null, "BRANCH", "main");

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();

    assertThat(dataset.getId()).isEqualTo(datasetId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.PHYSICAL_DATASET);
  }

  @Test
  public void getCatalogEntityByPathForSnapshot() throws Exception {
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId(datasetId))
            .setFullPathList(Collections.singletonList("table"));

    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
        .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("arctic", "table"), null, null, "SNAPSHOT", "1128544236092645872");

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();

    assertThat(dataset.getId()).isEqualTo(datasetId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.PHYSICAL_DATASET);
  }

  @Test
  public void getCatalogEntityByPathForTimestamp() throws Exception {
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId(datasetId))
            .setFullPathList(Collections.singletonList("table"));

    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
        .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("arctic", "table"), null, null, "TIMESTAMP", "1679029735226");

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();

    assertThat(dataset.getId()).isEqualTo(datasetId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.PHYSICAL_DATASET);
  }

  @Test
  public void getCatalogEntityByPathForView() throws Exception {
    final DatasetConfig datasetConfig =
      new DatasetConfig()
        .setType(DatasetType.VIRTUAL_DATASET)
        .setId(new EntityId(datasetId))
        .setFullPathList(Collections.singletonList("table"));

    when(catalog.getTableSnapshot(any(NamespaceKey.class), any(TableVersionContext.class)))
      .thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
      catalogServiceHelper.getCatalogEntityByPath(
        Arrays.asList("arctic", "view"), null, null, "BRANCH", "main");

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();

    assertThat(dataset.getId()).isEqualTo(datasetId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.VIRTUAL_DATASET);
  }
}
