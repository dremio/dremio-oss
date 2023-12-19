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
package com.dremio.service.reflection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.options.OptionManager;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestReflectionStatusServiceListing {

  @Mock
  private NamespaceService namespaceService;

  @Mock
  private SabotContext sabotContext;

  @Mock
  private ReflectionGoalsStore goalsStore;

  @Mock
  private ReflectionEntriesStore entriesStore;

  @Mock
  private MaterializationStore materializationStore;

  @Mock
  private ExternalReflectionStore externalReflectionStore;

  @Mock
  private ReflectionValidator validator;

  @Mock
  private CatalogService catalogService;

  @Mock
  private Catalog entityExplorer;

  @Mock
  private OptionManager optionManager;

  private String datasetId;

  private ReflectionId reflectionId;

  private ReflectionStatusService statusService;

  @Before
  public void setup() {
    datasetId = UUID.randomUUID().toString();
    final List<String> dataPath = Arrays.asList("source", "folder", "dataset");
    final DatasetConfig dataset = new DatasetConfig()
      .setId(new EntityId(datasetId))
      .setFullPathList(dataPath)
      .setType(DatasetType.VIRTUAL_DATASET);
    final DremioTable table = mock(DremioTable.class);
    when(table.getDatasetConfig()).thenReturn(dataset);
    when(entityExplorer.getTable(datasetId)).thenReturn(table);
    when(catalogService.getCatalog(any(MetadataRequestOptions.class))).thenReturn(entityExplorer);

    reflectionId = new ReflectionId(UUID.randomUUID().toString());
    ReflectionEntry entry = new ReflectionEntry();
    entry.setState(ReflectionState.ACTIVE).setId(reflectionId);
    when(entriesStore.get(reflectionId)).thenReturn(entry);

    when(namespaceService.findDatasetByUUID(datasetId)).thenReturn(dataset);

    statusService = new ReflectionStatusServiceImpl(
      sabotContext::getExecutors,
      DirectProvider.<MaterializationCache.CacheViewer>wrap(new TestReflectionStatusService.ConstantCacheViewer(true)),
      goalsStore,
      entriesStore,
      materializationStore,
      externalReflectionStore,
      validator,
      DirectProvider.wrap(catalogService),
      DirectProvider.wrap(optionManager)
    );
  }

  /**
   * Verify that we can list reflections for the sys.reflections table
   */
  @Test
  public void testGetReflection() {

    final ReflectionField field = new ReflectionField();
    field.setName("myDisplayField1");
    final ReflectionField field2 = new ReflectionField();
    field2.setName("myDisplayField2");
    final ReflectionDetails details = new ReflectionDetails();
    details.setDisplayFieldList(Arrays.asList(field, field2));

    final ReflectionGoal goal = new ReflectionGoal()
      .setId(reflectionId)
      .setDatasetId(datasetId)
      .setState(ReflectionGoalState.ENABLED)
      .setCreatedAt(0L)
      .setName("myReflection")
      .setType(ReflectionType.RAW)
      .setDetails(details);
    when(goalsStore.getAllNotDeleted()).thenReturn(Arrays.asList(goal));

    Iterator<AccelerationListManager.ReflectionInfo> reflections = statusService.getReflections();
    AccelerationListManager.ReflectionInfo info = reflections.next();
    assertEquals("source.folder.dataset", info.dataset);
    assertEquals(datasetId, info.datasetId);
    assertEquals("myDisplayField1, myDisplayField2", info.displayColumns);
    assertEquals("myReflection", info.name);
    assertEquals("UNKNOWN", info.status);
    assertEquals("RAW", info.type);
    assertFalse(reflections.hasNext());
  }

  /**
   * Verify that a missing dataset won't cause listing API to break
   */
  @Test
  public void testGetReflectionOnMissingDataset() {

    final ReflectionGoal goal = new ReflectionGoal()
      .setId(reflectionId)
      .setDatasetId("foo")
      .setState(ReflectionGoalState.ENABLED)
      .setCreatedAt(0L)
      .setName("myReflection")
      .setType(ReflectionType.RAW);
    when(goalsStore.getAllNotDeleted()).thenReturn(Arrays.asList(goal));

    Iterator<AccelerationListManager.ReflectionInfo> reflections = statusService.getReflections();
    assertFalse(reflections.hasNext());
  }
}
