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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import java.util.List;
import javax.inject.Provider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TestReflectionSettingsImpl {

  public static final String MY_SOURCE_NAME = "mysource";
  public static final List<String> MY_TABLE_KEY = List.of(MY_SOURCE_NAME, "mytable");
  @Mock private Provider<NamespaceService> namespaceServiceProvider;
  @Mock private Provider<CatalogService> catalogServiceProvider;
  @Mock private Provider<LegacyKVStoreProvider> legacyKVStoreProviderProvider;
  @Mock private LegacyKVStoreProvider legacyKVStoreProvider;
  @Mock private LegacyKVStore<CatalogEntityKey, AccelerationSettings> legacyKVStore;
  @Mock private Provider<OptionManager> optionManagerProvider;

  private ReflectionSettingsImpl reflectionSettings;

  private CatalogEntityKey mockStoreAccelerationSettings(
      TableVersionContext tableVersionContext, AccelerationSettings accelerationSettings) {
    CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(TestReflectionSettingsImpl.MY_TABLE_KEY)
            .tableVersionContext(tableVersionContext)
            .build();
    when(legacyKVStore.get(eq(catalogEntityKey))).thenReturn(accelerationSettings);
    return catalogEntityKey;
  }

  private void mockGetSource(boolean isVersioned) {
    CatalogService catalogService = mock(CatalogService.class);
    StoragePlugin storagePlugin = mock(StoragePlugin.class);

    when(storagePlugin.isWrapperFor(eq(VersionedPlugin.class))).thenReturn(isVersioned);
    when(catalogService.getSource(eq(MY_SOURCE_NAME))).thenReturn(storagePlugin);
    when(catalogServiceProvider.get()).thenReturn(catalogService);
  }

  @BeforeEach
  public void beforeEach() {
    when(legacyKVStoreProvider.getStore(any())).thenReturn(legacyKVStore);
    when(legacyKVStoreProviderProvider.get()).thenReturn(legacyKVStoreProvider);

    reflectionSettings =
        new ReflectionSettingsImpl(
            namespaceServiceProvider,
            catalogServiceProvider,
            legacyKVStoreProviderProvider,
            optionManagerProvider);
  }

  @Test
  public void testGetReflectionSettingsNullAndNotSpecifiedDifferent() {
    AccelerationSettings nullTableVersionContextAccelerationSettings =
        mock(AccelerationSettings.class);
    CatalogEntityKey nullTableVersionContextCatalogEntityKey =
        mockStoreAccelerationSettings(null, nullTableVersionContextAccelerationSettings);
    AccelerationSettings notSpecifiedTableVersionContextAccelerationSettings =
        mock(AccelerationSettings.class);
    CatalogEntityKey notSpecifiedTableVersionContextCatalogEntityKey =
        mockStoreAccelerationSettings(
            TableVersionContext.NOT_SPECIFIED, notSpecifiedTableVersionContextAccelerationSettings);

    AccelerationSettings accelerationSettingsWithNull =
        reflectionSettings.getReflectionSettings(nullTableVersionContextCatalogEntityKey);
    AccelerationSettings accelerationSettingsWithNotSpecified =
        reflectionSettings.getReflectionSettings(notSpecifiedTableVersionContextCatalogEntityKey);
    assertNotEquals(accelerationSettingsWithNull, accelerationSettingsWithNotSpecified);
  }

  @Test
  public void testGetReflectionSettings() {
    AccelerationSettings accelerationSettings = mock(AccelerationSettings.class);
    CatalogEntityKey catalogEntityKey =
        mockStoreAccelerationSettings(mock(TableVersionContext.class), accelerationSettings);
    assertEquals(accelerationSettings, reflectionSettings.getReflectionSettings(catalogEntityKey));
  }
}
