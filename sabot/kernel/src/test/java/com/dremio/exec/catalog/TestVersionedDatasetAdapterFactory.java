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
package com.dremio.exec.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.service.namespace.file.proto.FileType;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestVersionedDatasetAdapterFactory {

  public static final List<String> VERSIONED_TABLE_KEY = Arrays.asList("Arctic", "mytable");

  @Captor private ArgumentCaptor<GetDatasetOption[]> getDatasetOptionsArgumentCaptor;

  @Test
  public void testNewInstanceNull() {
    assertNull(
        new VersionedDatasetAdapterFactory()
            .newInstance(
                VERSIONED_TABLE_KEY,
                mock(ResolvedVersionContext.class),
                mock(StoragePlugin.class),
                mock(StoragePluginId.class),
                null));
  }

  @Test
  public void testNewInstanceException() throws ConnectorException {
    StoragePlugin storagePlugin = mock(StoragePlugin.class);
    when(storagePlugin.getDatasetHandle(any(), any(GetDatasetOption[].class)))
        .thenThrow(ConnectorException.class);

    assertThrows(
        UserException.class,
        () ->
            new VersionedDatasetAdapterFactory()
                .newInstance(
                    VERSIONED_TABLE_KEY,
                    mock(ResolvedVersionContext.class),
                    storagePlugin,
                    mock(StoragePluginId.class),
                    null));
  }

  @Test
  public void testNewInstance() throws Exception {
    EntityPath entityPath = new EntityPath(VERSIONED_TABLE_KEY);
    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    StoragePlugin storagePlugin = mock(StoragePlugin.class);

    when(storagePlugin.getDatasetHandle(eq(entityPath), any(GetDatasetOption[].class)))
        .thenReturn(Optional.of(mock(DatasetHandle.class)));

    VersionedDatasetAdapter versionedDatasetAdapter =
        new VersionedDatasetAdapterFactory()
            .newInstance(
                VERSIONED_TABLE_KEY,
                resolvedVersionContext,
                storagePlugin,
                mock(StoragePluginId.class),
                null);

    assertEquals(storagePlugin, versionedDatasetAdapter.getStoragePlugin());
    assertEquals(versionedDatasetAdapter.getOwner(entityPath, null, null), Optional.empty());

    verify(storagePlugin)
        .getDatasetHandle(eq(entityPath), getDatasetOptionsArgumentCaptor.capture());

    GetDatasetOption[] getDatasetOptions = getDatasetOptionsArgumentCaptor.getValue();
    assertEquals(
        resolvedVersionContext,
        ((VersionedDatasetAccessOptions) getDatasetOptions[3]).getVersionContext());
    assertEquals(
        FileType.ICEBERG, ((FileConfigOption) getDatasetOptions[4]).getFileConfig().getType());
  }
}
