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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.dfs.MetadataStoragePlugin;
import com.dremio.exec.store.metadatarefresh.SupportsUnlimitedSplits;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

@Ignore("DX-50441")
public class TestDatasetSaverImpl {

  @Mock
  private OptionManager optionManager;
  @Mock
  private DatasetHandle datasetHandle;
  @Mock
  private SupportsUnlimitedSplits supportsUnlimitedSplits;
  @Mock
  private SabotContext sabotContext;
  @Mock
  private MetadataStoragePlugin metadataStoragePlugin;
  private DatasetConfig datasetConfig = new DatasetConfig();

  DatasetSaverImpl datasetSaver;

  @Before
  public void init() {
    MockitoAnnotations.openMocks(this);
    datasetSaver = new DatasetSaverImpl(null, null, optionManager);
    List<String> entityPaths = new ArrayList<>();
    entityPaths.add("items");
    Mockito.when(datasetHandle.getDatasetPath()).thenReturn(new EntityPath(entityPaths));
    Mockito.when(optionManager.getOption(ExecConstants.ENABLE_ICEBERG)).thenReturn(true);
    Mockito.when(optionManager.getOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT)).thenReturn(true);
    Mockito.when(metadataStoragePlugin.allowUnlimitedSplits(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
  }

  @Test
  public void testSaveWhenThereIsException() throws Exception {

    // Given
    Status status = Status.fromThrowable(UserException.concurrentModificationError()
      .message(UserException.REFRESH_METADATA_FAILED_CONCURRENT_UPDATE_MSG)
      .buildSilently());
    Mockito.doThrow(new StatusRuntimeException(status))
      .when(metadataStoragePlugin).runRefreshQuery(Mockito.any(), Mockito.any());

    try {
      // When
      datasetSaver.save(datasetConfig, datasetHandle, metadataStoragePlugin,
        false, DatasetRetrievalOptions.DEFAULT);
    } catch (UserException userException) {
      Assert.assertEquals(UserException.REFRESH_METADATA_FAILED_CONCURRENT_UPDATE_MSG, userException.getMessage());
      return;
    }
    Assert.fail("Should throw exception");
  }
}
