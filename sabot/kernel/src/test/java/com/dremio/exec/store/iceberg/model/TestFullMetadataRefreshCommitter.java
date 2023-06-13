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
package com.dremio.exec.store.iceberg.model;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

@RunWith(MockitoJUnitRunner.class)
public class TestFullMetadataRefreshCommitter {

  @Mock
  private DatasetCatalogGrpcClient client;
  @Mock
  private Table table;
  @Mock
  private Snapshot snapshot;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private IcebergCommand icebergCommand;

  private FullMetadataRefreshCommitter fullMetadataRefreshCommitter;

  @Test
  public void testCommitWhenThereIsException() {
    // Given
    Mockito.doThrow(new StatusRuntimeException(Status.ABORTED)).when(fullMetadataRefreshCommitter).addOrUpdateDataSet();
    Mockito.doReturn(false).when(fullMetadataRefreshCommitter).isMetadataAlreadyCreated();

    try {
      // When
      fullMetadataRefreshCommitter.commit();
    } catch (UserException ex) {
      // Then
      Assert.assertEquals(ex.getErrorType(), UserBitShared.DremioPBError.ErrorType.CONCURRENT_MODIFICATION);
      Assert.assertEquals(UserException.REFRESH_METADATA_FAILED_CONCURRENT_UPDATE_MSG, ex.getMessage());
      return;
    }
    Assert.fail("Should throw exception");
  }


  @Before
  public void init() {
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    datasetConfig.setPhysicalDataset(new PhysicalDataset());
    datasetConfig.setReadDefinition(new ReadDefinition());
    fullMetadataRefreshCommitter = Mockito.spy(new FullMetadataRefreshCommitter(
      "test", new ArrayList<>(), null, "test", new BatchSchema(new ArrayList<>()), null,
      new ArrayList<>(), icebergCommand, client, datasetConfig,
      null, null, null));
    Mockito.when(icebergCommand.endTransaction()).thenReturn(table);
    Mockito.when(table.currentSnapshot()).thenReturn(snapshot);
    Mockito.when(snapshot.summary()).thenReturn(new HashMap<>());
    Mockito.when(icebergCommand.getRootPointer()).thenReturn("/test/metadata.json");
    Mockito.when(icebergCommand.getIcebergSchema()).thenReturn(new Schema(1, new ArrayList<>()));
  }

}
