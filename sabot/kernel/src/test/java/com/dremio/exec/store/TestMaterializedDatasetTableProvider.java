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
package com.dremio.exec.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.MaterializedDatasetTable;
import com.dremio.exec.catalog.MaterializedDatasetTableProvider;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

public class TestMaterializedDatasetTableProvider {

  @Test
  public void testTimeTravelFlagOnScan() throws Exception {
    RelOptTable.ToRelContext toRelContext = mock(RelOptTable.ToRelContext.class, Mockito.RETURNS_DEEP_STUBS);
    when(toRelContext.getCluster().getTypeFactory()).thenReturn(SqlTypeFactoryImpl.INSTANCE);
    when(toRelContext.getCluster().getPlanner().getContext().unwrap(PlannerSettings.class))
      .thenReturn(mock(PlannerSettings.class));

    DatasetRetrievalOptions options = DatasetRetrievalOptions.newBuilder()
      .setTimeTravelRequest(TimeTravelOption.newSnapshotIdRequest("1"))
      .build()
      .withFallback(DatasetRetrievalOptions.DEFAULT);

    MaterializedDatasetTableProvider provider = getProviderWithOptions(options);
    MaterializedDatasetTable table = provider.get();
    RelNode rel = table.toRel(toRelContext, null);

    assertThat(rel).isInstanceOf(ScanCrel.class);
    ScanCrel scan = (ScanCrel) rel;
    assertThat(scan.isSubstitutable()).isFalse();

    provider = getProviderWithOptions(DatasetRetrievalOptions.DEFAULT);
    table = provider.get();
    rel = table.toRel(toRelContext, null);

    assertThat(rel).isInstanceOf(ScanCrel.class);
    scan = (ScanCrel) rel;
    assertThat(scan.isSubstitutable()).isTrue();
  }

  private MaterializedDatasetTableProvider getProviderWithOptions(DatasetRetrievalOptions options) throws Exception {
    BatchSchema schema = BatchSchema.newBuilder()
      .addField(Field.nullable("col1", new ArrowType.Int(32, true)))
      .build();
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setRecordSchema(ByteString.copyFrom(schema.serialize()));

    StoragePlugin storagePlugin = mock(StoragePlugin.class);
    when(storagePlugin.getDatasetMetadata(any(), any(), any())).thenReturn(
      DatasetMetadata.of(DatasetStats.of(1), schema));
    when(storagePlugin.listPartitionChunks(any(), any())).thenReturn(Collections::emptyIterator);
    RelOptTable.ToRelContext toRelContext = mock(RelOptTable.ToRelContext.class, Mockito.RETURNS_DEEP_STUBS);
    when(toRelContext.getCluster().getTypeFactory()).thenReturn(SqlTypeFactoryImpl.INSTANCE);
    when(toRelContext.getCluster().getPlanner().getContext().unwrap(PlannerSettings.class))
      .thenReturn(mock(PlannerSettings.class));

    return new MaterializedDatasetTableProvider(
        datasetConfig,
        () -> new EntityPath(Lists.newArrayList("a", "b")),
        storagePlugin,
        mock(StoragePluginId.class, Mockito.RETURNS_DEEP_STUBS),
        mock(SchemaConfig.class, Mockito.RETURNS_DEEP_STUBS),
        options, mock(OptionManager.class));
  }
}
