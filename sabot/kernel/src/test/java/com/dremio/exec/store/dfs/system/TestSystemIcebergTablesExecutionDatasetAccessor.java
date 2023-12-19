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
package com.dremio.exec.store.dfs.system;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.store.iceberg.TableSchemaProvider;
import com.dremio.exec.store.iceberg.TableSnapshotProvider;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.file.proto.FileConfig;

public class TestSystemIcebergTablesExecutionDatasetAccessor {

  @Mock
  private EntityPath entityPath;
  @Mock
  private Supplier<Table> tableSupplier;
  @Mock
  private Configuration configuration;
  @Mock
  private TableSnapshotProvider tableSnapshotProvider;
  @Mock
  private SystemIcebergTablesStoragePlugin plugin;
  @Mock
  private TableSchemaProvider tableSchemaProvider;
  @Mock
  private OptionResolver optionResolver;

  private SystemIcebergTablesExecutionDatasetAccessor datasetHandle;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    datasetHandle = new SystemIcebergTablesExecutionDatasetAccessor(entityPath, tableSupplier, configuration,
      tableSnapshotProvider, plugin, tableSchemaProvider, optionResolver);
  }

  @Test
  public void testProvideSignature() throws ConnectorException {
    DatasetMetadata metadata = mock(DatasetMetadata.class);
    assertThat(datasetHandle.provideSignature(metadata)).isEqualTo(BytesOutput.NONE);
  }

  @Test
  public void testGetFileConfig() {
    Table mockTable = mock(Table.class);
    when(tableSupplier.get()).thenReturn(mockTable);
    FileConfig fileConfig = datasetHandle.getFileConfig();
    assertThat(fileConfig.getLocation()).isEqualTo(mockTable.location());
  }

}
