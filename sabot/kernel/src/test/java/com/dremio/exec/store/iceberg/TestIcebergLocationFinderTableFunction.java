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
package com.dremio.exec.store.iceberg;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.NotFoundException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.physical.config.IcebergLocationFinderFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

/**
 * Tests for {@link IcebergLocationFinderTableFunction}
 */
public class TestIcebergLocationFinderTableFunction extends BaseTestTableFunction {

  private static final Configuration CONF = new Configuration();
  private static FileSystem fs;
  @Mock
  private StoragePluginId pluginId;
  @Mock(extraInterfaces = { SupportsIcebergRootPointer.class })
  private SupportsIcebergMutablePlugin plugin;

  @BeforeClass
  public static void initStatics() throws Exception {
    fs = HadoopFileSystem.get(Path.of("/"), CONF);
  }

  @Before
  public void prepareMocks() throws Exception {
    when(fec.getStoragePlugin(pluginId)).thenReturn(plugin);
    when(plugin.createFSWithAsyncOptions(anyString(), anyString(), any())).thenReturn(fs);
    when(plugin.getFsConfCopy()).thenReturn(CONF);
    when(plugin.createIcebergFileIO(any(), any(), any(), any(), any()))
      .thenReturn(new DremioFileIO(fs, null, null, null, null,
        new HadoopFileSystemConfigurationAdapter(CONF)));
  }

  @Test
  public void testTableLocationFinder() throws Exception {
    Table input = t(
      th(SystemSchemas.METADATA_FILE_PATH),
      tr(Resources.getResource("iceberg/empty_table/metadata/v1.metadata.json").toURI().toString()),
      tr(Resources.getResource("iceberg/empty_table/metadata/v1.gc_disabled.metadata.json").toURI().toString()), // should get skipped
      tr(Resources.getResource("iceberg/partitionednation/metadata/v1.metadata.json").toURI().toString()),
      tr(Resources.getResource("iceberg/v2/orders/metadata/v1.metadata.json").toURI().toString())
    );

    Table output = t(
      th(SystemSchemas.TABLE_LOCATION),
      tr("/tmp/empty_iceberg"),
      tr("/tmp/iceberg"),
      tr("/tmp/iceberg-test-tables/v2/orders")
    );

    validateSingle(getPop(), TableFunctionOperator.class, input, output, 3);
  }

  @Test
  public void testTableLocationFinderContinueOnError() throws Exception {
    Table input = t(
      th(SystemSchemas.METADATA_FILE_PATH),
      tr(Resources.getResource("iceberg/empty_table/metadata/v1.metadata.json").toURI().toString()),
      tr(new File("iceberg/v2/path/does/not/exist.metadata.json").toURI().toString()),
      tr(Resources.getResource("iceberg/partitionednation/metadata/v1.metadata.json").toURI().toString())
    );

    Table output = t(
      th(SystemSchemas.TABLE_LOCATION),
      tr("/tmp/empty_iceberg"),
      tr("/tmp/iceberg")
    );

    validateSingle(getPop(true), TableFunctionOperator.class, input, output, 3);

    assertThatThrownBy(() -> validateSingle(getPop(false), TableFunctionOperator.class, input, output, 3))
      .isInstanceOf(NotFoundException.class);
  }

  private TableFunctionPOP getPop() {
    return getPop(false);
  }

  private TableFunctionPOP getPop(boolean continueOnError) {
    return new TableFunctionPOP(
      PROPS,
      null,
      new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_TABLE_LOCATION_FINDER,
        true,
        new IcebergLocationFinderFunctionContext(pluginId,
          SystemSchemas.TABLE_LOCATION_SCHEMA,
          ImmutableList.of(SchemaPath.getSimplePath(SystemSchemas.TABLE_LOCATION)),
          ImmutableMap.of(TableProperties.GC_ENABLED, Boolean.FALSE.toString()),
          continueOnError)));
  }
}
