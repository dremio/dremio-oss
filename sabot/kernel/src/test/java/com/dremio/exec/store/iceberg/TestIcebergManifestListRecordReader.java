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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.expressions.Expressions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;

import com.dremio.common.expression.BasePath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.google.common.collect.ImmutableList;

public class TestIcebergManifestListRecordReader extends BaseTestOperator {

  private static final int DEFAULT_BATCH_SIZE = 3;
  private static final String METADATA_JSON = "/metadata/v5.metadata.json";
  private static final long SNAPSHOT_ID = 5668393571500528520L;

  private static IcebergTestTables.Table table;

  private OperatorContextImpl context;
  private FileSystem fs;
  @Mock(extraInterfaces = { MutablePlugin.class })
  private SupportsIcebergRootPointer plugin;

  @BeforeClass
  public static void initTables() {
    table = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
  }

  @AfterClass
  public static void closeTables() throws Exception {
    table.close();
  }

  @Before
  public void beforeTest() throws Exception {
    context = testContext.getNewOperatorContext(getTestAllocator(), null, DEFAULT_BATCH_SIZE, null);
    testCloseables.add(context);
    Configuration conf = new Configuration();
    fs = HadoopFileSystem.get(Path.of("/"), conf, context.getStats());

    when(plugin.createFSWithAsyncOptions(anyString(), anyString(), any())).thenReturn(fs);
    when(plugin.getFsConfCopy()).thenReturn(conf);
    when(plugin.createIcebergFileIO(any(), any(), any(), any(), any()))
        .thenReturn(new DremioFileIO(fs, null, null, null, null,
            new HadoopFileSystemConfigurationAdapter(conf)));
  }

  @Test
  public void testReadDataManifests() throws Exception {
    readAndValidate(table.getLocation() + METADATA_JSON, SNAPSHOT_ID, ManifestContentType.DATA,
        ImmutableList.of(table.getLocation() + "/metadata/8a83125a-a077-4f1e-974b-fcbaf370b085-m0.avro"));
  }

  @Test
  public void testReadDeleteManifests() throws Exception {
    readAndValidate(table.getLocation() + METADATA_JSON, SNAPSHOT_ID, ManifestContentType.DELETES,
        ImmutableList.of(
            table.getLocation() + "/metadata/07fe993a-9195-4cbc-bf9a-6b81816b9758-m0.avro",
            table.getLocation() + "/metadata/d1e51173-03f4-4b54-865a-c6c3185a92a5-m0.avro",
            table.getLocation() + "/metadata/d45e915a-acf8-4914-9907-0772d5356e4a-m0.avro"));
  }

  private void readAndValidate(String jsonPath, long snapshotId, ManifestContentType manifestContent, List<String> expectedManifestFiles)
      throws Exception {
    List<String> actual = new ArrayList<>();
    try (AutoCloseable closeable = with(ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN, true)) {
      IcebergExtendedProp extendedProp = new IcebergExtendedProp(
          null,
          IcebergSerDe.serializeToByteArray(Expressions.alwaysTrue()),
          snapshotId,
          null);

      IcebergManifestListRecordReader reader = new IcebergManifestListRecordReader(
          context,
          jsonPath,
          plugin,
          ImmutableList.of("table"),
          null,
          SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
          PROPS,
          null,
          extendedProp,
          manifestContent);
      testCloseables.add(reader);

      TestOutputMutator outputMutator = new TestOutputMutator(getTestAllocator());
      testCloseables.add(outputMutator);
      // don't materialize projected columns like row num here to simulate how it works in the production stack
      SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA.materializeVectors(
          ImmutableList.of(
              BasePath.getSimple(SystemSchemas.SPLIT_IDENTITY),
              BasePath.getSimple(SystemSchemas.SPLIT_INFORMATION),
              BasePath.getSimple(SystemSchemas.COL_IDS)),
          outputMutator);
      reader.setup(outputMutator);
      reader.allocate(outputMutator.getFieldVectorMap());

      int records;
      StructVector splitIdentityVector = (StructVector) outputMutator.getVector(SystemSchemas.SPLIT_IDENTITY);
      VarCharVector pathVector = (VarCharVector) splitIdentityVector.getChild(SplitIdentity.PATH);
      while ((records = reader.next()) > 0) {
        for (int i = 0; i < records; i++) {
          actual.add(new String(pathVector.get(i), StandardCharsets.UTF_8));
        }
      }
    }

    assertThat(actual).isEqualTo(expectedManifestFiles);
  }
}
