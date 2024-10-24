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

import static com.dremio.io.file.UriSchemes.SCHEME_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.BasePath;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

public class TestIcebergManifestListRecordReader extends BaseTestOperator {

  private static final int DEFAULT_BATCH_SIZE = 3;
  private static final String METADATA_JSON = "/metadata/v5.metadata.json";
  private static final long SNAPSHOT_ID = 5668393571500528520L;

  private static final String INCONSISTENT_PARTITION_METADATA_JSON =
      "/metadata/00000-eb45c383-9e13-43bd-882f-1ccad26fb644.metadata.json";
  private static final long INCONSISTENT_PARTITION_SNAPSHOT_ID = 2458326060911372002L;

  private static IcebergTestTables.Table table;
  private static IcebergTestTables.Table inconsistentPartitionTable;

  private OperatorContextImpl context;
  private FileSystem fs;

  private Configuration conf;

  @Mock(extraInterfaces = {MutablePlugin.class})
  private SupportsIcebergRootPointer plugin;

  @BeforeClass
  public static void initTables() {
    table = IcebergTestTables.V2_MULTI_ROWGROUP_ORDERS_WITH_DELETES.get();
    inconsistentPartitionTable = IcebergTestTables.INCONSISTENT_PARTITIONS.get();
  }

  @AfterClass
  public static void closeTables() throws Exception {
    AutoCloseables.close(table, inconsistentPartitionTable);
  }

  @Before
  public void beforeTest() throws Exception {
    context = testContext.getNewOperatorContext(getTestAllocator(), null, DEFAULT_BATCH_SIZE, null);
    testCloseables.add(context);
    conf = new Configuration();
    fs = HadoopFileSystem.get(Path.of("/"), conf, context.getStats());

    when(plugin.createFSWithAsyncOptions(anyString(), anyString(), any())).thenReturn(fs);
    when(plugin.getFsConfCopy()).thenReturn(conf);
    when(plugin.createIcebergFileIO(any(), any(), any(), any(), any()))
        .thenReturn(
            new DremioFileIO(
                fs, null, null, null, null, new HadoopFileSystemConfigurationAdapter(conf)));
    when(plugin.loadTableMetadata(any(), any(), any(), any()))
        .thenAnswer(
            (Answer<TableMetadata>)
                invocation -> {
                  Object[] args = invocation.getArguments();
                  return IcebergUtils.loadTableMetadata(
                      (FileIO) args[0], (OperatorContext) args[1], (String) args[3]);
                });
  }

  @Test
  public void testReadDataManifests() throws Exception {
    readAndValidate(
        table.getLocation() + METADATA_JSON,
        SNAPSHOT_ID,
        ManifestContentType.DATA,
        ImmutableList.of(
            table.getLocation() + "/metadata/8a83125a-a077-4f1e-974b-fcbaf370b085-m0.avro"));
  }

  @Test
  public void testReadDeleteManifests() throws Exception {
    readAndValidate(
        table.getLocation() + METADATA_JSON,
        SNAPSHOT_ID,
        ManifestContentType.DELETES,
        ImmutableList.of(
            table.getLocation() + "/metadata/07fe993a-9195-4cbc-bf9a-6b81816b9758-m0.avro",
            table.getLocation() + "/metadata/d1e51173-03f4-4b54-865a-c6c3185a92a5-m0.avro",
            table.getLocation() + "/metadata/d45e915a-acf8-4914-9907-0772d5356e4a-m0.avro"));
  }

  @Test
  public void testInconsistentPartitionDepth() throws Exception {
    readAndValidate(
        inconsistentPartitionTable.getLocation() + INCONSISTENT_PARTITION_METADATA_JSON,
        INCONSISTENT_PARTITION_SNAPSHOT_ID,
        ManifestContentType.DATA,
        ImmutableList.of(
            inconsistentPartitionTable.getLocation()
                + "/metadata/b2191648-8392-47e5-bb99-410a2d7b2181.avro",
            inconsistentPartitionTable.getLocation()
                + "/metadata/83af96e4-1754-4824-919b-7e8372a0deda.avro"),
        true,
        null,
        // [(not_null(ref(name="dir1")) and ref(name="dir1") == "sub_1_1")]
        Expressions.and(Expressions.notNull("dir1"), Expressions.equal("dir1", "sub_1_1")));
  }

  @Test
  public void testReadDataManifestsWithScheme() throws Exception {
    readAndValidate(
        table.getLocation() + METADATA_JSON,
        SNAPSHOT_ID,
        ManifestContentType.DATA,
        ImmutableList.of(
            "file"
                + SCHEME_SEPARATOR
                + table.getLocation()
                + "/metadata/8a83125a-a077-4f1e-974b-fcbaf370b085-m0.avro"),
        false,
        IcebergUtils.getDefaultPathScheme(fs.getScheme(), conf),
        Expressions.alwaysTrue());
  }

  private void readAndValidate(
      String jsonPath,
      long snapshotId,
      ManifestContentType manifestContent,
      List<String> expectedManifestFiles)
      throws Exception {
    readAndValidate(
        jsonPath,
        snapshotId,
        manifestContent,
        expectedManifestFiles,
        false,
        null,
        Expressions.alwaysTrue());
  }

  private void readAndValidate(
      String jsonPath,
      long snapshotId,
      ManifestContentType manifestContent,
      List<String> expectedManifestFiles,
      boolean isInternalIcebergScanTableMetadata,
      String schemeVariate,
      Expression icebergFilterExpression)
      throws Exception {
    List<String> actual = new ArrayList<>();
    IcebergExtendedProp extendedProp =
        new IcebergExtendedProp(
            null, IcebergSerDe.serializeToByteArray(icebergFilterExpression), snapshotId, null);

    IcebergManifestListRecordReader reader =
        new IcebergManifestListRecordReader(
            context,
            jsonPath,
            plugin,
            ImmutableList.of("table"),
            null,
            SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            PROPS,
            null,
            extendedProp,
            manifestContent,
            isInternalIcebergScanTableMetadata,
            schemeVariate);
    testCloseables.add(reader);

    TestOutputMutator outputMutator = new TestOutputMutator(getTestAllocator());
    testCloseables.add(outputMutator);
    // don't materialize projected columns like row num here to simulate how it works in the
    // production stack
    SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA.materializeVectors(
        ImmutableList.of(
            BasePath.getSimple(SystemSchemas.SPLIT_IDENTITY),
            BasePath.getSimple(SystemSchemas.SPLIT_INFORMATION),
            BasePath.getSimple(SystemSchemas.COL_IDS)),
        outputMutator);
    reader.setup(outputMutator);
    reader.allocate(outputMutator.getFieldVectorMap());

    int records;
    StructVector splitIdentityVector =
        (StructVector) outputMutator.getVector(SystemSchemas.SPLIT_IDENTITY);
    VarCharVector pathVector = (VarCharVector) splitIdentityVector.getChild(SplitIdentity.PATH);
    while ((records = reader.next()) > 0) {
      for (int i = 0; i < records; i++) {
        actual.add(new String(pathVector.get(i), StandardCharsets.UTF_8));
      }
    }

    assertThat(actual).isEqualTo(expectedManifestFiles);
  }
}
