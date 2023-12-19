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
package com.dremio.exec.store.dfs.copyinto;

import static com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.COPY_FILE_HISTORY_TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.IntStream;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadata;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
public class TestCopyFileHistoryTableMetadata {

  private static final String PLUGIN_NAME = "__testPlugin";
  private static final String PLUGIN_PATH = "/path/to/plugin/testPlugin";
  private SystemIcebergTableMetadata tableMetadata;
  @Parameterized.Parameter(0)
  public int schemaVersion;

  @Parameterized.Parameters(name = "schemaVersion={0}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = new ArrayList<>();
    // add more schema versions if needed
    testParams.add(new Object[]{1});
    return testParams;
  }

  @Before
  public void setup() {
    tableMetadata = new CopyFileHistoryTableMetadata(schemaVersion,
      CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion),
      PLUGIN_NAME,
      PLUGIN_PATH,
      COPY_FILE_HISTORY_TABLE_NAME);
  }

  @Test
  public void testGetTableName() {
    assertThat(tableMetadata.getTableName()).isEqualTo(COPY_FILE_HISTORY_TABLE_NAME);
  }

  @Test
  public void testGetIcebergSchema() {
    Schema schema = tableMetadata.getIcebergSchema();
    if (schemaVersion == 1) {
      assertThat(schema).isNotNull();
      assertThat(6).isEqualTo(schema.columns().size());
      assertThat(Types.TimestampType.withZone()).isEqualTo(schema.findField(1).type());
      assertThat(new Types.StringType()).isEqualTo(schema.findField(2).type());
      assertThat(new Types.StringType()).isEqualTo(schema.findField(3).type());
      assertThat(new Types.StringType()).isEqualTo(schema.findField(4).type());
      assertThat(new Types.LongType()).isEqualTo(schema.findField(5).type());
      assertThat(new Types.LongType()).isEqualTo(schema.findField(6).type());
    }
  }

  @Test
  public void testGetBatchSchema() {
    BatchSchema batchSchema = tableMetadata.getBatchSchema();
    Schema schema = tableMetadata.getIcebergSchema();
    assertThat(batchSchema).isNotNull();
    if (schemaVersion == 1) {
      assertThat(batchSchema.getFieldCount()).isEqualTo(6);
      assertThat(IntStream.range(0, batchSchema.getFieldCount())
        .allMatch(i -> batchSchema.getColumn(i).getName().equals(schema.findColumnName(i + 1)))).isTrue();
    }
  }

  @Test
  public void testGetTableLocation() {
    assertThat(tableMetadata.getTableLocation()).isEqualTo("/path/to/plugin/testPlugin/" + COPY_FILE_HISTORY_TABLE_NAME);
  }

  @Test
  public void testGetNamespaceKey() {
    NamespaceKey namespaceKey = tableMetadata.getNamespaceKey();
    assertThat(namespaceKey).isNotNull();
    assertThat(namespaceKey).isEqualTo(new NamespaceKey(ImmutableList.of(PLUGIN_NAME, tableMetadata.getTableName())));
  }

  @Test
  public void testGetSchemaVersion() {
    assertThat(tableMetadata.getSchemaVersion()).isEqualTo(schemaVersion);
  }

  @Test
  public void testGetIcebergTablePropsForCreate() {
    IcebergTableProps props = tableMetadata.getIcebergTablePropsForCreate();
    assertThat(props).isNotNull();
    assertThat(props.getIcebergOpType()).isEqualTo(IcebergCommandType.CREATE);
  }

  @Test
  public void testGetIcebergTablePropsForInsert() {
    IcebergTableProps props = tableMetadata.getIcebergTablePropsForInsert();
    assertThat(props).isNotNull();
    assertThat(props.getIcebergOpType()).isEqualTo(IcebergCommandType.INSERT);
  }
}
