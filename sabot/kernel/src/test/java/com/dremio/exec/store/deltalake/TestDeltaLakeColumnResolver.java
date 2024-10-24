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

package com.dremio.exec.store.deltalake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.BaseTestQuery;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.parquet.ParquetColumnDeltaLakeResolver;
import java.util.Collections;
import org.junit.Test;

public class TestDeltaLakeColumnResolver extends BaseTestQuery {
  private BatchSchema loadSchemaFromCommitJson(String tableName) throws Exception {
    DeltaLogSnapshot snapshot =
        TestDeltaLogCommitJsonReader.parseCommitJson(
            "/deltalake/" + tableName + "/_delta_log/00000000000000000000.json");
    String schemaString = snapshot.getSchema();
    return DeltaLakeSchemaConverter.newBuilder()
        .withMapEnabled(true)
        .withColumnMapping(snapshot.getColumnMappingMode())
        .build()
        .fromSchemaString(schemaString);
  }

  @Test
  public void testSimpleSchema() throws Exception {
    // c_int: int
    // c_str: string
    BatchSchema batchSchema = loadSchemaFromCommitJson("columnMapping");
    assertTrue(ParquetColumnDeltaLakeResolver.hasColumnMapping(batchSchema));

    ParquetColumnDeltaLakeResolver resolver =
        new ParquetColumnDeltaLakeResolver(Collections.emptyList(), batchSchema, null);
    assertEquals(
        "col-222c7cf9-47f8-4153-8a34-8b504331d852", resolver.getParquetColumnName("c_int"));
    assertEquals(
        "col-78b58c50-2fc9-4cc8-ac3e-e1c6b1d006a9", resolver.getParquetColumnName("c_str"));
    assertEquals(
        "c_int", resolver.getBatchSchemaColumnName("col-222c7cf9-47f8-4153-8a34-8b504331d852"));
    assertEquals(
        "c_str", resolver.getBatchSchemaColumnName("col-78b58c50-2fc9-4cc8-ac3e-e1c6b1d006a9"));
  }

  @Test
  public void testComplexTypesSchema() throws Exception {
    // id:       int
    // c_array:  array<string>
    // c_map:    map<string,int>
    // c_struct: struct<i:int,s:string>
    BatchSchema batchSchema = loadSchemaFromCommitJson("columnMappingComplexTypes");
    assertTrue(ParquetColumnDeltaLakeResolver.hasColumnMapping(batchSchema));

    ParquetColumnDeltaLakeResolver resolver =
        new ParquetColumnDeltaLakeResolver(Collections.emptyList(), batchSchema, null);
    assertEquals("col-9ea0f164-86ea-4862-be27-becce5bec908", resolver.getParquetColumnName("id"));
    assertEquals(
        "col-6a1cbf5a-b017-4ec3-b610-60ea048e1ea1", resolver.getParquetColumnName("c_array"));
    assertEquals(
        "col-3acba239-44fb-454c-be60-bfb27b478625", resolver.getParquetColumnName("c_map"));
    assertEquals(
        "col-3ea47a86-00a4-44c2-8f54-57855bfea0ce", resolver.getParquetColumnName("c_struct"));

    assertEquals(
        "id", resolver.getBatchSchemaColumnName("col-9ea0f164-86ea-4862-be27-becce5bec908"));
    assertEquals(
        "c_array", resolver.getBatchSchemaColumnName("col-6a1cbf5a-b017-4ec3-b610-60ea048e1ea1"));
    assertEquals(
        "c_array.list.element",
        resolver.getBatchSchemaColumnName("col-6a1cbf5a-b017-4ec3-b610-60ea048e1ea1.list.element"));
    assertEquals(
        "c_map", resolver.getBatchSchemaColumnName("col-3acba239-44fb-454c-be60-bfb27b478625"));
    assertEquals(
        "c_map.entries",
        resolver.getBatchSchemaColumnName("col-3acba239-44fb-454c-be60-bfb27b478625.key_value"));
    assertEquals(
        "c_map.entries.key",
        resolver.getBatchSchemaColumnName(
            "col-3acba239-44fb-454c-be60-bfb27b478625.key_value.key"));
    assertEquals(
        "c_map.entries.value",
        resolver.getBatchSchemaColumnName(
            "col-3acba239-44fb-454c-be60-bfb27b478625.key_value.value"));
    assertEquals(
        "c_struct", resolver.getBatchSchemaColumnName("col-3ea47a86-00a4-44c2-8f54-57855bfea0ce"));
    assertEquals(
        "c_struct.i",
        resolver.getBatchSchemaColumnName(
            "col-3ea47a86-00a4-44c2-8f54-57855bfea0ce.col-4483f2d6-7aee-4ccb-a0ea-5c1a9e2fbaa9"));
    assertEquals(
        "c_struct.s",
        resolver.getBatchSchemaColumnName(
            "col-3ea47a86-00a4-44c2-8f54-57855bfea0ce.col-5098abb0-b7e0-416b-a9f7-304220e69ce0"));
    assertEquals(
        "i", resolver.getBatchSchemaColumnName("col-4483f2d6-7aee-4ccb-a0ea-5c1a9e2fbaa9"));
    assertEquals(
        "s", resolver.getBatchSchemaColumnName("col-5098abb0-b7e0-416b-a9f7-304220e69ce0"));
  }

  @Test
  public void testNestedTypesSchema() throws Exception {
    // id:       int
    // c_array:  array<map<string,int>>
    // c_map:    map<string,int>
    // c_struct: array<struct<i:int,s:string>>
    BatchSchema batchSchema = loadSchemaFromCommitJson("columnMappingNestedTypes");
    assertTrue(ParquetColumnDeltaLakeResolver.hasColumnMapping(batchSchema));

    ParquetColumnDeltaLakeResolver resolver =
        new ParquetColumnDeltaLakeResolver(Collections.emptyList(), batchSchema, null);
    assertEquals("col-d134036f-a15b-40b3-8345-8ce9a86dc6c5", resolver.getParquetColumnName("id"));
    assertEquals(
        "col-cfc8907e-b152-4685-b348-dc1f1c70bb27", resolver.getParquetColumnName("c_array"));
    assertEquals(
        "col-c350e179-5aca-43ef-80cd-8f86b5aeb3f0", resolver.getParquetColumnName("c_map"));
    assertEquals(
        "col-63fffbb9-55b1-4f1f-930f-bbb115031f62", resolver.getParquetColumnName("c_struct"));

    assertEquals(
        "id", resolver.getBatchSchemaColumnName("col-d134036f-a15b-40b3-8345-8ce9a86dc6c5"));
    assertEquals(
        "c_array", resolver.getBatchSchemaColumnName("col-cfc8907e-b152-4685-b348-dc1f1c70bb27"));
    assertEquals(
        "c_array.list.element",
        resolver.getBatchSchemaColumnName("col-cfc8907e-b152-4685-b348-dc1f1c70bb27.list.element"));
    assertEquals(
        "c_array.list.element.entries",
        resolver.getBatchSchemaColumnName(
            "col-cfc8907e-b152-4685-b348-dc1f1c70bb27.list.element.key_value"));
    assertEquals(
        "c_array.list.element.entries.key",
        resolver.getBatchSchemaColumnName(
            "col-cfc8907e-b152-4685-b348-dc1f1c70bb27.list.element.key_value.key"));
    assertEquals(
        "c_array.list.element.entries.value",
        resolver.getBatchSchemaColumnName(
            "col-cfc8907e-b152-4685-b348-dc1f1c70bb27.list.element.key_value.value"));
    assertEquals(
        "c_map", resolver.getBatchSchemaColumnName("col-c350e179-5aca-43ef-80cd-8f86b5aeb3f0"));
    assertEquals(
        "c_map.entries",
        resolver.getBatchSchemaColumnName("col-c350e179-5aca-43ef-80cd-8f86b5aeb3f0.key_value"));
    assertEquals(
        "c_map.entries.key",
        resolver.getBatchSchemaColumnName(
            "col-c350e179-5aca-43ef-80cd-8f86b5aeb3f0.key_value.key"));
    assertEquals(
        "c_map.entries.value",
        resolver.getBatchSchemaColumnName(
            "col-c350e179-5aca-43ef-80cd-8f86b5aeb3f0.key_value.value"));
    assertEquals(
        "c_struct", resolver.getBatchSchemaColumnName("col-63fffbb9-55b1-4f1f-930f-bbb115031f62"));
    assertEquals(
        "c_struct.list.element",
        resolver.getBatchSchemaColumnName("col-63fffbb9-55b1-4f1f-930f-bbb115031f62.list.element"));
    assertEquals(
        "c_struct.list.element.i",
        resolver.getBatchSchemaColumnName(
            "col-63fffbb9-55b1-4f1f-930f-bbb115031f62.list.element.col-803266e8-7e28-41a0-b8f0-9c6f8bbf617a"));
    assertEquals(
        "c_struct.list.element.s",
        resolver.getBatchSchemaColumnName(
            "col-63fffbb9-55b1-4f1f-930f-bbb115031f62.list.element.col-da9a198d-a2c8-4d3e-9a80-7df4285ac291"));
    assertEquals(
        "i", resolver.getBatchSchemaColumnName("col-803266e8-7e28-41a0-b8f0-9c6f8bbf617a"));
    assertEquals(
        "s", resolver.getBatchSchemaColumnName("col-da9a198d-a2c8-4d3e-9a80-7df4285ac291"));
  }
}
