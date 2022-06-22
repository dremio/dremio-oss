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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_METADATA_FUNCTIONS;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.BaseTable;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;

/**
 * Abstract class for Iceberg Metadata functions tests
 */
public abstract class IcebergMetadataTestTable extends BaseIcebergTable {

  protected static BaseTable table;

  @BeforeClass
  public static void initIcebergTable() throws Exception {
    createIcebergTable();
    setSystemOption(ENABLE_ICEBERG_METADATA_FUNCTIONS, "true");
    table = new BaseTable(ops, tableName);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    setSystemOption(ENABLE_ICEBERG_METADATA_FUNCTIONS, "false");
  }

  protected void expectedSchema(List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema, String query, Object... args) throws Exception {
    testBuilder()
      .sqlQuery(query, args)
      .schemaBaseLine(expectedSchema)
      .build()
      .run();
  }

}
