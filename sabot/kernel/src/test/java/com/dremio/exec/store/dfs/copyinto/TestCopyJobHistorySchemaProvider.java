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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestCopyJobHistorySchemaProvider {

  @ParameterizedTest
  @ValueSource(ints = {1})
  public void testGetSchema(int schemaVersion) {
    Schema schema = CopyJobHistoryTableSchemaProvider.getSchema(schemaVersion);
    if (schemaVersion == 1) {
      // Test for schema version 1
      assertThat(schema.columns().size()).isEqualTo(10);
      assertThat(Types.TimestampType.withZone()).isEqualTo(schema.findField(1).type());
      assertThat(schema.findField(1).name()).isEqualTo("executed_at");
      assertThat(new Types.StringType()).isEqualTo(schema.findField(2).type());
      assertThat(schema.findField(2).name()).isEqualTo("job_id");
      assertThat(new Types.StringType()).isEqualTo(schema.findField(3).type());
      assertThat(schema.findField(3).name()).isEqualTo("table_name");
      assertThat(new Types.LongType()).isEqualTo(schema.findField(4).type());
      assertThat(schema.findField(4).name()).isEqualTo("records_loaded_count");
      assertThat(new Types.LongType()).isEqualTo(schema.findField(5).type());
      assertThat(schema.findField(5).name()).isEqualTo("records_rejected_count");
      assertThat(new Types.StringType()).isEqualTo(schema.findField(6).type());
      assertThat(schema.findField(6).name()).isEqualTo("copy_options");
      assertThat(new Types.StringType()).isEqualTo(schema.findField(7).type());
      assertThat(schema.findField(7).name()).isEqualTo("user_name");
      assertThat(new Types.LongType()).isEqualTo(schema.findField(8).type());
      assertThat(schema.findField(8).name()).isEqualTo("base_snapshot_id");
      assertThat(new Types.StringType()).isEqualTo(schema.findField(9).type());
      assertThat(schema.findField(9).name()).isEqualTo("storage_location");
      assertThat(new Types.StringType()).isEqualTo(schema.findField(10).type());
      assertThat(schema.findField(10).name()).isEqualTo("file_format");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2})
  public void testGetUnsupportedVersion(int unsupportedSchemaVersion) {
    assertThrows("Unsupported copy_job_history table schema version. Currently supported schema version are: 1",
      UnsupportedOperationException.class, () -> CopyJobHistoryTableSchemaProvider.getSchema(unsupportedSchemaVersion));
  }

  @ParameterizedTest
  @ValueSource(ints = {1})
  public void testGetUserNameCol(int schemaVersion) {
    String userNameColName = CopyJobHistoryTableSchemaProvider.getUserNameColName(schemaVersion);
    if (schemaVersion == 1) {
      assertThat(userNameColName).isEqualTo("user_name");
    }
  }
}
