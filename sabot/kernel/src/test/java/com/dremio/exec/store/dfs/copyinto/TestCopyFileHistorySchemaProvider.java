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

public class TestCopyFileHistorySchemaProvider {

  @ParameterizedTest
  @ValueSource(ints = {1})
  public void testGetSchema(int schemaVersion) {
    Schema schema = CopyFileHistoryTableSchemaProvider.getSchema(schemaVersion);
    if (schemaVersion == 1) {
      // Test for schema version 1
      assertThat(schema.columns().size()).isEqualTo(6);
      assertThat(schema.findField(1).type()).isEqualTo(Types.TimestampType.withZone());
      assertThat(schema.findField(1).name()).isEqualTo("event_timestamp");
      assertThat(schema.findField(2).type()).isEqualTo(new Types.StringType());
      assertThat(schema.findField(2).name()).isEqualTo("job_id");
      assertThat(schema.findField(3).type()).isEqualTo(new Types.StringType());
      assertThat(schema.findField(3).name()).isEqualTo("file_path");
      assertThat(schema.findField(4).type()).isEqualTo(new Types.StringType());
      assertThat(schema.findField(4).name()).isEqualTo("file_state");
      assertThat(schema.findField(5).type()).isEqualTo(new Types.LongType());
      assertThat(schema.findField(5).name()).isEqualTo("records_loaded_count");
      assertThat(schema.findField(6).type()).isEqualTo(new Types.LongType());
      assertThat(schema.findField(6).name()).isEqualTo("records_rejected_count");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2})
  public void testGetUnsupportedVersion(int unsupportedSchemaVersion) {
    assertThrows("Unsupported copy_file_history table schema version. Currently supported schema version are: 1",
      UnsupportedOperationException.class, () -> CopyFileHistoryTableSchemaProvider.getSchema(unsupportedSchemaVersion));
  }
}
