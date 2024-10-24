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

import com.dremio.exec.ExecConstants;
import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Test;

public class TestCopyErrorsHistoryViewSchemaProvider {

  @Test
  public void testGetSchema() {
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    for (long i = 1; i <= schemaVersion; i++) {
      Schema schema = new CopyErrorsHistoryViewSchemaProvider(i).getSchema();
      if (i <= 3) {
        assertThat(schema.columns().size()).isEqualTo(10);
      }
    }
  }

  @Test
  public void testGetUnsupportedVersion() {
    long schemaVersion =
        ExecConstants.SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION.getDefault().getNumVal();
    assertThrows(
        "Unsupported copy_errors_history view schema version",
        UnsupportedOperationException.class,
        () -> new CopyErrorsHistoryViewSchemaProvider(schemaVersion + 1));
  }
}
