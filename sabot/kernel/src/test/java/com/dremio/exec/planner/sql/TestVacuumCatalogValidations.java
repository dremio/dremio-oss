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
package com.dremio.exec.planner.sql;

import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;

/**
 * Validations on `VACUUM CATALOG` sql command
 */
public class TestVacuumCatalogValidations extends BaseTestQuery {

  @Test
  public void testFeatureNotEnabled() {
    assertThatThrownBy(() -> runSQL(String.format("VACUUM CATALOG %s", TEMP_SCHEMA_HADOOP)))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("UNSUPPORTED_OPERATION ERROR: VACUUM CATALOG command is not supported.");
  }

  @Test
  public void testInvalidPath() throws Exception {
    try (AutoCloseable c = enableVacuumCatalog();
         DmlQueryTestUtils.Table table = createBasicTable(TEMP_SCHEMA_HADOOP,2, 1)) {
      assertThatThrownBy(() -> runSQL(String.format("VACUUM CATALOG %s.%s", TEMP_SCHEMA_HADOOP, table.fqn)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("PARSE ERROR: Catalog name cannot have multiple path components.");
    }
  }

  @Test
  public void testAssertSourceType() throws Exception {
    try (AutoCloseable c = enableVacuumCatalog()) {
      assertThatThrownBy(() -> runSQL(String.format("VACUUM CATALOG %s", TEMP_SCHEMA_HADOOP)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("UNSUPPORTED_OPERATION ERROR: VACUUM CATALOG is supported only on versioned sources.");
    }
  }

  private static AutoCloseable enableVacuumCatalog() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM_CATALOG, "true");
    return () ->
      setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM_CATALOG,
        ExecConstants.ENABLE_ICEBERG_VACUUM_CATALOG.getDefault().getBoolVal().toString());
  }
}
