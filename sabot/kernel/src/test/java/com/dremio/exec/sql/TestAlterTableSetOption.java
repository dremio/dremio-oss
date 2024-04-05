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
package com.dremio.exec.sql;

import com.dremio.BaseTestQuery;
import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.TemporarySystemProperties;
import com.dremio.test.UserExceptionAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestAlterTableSetOption extends BaseTestQuery {

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void setup() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @Test
  public void oldCmd() throws Exception {
    errorMsgTestHelper(
        "ALTER TABLE tbl ENABLE HIVE VARCHAR COMPATIBILITY", "Encountered \"ENABLE HIVE\"");
  }

  @Test
  public void badSql() {
    String[] queries = {
      "ALTER SESSION tbl SET hive.parquet.enforce_varchar_width = ON",
    };
    for (String q : queries) {
      UserExceptionAssert.assertThatThrownBy(() -> test(q))
          .hasErrorType(UserBitShared.DremioPBError.ErrorType.PARSE);
    }
  }

  @Test
  public void testOnNonTable() throws Exception {
    // sys table
    errorMsgTestHelper(
        "ALTER TABLE INFORMATION_SCHEMA.CATALOGS set INFORMATION_SCHEMA.CATALOG_NAME = 34",
        "Source [INFORMATION_SCHEMA] doesn't support modifying options");
  }
}
