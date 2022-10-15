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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;

/**
 * Enables ENABLE_ICEBERG_ADVANCED_DML for a local filesystem-based Hadoop source.
 */
public class ITDmlQueryBase extends BaseTestQuery {

  @BeforeClass
  public static void beforeClass() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_JOINED_TABLE, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_MERGE_STAR, "true");
  }

  @AfterClass
  public static void afterClass() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML,
      ExecConstants.ENABLE_ICEBERG_ADVANCED_DML.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_JOINED_TABLE,
      ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_JOINED_TABLE.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_MERGE_STAR,
      ExecConstants.ENABLE_ICEBERG_ADVANCED_DML_MERGE_STAR.getDefault().getBoolVal().toString());
  }

  @Before
  public void before() throws Exception {
    // Note: dfs_hadoop is immutable.
    test("USE dfs_hadoop");
  }
}
