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

package com.dremio.exec;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITHiveProjectPushDownV2 extends ITHiveProjectPushDown {

  private static AutoCloseable icebergEnabled;

  @BeforeClass
  public static void enableUnlimitedSplitSupport() {
    queryPlanKeyword = "IcebergManifestList(table=[";
    icebergEnabled = enableUnlimitedSplitsSupportFlags();
  }

  @AfterClass
  public static void disableUnlimitedSplitSupport() throws Exception {
    queryPlanKeyword = "mode=[NATIVE_PARQUET]";
    icebergEnabled.close();
  }

  @Override
  @Test
  public void projectPushDownOnHiveParquetTable() throws Exception {
    String query = "SELECT boolean_field, boolean_part, int_field, int_part FROM hive.readtest_parquet";
    testPhysicalPlan(query, expectedColumnsProjectionString("boolean_field", "boolean_part", "int_field", "int_part"), queryPlanKeyword);
    //TODO Include verification of data returned after DX-34840
    //testHelper(query, 2, expectedColumnsProjectionString("boolean_field", "int_field", "boolean_part", "int_part"), queryPlanKeyword);
  }

  protected static String expectedColumnsProjectionString(String ...expectedColumns) {
    List<String> sortedColumns = Arrays.asList(expectedColumns);
    Collections.sort(sortedColumns);
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    for (int i = 0; i < sortedColumns.size(); i++) {
      sb.append(sortedColumns.get(i)).append("=[$").append(i).append("]");
      if (i < expectedColumns.length - 1) {
        sb.append(", ");
      }
    }
    sb.append(")");
    return sb.toString();
  }

}
