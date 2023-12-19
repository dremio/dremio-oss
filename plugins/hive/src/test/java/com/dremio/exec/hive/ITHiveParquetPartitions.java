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
package com.dremio.exec.hive;

import org.apache.hadoop.hive.ql.Driver;
import org.joda.time.LocalDateTime;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITHiveParquetPartitions extends LazyDataGeneratingHiveTestBase {

  @BeforeClass
  public static void setup() throws Exception {
    copyFromJar("parquet/invalid_partition", java.nio.file.Paths.get(dataGenerator.getWhDir() + "/invalid_partition"));
    dataGenerator.generateTestData(hiveDriver -> generateTestData(hiveDriver, dataGenerator.getWhDir()));
  }

  private static Void generateTestData(Driver hiveDriver, String whDir) {
    String createTable = "CREATE EXTERNAL TABLE invalid_partition(s_col STRING, i_col INT) " +
      "PARTITIONED BY (d_col DATE) STORED AS PARQUET LOCATION 'file://" + whDir + "/invalid_partition'";
    HiveTestUtilities.executeQuery(hiveDriver, createTable);

    String recoverTable = "MSCK REPAIR TABLE invalid_partition";
    HiveTestUtilities.executeQuery(hiveDriver, recoverTable);

    return null;
  }

  @Test
  public void testInvalidPartitionSelect() throws Exception {
    String query = "SELECT * FROM hive.invalid_partition";
    testBuilder().sqlQuery(query)
      .unOrdered()
      .baselineColumns("s_col", "i_col", "d_col")
      .baselineValues("a", 1, LocalDateTime.parse("2022-12-28"))
      .baselineValues("b", 2, null)
      .go();
  }
}
