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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.op.scan.ScanOperator;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests Hive File Format stat in profile
 */
public class ITHiveFileFormatInProfile extends HiveStatsTestBase {

  @Test
  public void testParquetFileFormat() throws Exception {
    final String query = "select * from hive.parquet_region";
    runQueryAndVerifyFileFormat(query, (1 << ScanOperator.HiveFileFormat.PARQUET.ordinal()));
  }

  @Test
  public void testORCFileFormat() throws Exception {
    final String query = "select * from hive.orc_region";
    runQueryAndVerifyFileFormat(query, (1 << ScanOperator.HiveFileFormat.ORC.ordinal()));
  }

  @Test
  public void testAvroFileFormat() throws Exception {
    final String query = "select * from hive.db1.avro";
    runQueryAndVerifyFileFormat(query, (1 << ScanOperator.HiveFileFormat.AVRO.ordinal()));
  }

  @Test
  public void testTextFileFormat() throws Exception {
    final String query = "select * from hive.text_date";
    runQueryAndVerifyFileFormat(query, (1 << ScanOperator.HiveFileFormat.TEXT.ordinal()));
  }

  @Test
  public void testRcFileFormat() throws Exception {
    final String query = "select * from hive.skipper.kv_rcfile_large";
    runQueryAndVerifyFileFormat(query, (1 << ScanOperator.HiveFileFormat.RCFILE.ordinal()));
  }

  private void runQueryAndVerifyFileFormat(final String query, long expectedValue) throws Exception {
    final UserBitShared.OperatorProfile hiveReaderProfile = getHiveReaderProfile(query);
    final long numMetricValue = getMetricValue(hiveReaderProfile, ScanOperator.Metric.HIVE_FILE_FORMATS);
    Assert.assertEquals(expectedValue, numMetricValue);
  }
}
