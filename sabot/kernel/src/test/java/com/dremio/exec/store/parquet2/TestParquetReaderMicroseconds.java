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
package com.dremio.exec.store.parquet2;

import static com.dremio.exec.ExecConstants.PARQUET_READER_VECTORIZE;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.joda.time.format.DateTimeFormatter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.JodaDateUtility;
import com.dremio.common.util.TestTools;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Test reading TIMESTAMP and TIME MICROSECOND fields in parquet
 * using the Rowwise reader. This test uses a file with mixed datatypes.
 */
public class TestParquetReaderMicroseconds extends BaseTestQuery {
  private static final String WORKING_PATH = TestTools.getWorkingPath();

  @BeforeClass
  public static void setUp() {
    setSystemOption(PARQUET_READER_VECTORIZE.getOptionName(), "false");
  }

  @AfterClass
  public static void tearDown() {
    resetSystemOption(PARQUET_READER_VECTORIZE.getOptionName());
  }


  @Test
  public void testRowwiseMicroseconds() throws Exception {
    /*
     * Test microsecond time and timestamp support for the Rowise reader.
     * Rowise is used for files using RLE dictionary encoding.
     */

    final String parquetFiles = TestParquetUtil.setupParquetFiles("testRowiseMicroseconds", "rowise_microseconds", "rowise_micros.parquet", WORKING_PATH);
    try {

      DateTimeFormatter JODA_MILLIS_FORMATTER = JodaDateUtility.formatTimeStampMilli;
      {
        final String colName = "colDTMicro";
        final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
        recordBuilder.add(ImmutableMap.of("`" + colName + "`", JODA_MILLIS_FORMATTER.parseLocalDateTime("2023-01-23 12:13:14.567")));
        recordBuilder.add(ImmutableMap.of("`" + colName + "`", JODA_MILLIS_FORMATTER.parseLocalDateTime("2023-01-23 12:13:14.667")));
        final List<Map<String, Object>> baseLineRecords = recordBuilder.build();

        testBuilder()
          .sqlQuery("SELECT " + colName + " FROM dfs.\"" + parquetFiles + "\"")
          .unOrdered()
          .baselineColumns(colName)
          .baselineRecords(baseLineRecords)
          .build()
          .run();
      }
      {
        final String colName = "colTMicro32";
        final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
        recordBuilder.add(ImmutableMap.of("`" + colName + "`", JODA_MILLIS_FORMATTER.parseLocalDateTime("1970-01-01 12:13:14.123")));
        recordBuilder.add(ImmutableMap.of("`" + colName + "`", JODA_MILLIS_FORMATTER.parseLocalDateTime("1970-01-01 12:13:14.843")));
        final List<Map<String, Object>> baseLineRecords = recordBuilder.build();

        testBuilder()
          .sqlQuery("SELECT " + colName + " FROM dfs.\"" + parquetFiles + "\"")
          .unOrdered()
          .baselineColumns(colName)
          .baselineRecords(baseLineRecords)
          .build()
          .run();
      }
      {
        final String colName = "colTMicro";
        final ImmutableList.Builder<Map<String, Object>> recordBuilder = ImmutableList.builder();
        recordBuilder.add(ImmutableMap.of("`" + colName + "`", JODA_MILLIS_FORMATTER.parseLocalDateTime("1970-01-01 12:13:14.123")));
        recordBuilder.add(ImmutableMap.of("`" + colName + "`", JODA_MILLIS_FORMATTER.parseLocalDateTime("1970-01-01 12:13:14.843")));
        final List<Map<String, Object>> baseLineRecords = recordBuilder.build();

        testBuilder()
          .sqlQuery("SELECT " + colName + " FROM dfs.\"" + parquetFiles + "\"")
          .unOrdered()
          .baselineColumns(colName)
          .baselineRecords(baseLineRecords)
          .build()
          .run();
      }
    } finally {
      TestParquetUtil.delete(Paths.get(parquetFiles));
    }
  }
}
