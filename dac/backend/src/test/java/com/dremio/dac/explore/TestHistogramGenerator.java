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
package com.dremio.dac.explore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.dremio.dac.explore.HistogramGenerator.TruncEvalEnum;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.collect.ImmutableSet;

/**
 * Test class for Histogram Generation
 */
public class TestHistogramGenerator {

  @Test
  public void testTimeIntervals() throws Exception {
    HistogramGenerator hg = new HistogramGenerator(null);

    String myTimeStr = "2016-02-29 13:59:01";
    DateTimeFormatter dtf = DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD HH24:MI:SS");
    LocalDateTime myTime = dtf.parseLocalDateTime(myTimeStr);
    System.out.println("Exact time: " + myTime + ", Month: " + myTime.getMonthOfYear());
    for (HistogramGenerator.TruncEvalEnum value : HistogramGenerator.TruncEvalEnum.getSortedAscValues()) {
      LocalDateTime roundedDown = hg.roundTime(myTime, value, true);
      LocalDateTime roundedUp = hg.roundTime(myTime, value, false);
      switch(value) {
        case SECOND:
          assertEquals(myTime, roundedDown);
          assertEquals(myTime, roundedUp);
          break;
        case MINUTE:
          assertEquals(dtf.parseLocalDateTime("2016-02-29 13:59:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("2016-02-29 14:00:00"), roundedUp);
          break;
        case HOUR:
          assertEquals(dtf.parseLocalDateTime("2016-02-29 13:00:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("2016-02-29 14:00:00"), roundedUp);
          break;
        case DAY:
          assertEquals(dtf.parseLocalDateTime("2016-02-29 00:00:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("2016-03-01 00:00:00"), roundedUp);
          break;
        case WEEK:
          assertEquals(dtf.parseLocalDateTime("2016-02-29 00:00:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("2016-03-07 00:00:00"), roundedUp);
          break;
        case MONTH:
          assertEquals(dtf.parseLocalDateTime("2016-02-01 00:00:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("2016-03-01 00:00:00"), roundedUp);
          break;
        case QUARTER:
          assertEquals(dtf.parseLocalDateTime("2016-01-01 00:00:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("2016-04-01 00:00:00"), roundedUp);
          break;
        case YEAR:
          assertEquals(dtf.parseLocalDateTime("2016-01-01 00:00:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("2017-01-01 00:00:00"), roundedUp);
          break;
        case DECADE:
          assertEquals(dtf.parseLocalDateTime("2010-01-01 00:00:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("2020-01-01 00:00:00"), roundedUp);
          break;
        case CENTURY:
          assertEquals(dtf.parseLocalDateTime("2000-01-01 00:00:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("2100-01-01 00:00:00"), roundedUp);
          break;
        case MILLENNIUM:
          assertEquals(dtf.parseLocalDateTime("2000-01-01 00:00:00"), roundedDown);
          assertEquals(dtf.parseLocalDateTime("3000-01-01 00:00:00"), roundedUp);
          break;
        default:
          fail();
      }
    }
  }

  @Test
  public void testProduceRanges() {
    List<Number> ranges = new ArrayList<>();
    HistogramGenerator.produceRanges(ranges , new LocalDateTime(1970, 1, 1, 1, 0, 0), new LocalDateTime(1970, 1, 1, 11, 59, 0), TruncEvalEnum.HOUR);
    List<Number> expected = new ArrayList<>();
    for (int i = 0; i < 13; i++) {
      expected.add((i + 1 ) * 3600_000L);
    }
    Assert.assertEquals(expected.size(), ranges.size());
    Assert.assertEquals(expected, ranges);

  }

  @Test
  public void testSelectionCount() {
    testSelectionCountHelper("colName = 'val1'", 1562383L, DataType.TEXT, ImmutableSet.of("val1"));
    testSelectionCountHelper("colName = 'val1' OR colName = 'val2'", 1562L, DataType.TEXT, ImmutableSet.of("val1", "val2"));
    Set<String> selectedValues = new LinkedHashSet<>(Arrays.asList("val1", null));
    testSelectionCountHelper("colName = 'val1' OR colName IS NULL", 1562L, DataType.TEXT, selectedValues);
    testSelectionCountHelper("colName = 1 OR colName = 2", 15432L, DataType.INTEGER, ImmutableSet.of("1", "2"));
    testSelectionCountHelper("colName = DATE '2017-05-03' OR colName = DATE '2035-12-12'",
        23L, DataType.DATE, ImmutableSet.of("2017-05-03", "2035-12-12"));
    testSelectionCountHelper("colName = TIMESTAMP '2017-05-03 12:23:24' OR colName = TIMESTAMP '2035-12-12 05:23:23'",
        2L, DataType.DATETIME, ImmutableSet.of("2017-05-03 12:23:24", "2035-12-12 05:23:23"));
    testSelectionCountHelper("colName = TIME '12:23:24' OR colName = TIME '05:23:23'",
        6L, DataType.TIME, ImmutableSet.of("12:23:24", "05:23:23"));
    testSelectionCountHelper("colName = ''", 1562383L, DataType.TEXT, ImmutableSet.of(""));
    testSelectionCountHelper(null, 0L, DataType.INTEGER, ImmutableSet.<String>of());
  }

  private void testSelectionCountHelper(final String expFilter, final long expCount, DataType type, Set<String> selectedValues) {
    final DatasetPath datasetPath = new DatasetPath(Arrays.asList("dfs", "parquet", "lineitem.parquet"));
    final DatasetVersion datasetVersion = DatasetVersion.newVersion();

    final QueryExecutor queryExecutor = mock(QueryExecutor.class);
    when(queryExecutor.runQuery(any(SqlQuery.class), any(QueryType.class), any(DatasetPath.class), any(DatasetVersion.class)))
        .thenAnswer(
          (Answer<JobData>) invocation -> {
            String query = invocation.getArgumentAt(0, SqlQuery.class).getSql();
            com.dremio.service.jobs.JobData jobData = mock(com.dremio.service.jobs.JobData.class);
            if ("SELECT * FROM dataset".equals(query)) {
              when(jobData.getJobResultsTable()).thenReturn("jobResults.previewJob");
            } else if (query.contains("jobResults.previewJob")) {
              if (expFilter != null) {
                assertTrue(query, query.contains(expFilter));
              } else {
                assertFalse(query, query.contains("WHERE"));
              }
              JobDataFragment fragment = mock(JobDataFragment.class);
              when(jobData.truncate(1)).thenReturn(fragment);
              when(fragment.getSchema()).thenReturn(
                  BatchSchema.newBuilder()
                      .addField(new Field("dremio_selection_count", true, new ArrowType.Int(64, true), null))
                      .build()
              );
              when(fragment.extractValue("dremio_selection_count", 0)).thenReturn(expCount);
            }

            return new JobDataWrapper(jobData);
          }
        );

    HistogramGenerator hg = new HistogramGenerator(queryExecutor);
    long count = hg.getSelectionCount(datasetPath, datasetVersion,
        new SqlQuery("SELECT * FROM dataset", "user"),
        type, "colName", selectedValues
    );

    assertEquals(expCount, count);
  }
}
