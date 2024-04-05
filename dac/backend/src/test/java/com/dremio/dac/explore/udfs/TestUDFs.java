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
package com.dremio.dac.explore.udfs;

import static com.dremio.dac.server.JobsServiceTestUtils.submitJobAndGetData;
import static java.util.Arrays.asList;

import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** test UDFs */
public class TestUDFs extends BaseTestServer {

  private BufferAllocator allocator;

  @Before
  public void setUp() {
    allocator =
        getSabotContext().getAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
  }

  @After
  public void cleanUp() {
    allocator.close();
  }

  private JobDataFragment runQueryAndGetResults(String sql) throws JobNotFoundException {
    return submitJobAndGetData(
        l(JobsService.class),
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(sql, ImmutableList.of("cp"), DEFAULT_USERNAME))
            .build(),
        0,
        500,
        allocator);
  }

  @Test
  public void testExtractListSingle0() throws Exception {
    validate(
        "select a[0] as a from cp.\"json/extract_list.json\"", "a", "Shopping", "Bars", "Bars");
  }

  @Test
  public void testExtractListSingle4() throws Exception {
    validate("select a[4] as a from cp.\"json/extract_list.json\"", "a", null, "Restaurants", null);
  }

  private void validate(String sql, String col, String... values) throws Exception {
    try (final JobDataFragment result = runQueryAndGetResults(sql)) {
      List<String> actual = new ArrayList<>();
      for (int i = 0; i < result.getReturnedRowCount(); i++) {
        actual.add(result.extractString(col, i));
      }
      Assert.assertEquals(asList(values), actual);
    }
  }

  @Test
  @Ignore("flakey")
  public void testExtractListRange4() throws Exception {
    try (final JobDataFragment result =
        runQueryAndGetResults(
            "select extract_list_range(a, 2, 1, 3, -1)['root'] as a from cp.\"json/extract_list.json\"")) {
      String column = "a";
      Assert.assertEquals(result.getReturnedRowCount(), 3);
      Assert.assertTrue(
          result.extractString(column, 0).equals("[]")
              || result.extractString(column, 0).equals("null"));
      Assert.assertEquals("[\"Nightlife\"]", result.extractString(column, 1));
      Assert.assertTrue(
          result.extractString(column, 0).equals("[]")
              || result.extractString(column, 0).equals("null"));
    }
  }

  @Test
  public void testTitleCase() throws Exception {
    validate(
        "select title(a) as a from cp.\"json/convert_case.json\"",
        "a",
        "Los Angeles",
        "Los Angeles",
        "Los Angeles",
        "Los Angeles",
        "Los Angeles",
        "Washington",
        "Washington");
  }

  @Test
  public void testExtractMap() throws Exception {
    String extracted = "{\"close\":\"19:00\",\"open\":\"10:00\"}";
    validate(
        "SELECT map_table.a.Tuesday as t " + "FROM cp.\"json/extract_map.json\" map_table",
        "t",
        extracted,
        extracted,
        extracted);
  }

  @Test
  public void testExtractMapNested() throws Exception {
    String extracted = "19:00";
    validate(
        "SELECT map_table.a.Tuesday.\"close\" as t "
            + "FROM cp.\"json/extract_map.json\" map_table",
        "t",
        extracted,
        extracted,
        extracted);
  }
}
