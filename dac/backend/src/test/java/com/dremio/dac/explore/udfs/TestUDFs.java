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
import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.google.common.collect.ImmutableList;

/**
 * test UDFs
 */
public class TestUDFs extends BaseTestServer {

  private BufferAllocator allocator;

  @Before
  public void setUp() {
    allocator = getSabotContext().getAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
  }

  @After
  public void cleanUp() {
    allocator.close();
  }


  @Test
  public void testFormatList() throws Exception {
    String sql = String.format("select %s(b, ',') as a, b from cp.\"json/nested.json\"", FormatList.NAME);
    try (final JobDataFragment result = runQueryAndGetResults(sql)) {
      List<String> actual = new ArrayList<>();
      for(int i = 0; i < result.getReturnedRowCount(); i++){

        Object a = result.extractValue("a", i);
        Object b = result.extractValue("b", i);
        actual.add(String.format("%s => %s", b, a));
      }
      Assert.assertEquals(Arrays.asList(
        "[\"A\",\"B\",\"C\"] => A,B,C",
        "[\"D\"] => D",
        "[\"E\",\"F\"] => E,F",
        "[] => "
      ), actual);
    }
  }

  private JobDataFragment runQueryAndGetResults(String sql) throws JobNotFoundException {
    return submitJobAndGetData(l(JobsService.class),
      JobRequest.newBuilder().setSqlQuery(new SqlQuery(sql, ImmutableList.of("cp"), DEFAULT_USERNAME)).build(),
      0, 500, allocator);
  }

  @Test
  public void testFormatListWithWhereWithNull() throws Exception {
    String sql = String.format("select %s(b, ',') as a, b from cp.\"json/nested.json\" where b is null", FormatList
      .NAME);
    try (final JobDataFragment result = runQueryAndGetResults(sql)) {
      List<String> actual = new ArrayList<>();
      for(int i = 0; i < result.getReturnedRowCount(); i++){

        Object a = result.extractValue("a", i);
        Object b = result.extractValue("b", i);
        actual.add(String.format("%s => %s", b, a));
      }
      Assert.assertEquals(0, actual.size());
    }
  }

  @Test
  public void testUnionType() throws Exception {
    String sql = "select * from cp.\"json/mixed.json\"";
    try (final JobDataFragment result = runQueryAndGetResults(sql)) {
      List<String> actual = new ArrayList<>();
      for(int i =0; i < result.getReturnedRowCount(); i++){
        Object a = result.extractValue("a", i);
        Object b = result.extractValue("b", i);
        String type = result.extractType("a", i).name();
        actual.add(String.format("%s, %s:%s", b, a, type));
      }
      Assert.assertEquals(Arrays.asList(
        "123, abc:TEXT",
        "123, 123:INTEGER",
        "123.0, 0.123:FLOAT",
        "123, {\"foo\":\"bar\"}:MAP",
        "123, 123:INTEGER",
        "100.0, 0.123:FLOAT",
        "0.0, 0.123:FLOAT",
        "-123.0, 0.123:FLOAT",
        "0, 0.123:FLOAT"
      ), actual);
    }

  }

  private void validateCleanDataSingleField(String call, String col, String... expected) throws JobNotFoundException {
//    {"a": "abc", "b":"123" }
//    {"a": 123, "b":123 }
//    {"a": 0.123, "b":123.0 }
//    {"a": { "foo" : "bar"}, "b":123 }

    String sql = format("select %s as actual, %s from cp.\"json/mixed.json\"", call, col);
    try (final JobDataFragment result = runQueryAndGetResults(sql)) {
      Assert.assertEquals(expected.length, result.getReturnedRowCount());
      for (int i = 0; i < expected.length; i++) {
        String expRow = expected[i];
        String actual = result.extractString("actual", i);
        Assert.assertEquals(call + " on " + col + "=" + result.extractString(col, i) + " row:" + i, expRow, actual);
      }
    }
  }

  @Test
  public void testCleanDataBoolean() throws Exception {
    validateCleanDataSingleField("clean_data_to_Boolean(d, 1, 0, false)", "a",
      "true",
      "false",
      "true",
      "false",
      "true",
      "true",
      "true",
      "false",
      "false");
  }

  @Test
  public void testCleanDataDefault() throws Exception {
    validateCleanDataSingleField("clean_data_to_TEXT(a, 0, 0, 'blah')", "a",
        "abc",
        "blah",
        "blah",
        "blah",
        "blah",
        "blah",
        "blah",
        "blah",
        "blah");
  }

  @Test
  public void testCleanDataCast() throws Exception {
    validateCleanDataSingleField("clean_data_to_TEXT(a, 1, 1, '')", "a",
        "abc",
        "123",
        "0.123",
        "{\n  \"foo\" : \"bar\"\n}",
        "123",
        "0.123",
        "0.123",
        "0.123",
        "0.123");
  }

  @Test
  public void testCleanDataNull() throws Exception {
    validateCleanDataSingleField("clean_data_to_TEXT(a, 0, 1, '')", "a",
        "abc",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  @Test
  public void testIsCleanDataFloat() throws Exception {
    validateCleanDataSingleField("is_convertible_data(c, 1, cast('FLOAT' as VARCHAR))", "c",
        "true",
        "true",
        "true",
        "true",
        "true",
        "true",
        "true",
        "true",
        "true");
  }


  @Test
  public void testConvertToFloatScientificNotation() throws Exception {
    validateCleanDataSingleField("convert_to_FLOAT(c, 1, 1, 0.0)", "c",
        "3.2E-90",
        "-5.0E-10",
        "1.0E7",
        "3.0E10",
        "-5.0E-10",
        "2.0E10",
        "2.0E7",
        "-3.0E10",
        "5.0E-10");
  }

  @Test
  public void testCastScientificNotation() throws Exception {
    validateCleanDataSingleField("cast(c as DOUBLE)", "c",
        "3.2E-90",
        "-5.0E-10",
        "1.0E7",
        "3.0E10",
        "-5.0E-10",
        "2.0E10",
        "2.0E7",
        "-3.0E10",
        "5.0E-10");
  }

  @Test
  public void testCleanDataIntCast() throws Exception {
    validateCleanDataSingleField("clean_data_to_Integer(b, 1, 0, 0)", "b",
        "123",
        "123",
        "123",
        "123",
        "123",
        "100",
        "0",
        "-123",
        "0");
  }

  @Test
  public void testCleanDataIntDefault() throws Exception {
    validateCleanDataSingleField("clean_data_to_Integer(b, 0, 0, 0)", "b",
        "0",
        "123",
        "0",
        "123",
        "123",
        "0",
        "0",
        "0",
        "0");
  }

  @Test
  public void testCleanDataIntDefaultNull() throws Exception {
    validateCleanDataSingleField("clean_data_to_Integer(b, 0, 1, 0)", "b",
        null,
        "123",
        null,
        "123",
        "123",
        null,
        null,
        null,
        "0");
  }

  @Test
  public void testExtractListSingle0() throws Exception {
    validate("select a[0] as a from cp.\"json/extract_list.json\"", "a",
        "Shopping",
        "Bars",
        "Bars"
        );
  }

  @Test
  public void testExtractListSingle4() throws Exception {
    validate("select a[4] as a from cp.\"json/extract_list.json\"", "a",
        null,
        "Restaurants",
        null
        );
  }

  private void validate(String sql, String col, String... values) throws Exception {
    try (final JobDataFragment result = runQueryAndGetResults(sql)) {
      List<String> actual = new ArrayList<>();
      for (int i =0; i < result.getReturnedRowCount(); i++) {
        actual.add(result.extractString(col, i));
      }
      Assert.assertEquals(asList(values), actual);
    }
  }

  @Test
  @Ignore("flakey")
  public void testExtractListRange4() throws Exception {
    try (final JobDataFragment result = runQueryAndGetResults("select extract_list_range(a, 2, 1, 3, -1)['root'] as a from cp.\"json/extract_list.json\"")) {
      String column = "a";
      Assert.assertEquals(result.getReturnedRowCount(), 3);
      Assert.assertTrue(result.extractString(column, 0).equals("[]") || result.extractString(column, 0).equals("null"));
      Assert.assertEquals("[\"Nightlife\"]", result.extractString(column, 1));
      Assert.assertTrue(result.extractString(column, 0).equals("[]") || result.extractString(column, 0).equals("null"));
    }
  }

  @Test
  public void testTitleCase() throws Exception {
    validate("select title(a) as a from cp.\"json/convert_case.json\"", "a",
        "Los Angeles",
        "Los Angeles",
        "Los Angeles",
        "Los Angeles",
        "Los Angeles",
        "Washington",
        "Washington"
            );
  }

  @Test
  public void testDremioTypeOfInteger() throws Exception {
    validate("SELECT \"t\" AS \"t2\"" +
                    "FROM cp.\"json/mixed_example.json\" WHERE dremio_type_of(\"t\") = 'INTEGER' LIMIT 5", "t2",
            "0", "0", "0", "0", "0"
    );
  }

  @Test
  public void testDremioTypeOfText() throws Exception {
    validate("SELECT \"t\" AS \"t2\"" +
                    "FROM cp.\"json/mixed_example.json\" WHERE dremio_type_of(\"t\") = 'TEXT' ORDER BY t2 DESC LIMIT 5", "t2",
            "zero", "zero", "nan", "nan", "nan"
    );
  }

  @Test
  public void testDremioTypeOfFloat() throws Exception {
    validate("SELECT \"t\" AS \"t2\" " +
                    "FROM cp.\"json/mixed_example.json\" WHERE dremio_type_of(\"t\") = 'FLOAT'", "t2",
            "0.0", "0.0", "0.0", "1.0", "1.0"
    );
  }

  @Test
  public void testExtractMap() throws Exception {
    String extracted = "{\"close\":\"19:00\",\"open\":\"10:00\"}";
    validate("SELECT map_table.a.Tuesday as t " +
                    "FROM cp.\"json/extract_map.json\" map_table", "t",
            extracted, extracted, extracted
    );
  }

  @Test
  public void testExtractMapNested() throws Exception {
    String extracted = "19:00";
    validate("SELECT map_table.a.Tuesday.\"close\" as t " +
                    "FROM cp.\"json/extract_map.json\" map_table", "t",
            extracted, extracted, extracted
    );
  }
}
