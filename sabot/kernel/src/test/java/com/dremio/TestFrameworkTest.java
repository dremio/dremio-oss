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
package com.dremio;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Ignore;
import org.junit.Test;

// TODO - update framework to remove any dependency on the Dremio engine for reading baseline result
// sets
// currently using it with the assumption that the csv and json readers are well tested, and
// handling diverse
// types in the test framework would require doing some redundant work to enable casting outside of
// Dremio or
// some better tooling to generate parquet files that have all of the parquet types
public class TestFrameworkTest extends BaseTestQuery {

  private static String CSV_COLS =
      " cast(columns[0] as bigint) employee_id, columns[1] as first_name, columns[2] as last_name ";

  @Test(expected = AssertionError.class)
  public void testSchemaTestBuilderSetInvalidBaselineValues() throws Exception {
    final String query = "SELECT ltrim('dremio') as col FROM (VALUES(1)) limit 0";

    List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList();
    MajorType majorType = Types.required(MinorType.VARCHAR);
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .baselineValues(new Object[0])
        .build()
        .run();
  }

  @Test(expected = AssertionError.class)
  public void testSchemaTestBuilderSetInvalidBaselineRecords() throws Exception {
    final String query = "SELECT ltrim('dremio') as col FROM (VALUES(1)) limit 0";

    List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList();
    MajorType majorType = Types.required(MinorType.VARCHAR);
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .baselineRecords(Collections.<Map<String, Object>>emptyList())
        .build()
        .run();
  }

  @Test(expected = AssertionError.class)
  public void testSchemaTestBuilderSetInvalidBaselineColumns() throws Exception {
    final String query = "SELECT ltrim('dremio') as col FROM (VALUES(1)) limit 0";

    List<Pair<SchemaPath, MajorType>> expectedSchema = Lists.newArrayList();
    MajorType majorType = Types.required(MinorType.VARCHAR);
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .baselineColumns("col")
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testCSVVerification() throws Exception {
    testBuilder()
        .sqlQuery(
            "select employee_id, first_name, last_name from cp.\"testframework/small_test_data.json\"")
        .ordered()
        .csvBaselineFile("testframework/small_test_data.tsv")
        .baselineTypes(MinorType.BIGINT, MinorType.VARCHAR, MinorType.VARCHAR)
        .baselineColumns("employee_id", "first_name", "last_name")
        .build()
        .run();
  }

  @Test
  public void testBaselineValsVerification() throws Exception {
    testBuilder()
        .sqlQuery(
            "select employee_id, first_name, last_name from cp.\"testframework/small_test_data.json\" limit 1")
        .ordered()
        .baselineColumns("employee_id", "first_name", "last_name")
        .baselineValues(12L, "Jewel", "Creek")
        .build()
        .run();

    testBuilder()
        .sqlQuery(
            "select employee_id, first_name, last_name from cp.\"testframework/small_test_data.json\" limit 1")
        .unOrdered()
        .baselineColumns("employee_id", "first_name", "last_name")
        .baselineValues(12L, "Jewel", "Creek")
        .build()
        .run();
  }

  @Test
  @Ignore("decimal")
  public void testDecimalBaseline() throws Exception {
    //      test("select cast(columns[0]  as DECIMAL38SPARSE(38,2) ) \"dec_col\" from
    // cp.\"testframework/decimal_test.tsv\"");
    // type information can be provided explicitly
    testBuilder()
        .sqlQuery(
            "select cast(dec_col as decimal(38,2)) dec_col from cp.\"testframework/decimal_test.json\"")
        .unOrdered()
        .csvBaselineFile("testframework/decimal_test.tsv")
        .baselineTypes(
            MajorType.newBuilder()
                .setMinorType(MinorType.DECIMAL)
                .setMode(DataMode.REQUIRED)
                .setPrecision(32)
                .setScale(2)
                .build())
        //              .baselineTypes(new MajorType(MinorType.DECIMAL38SPARSE, DataMode.REQUIRED,
        // 38, 2, 0, null))
        .baselineColumns("dec_col")
        .build()
        .run();

    // type information can also be left out, this will prompt the result types of the test query to
    // drive the
    // interpretation of the test file
    //      testBuilder()
    //          .sqlQuery("select cast(dec_col as decimal(38,2)) dec_col from
    // cp.\"testframework/decimal_test.json\"")
    //          .unOrdered()
    //          .csvBaselineFile("testframework/decimal_test.tsv")
    //          .baselineColumns("dec_col")
    //          .build().run();

    // Or you can provide explicit values to the builder itself to avoid going through the Dremio
    // engine at all to
    // populate the baseline results
    //      testBuilder()
    //          .sqlQuery("select cast(dec_col as decimal(38,2)) dec_col from
    // cp.\"testframework/decimal_test.json\"")
    //          .unOrdered()
    //          .baselineColumns("dec_col")
    //          .baselineValues(new BigDecimal("3.70"))
    //          .build().run();
  }

  @Test
  public void testMapOrdering() throws Exception {
    testBuilder()
        .sqlQuery("select * from cp.\"/testframework/map_reordering.json\"")
        .unOrdered()
        .jsonBaselineFile("testframework/map_reordering2.json")
        .build()
        .run();
  }

  @Test
  public void testBaselineValsVerificationWithNulls() throws Exception {
    testBuilder()
        .sqlQuery("select * from cp.\"store/json/json_simple_with_null.json\"")
        .ordered()
        .baselineColumns("a", "b")
        .baselineValues(5L, 10L)
        .baselineValues(7L, null)
        .baselineValues(null, null)
        .baselineValues(9L, 11L)
        .build()
        .run();

    testBuilder()
        .sqlQuery("select * from cp.\"store/json/json_simple_with_null.json\"")
        .unOrdered()
        .baselineColumns("a", "b")
        .baselineValues(5L, 10L)
        .baselineValues(9L, 11L)
        .baselineValues(7L, null)
        .baselineValues(null, null)
        .build()
        .run();
  }

  @Test
  public void testBaselineValsVerificationWithComplexAndNulls() throws Exception {
    testBuilder()
        .sqlQuery("select * from cp.\"/jsoninput/input2.json\" limit 1")
        .ordered()
        .baselineColumns("integer", "float", "x", "z", "l", "rl")
        .baselineValues(
            2010L,
            17.4,
            mapOf("y", "kevin", "z", "paul"),
            listOf(mapOf("orange", "yellow", "pink", "red"), mapOf("pink", "purple")),
            listOf(4L, 2L),
            listOf(listOf(2L, 1L), listOf(4L, 6L)))
        .build()
        .run();
  }

  @Test
  public void testCSVVerification_missing_records_fails() {
    assertThatThrownBy(
            () ->
                testBuilder()
                    .sqlQuery(
                        "select employee_id, first_name, last_name from cp.\"testframework/small_test_data.json\"")
                    .ordered()
                    .csvBaselineFile("testframework/small_test_data_extra.tsv")
                    .baselineTypes(MinorType.BIGINT, MinorType.VARCHAR, MinorType.VARCHAR)
                    .baselineColumns("employee_id", "first_name", "last_name")
                    .build()
                    .run())
        .hasMessageStartingWith("Incorrect number of rows returned by query.")
        .hasMessageContaining("expected:<7> but was:<5>");
  }

  @Test
  public void testCSVVerification_extra_records_fails() {
    assertThatThrownBy(
            () ->
                testBuilder()
                    .sqlQuery(
                        "select "
                            + CSV_COLS
                            + " from cp.\"testframework/small_test_data_extra.tsv\"")
                    .ordered()
                    .csvBaselineFile("testframework/small_test_data.tsv")
                    .baselineTypes(MinorType.BIGINT, MinorType.VARCHAR, MinorType.VARCHAR)
                    .baselineColumns("employee_id", "first_name", "last_name")
                    .build()
                    .run())
        .hasMessageStartingWith("Incorrect number of rows returned by query.")
        .hasMessageContaining("expected:<5> but was:<7>");
  }

  @Test
  public void testCSVVerification_extra_column_fails() {
    assertThatThrownBy(
            () ->
                testBuilder()
                    .sqlQuery(
                        "select "
                            + CSV_COLS
                            + ", columns[3] as address from cp.\"testframework/small_test_data_extra_col.tsv\"")
                    .ordered()
                    .csvBaselineFile("testframework/small_test_data.tsv")
                    .baselineTypes(MinorType.BIGINT, MinorType.VARCHAR, MinorType.VARCHAR)
                    .baselineColumns("employee_id", "first_name", "last_name")
                    .build()
                    .run())
        .hasMessage(
            "Incorrect keys, "
                + "expected:(`employee_id`,`first_name`,`last_name`) "
                + "actual:(`address`,`employee_id`,`first_name`,`last_name`)");
  }

  @Test
  public void testCSVVerification_missing_column_fails() {
    assertThatThrownBy(
            () ->
                testBuilder()
                    .sqlQuery(
                        "select employee_id, first_name, last_name from cp.\"testframework/small_test_data.json\"")
                    .ordered()
                    .csvBaselineFile("testframework/small_test_data_extra_col.tsv")
                    .baselineTypes(
                        MinorType.BIGINT, MinorType.VARCHAR, MinorType.VARCHAR, MinorType.VARCHAR)
                    .baselineColumns("employee_id", "first_name", "last_name", "address")
                    .build()
                    .run())
        .hasMessageStartingWith(
            "Incorrect keys,"
                + " expected:(`address`,`employee_id`,`first_name`,`last_name`)"
                + " actual:(`employee_id`,`first_name`,`last_name`");
  }

  @Test
  public void testCSVVerificationOfTypes() {
    assertThatThrownBy(
            () ->
                testBuilder()
                    .sqlQuery(
                        "select employee_id, first_name, last_name from cp.\"testframework/small_test_data.json\"")
                    .ordered()
                    .csvBaselineFile("testframework/small_test_data.tsv")
                    .baselineTypes(MinorType.INT, MinorType.VARCHAR, MinorType.VARCHAR)
                    .baselineColumns("employee_id", "first_name", "last_name")
                    .build()
                    .run())
        .hasMessageContaining("at position 0 column '`employee_id`' mismatched values,")
        .hasMessageContaining("expected (Integer)")
        .hasMessageContaining("but received (Long):");
  }

  @Test
  public void testCSVVerificationOfOrder_checkFailure() {
    assertThatThrownBy(
            () ->
                testBuilder()
                    .sqlQuery(
                        "select columns[0] as employee_id, columns[1] as first_name, columns[2] as last_name from cp.\"testframework/small_test_data_reordered.tsv\"")
                    .ordered()
                    .csvBaselineFile("testframework/small_test_data.tsv")
                    .baselineColumns("employee_id", "first_name", "last_name")
                    .build()
                    .run())
        .hasMessageContaining("at position 0 column '`employee_id`' mismatched values,")
        .hasMessageContaining("expected (String):")
        .hasMessageContaining("but received (String):");
  }

  @Test
  public void testCSVVerificationOfUnorderedComparison() throws Throwable {
    testBuilder()
        .sqlQuery(
            "select columns[0] as employee_id, columns[1] as first_name, columns[2] as last_name from cp.\"testframework/small_test_data_reordered.tsv\"")
        .unOrdered()
        .csvBaselineFile("testframework/small_test_data.tsv")
        .baselineColumns("employee_id", "first_name", "last_name")
        .build()
        .run();
  }

  // TODO - enable more advanced type handling for JSON, currently basic support works
  // add support for type information taken from test query, or explicit type expectations
  @Test
  public void testBasicJSON() throws Exception {
    testBuilder()
        .sqlQuery("select * from cp.\"scan_json_test_3.json\"")
        .ordered()
        .jsonBaselineFile("/scan_json_test_3.json")
        .build()
        .run();

    testBuilder()
        .sqlQuery("select * from cp.\"scan_json_test_3.json\"")
        .unOrdered() // Check other verification method with same files
        .jsonBaselineFile("/scan_json_test_3.json")
        .build()
        .run();
  }

  @Test
  public void testComplexJSON_all_text() throws Exception {
    testBuilder()
        .sqlQuery("select * from cp.\"store/json/schema_change_int_to_string.json\"")
        .optionSettingQueriesForTestQuery("alter system set \"store.json.all_text_mode\" = true")
        .ordered()
        .jsonBaselineFile("store/json/schema_change_int_to_string.json")
        .optionSettingQueriesForBaseline("alter system set \"store.json.all_text_mode\" = true")
        .build()
        .run();

    testBuilder()
        .sqlQuery("select * from cp.\"store/json/schema_change_int_to_string.json\"")
        .optionSettingQueriesForTestQuery("alter system set \"store.json.all_text_mode\" = true")
        .unOrdered() // Check other verification method with same files
        .jsonBaselineFile("store/json/schema_change_int_to_string.json")
        .optionSettingQueriesForBaseline("alter system set \"store.json.all_text_mode\" = true")
        .build()
        .run();
    test("alter system set \"store.json.all_text_mode\" = false");
  }

  @Test
  public void testRepeatedColumnMatching() {
    assertThatThrownBy(
            () ->
                testBuilder()
                    .sqlQuery("select * from cp.\"store/json/schema_change_int_to_string.json\"")
                    .optionSettingQueriesForTestQuery(
                        "alter system set \"store.json.all_text_mode\" = true")
                    .ordered()
                    .jsonBaselineFile("testframework/schema_change_int_to_string_non-matching.json")
                    .optionSettingQueriesForBaseline(
                        "alter system set \"store.json.all_text_mode\" = true")
                    .build()
                    .run())
        .hasMessageContaining("at position 1 column '`field_1`' mismatched values,")
        .hasMessageContaining("expected (JsonStringArrayList):")
        .hasMessageContaining("[\"5\",\"2\",\"3\",\"4\",\"1\",\"2\"]")
        .hasMessageContaining("but received (JsonStringArrayList):")
        .hasMessageContaining("[\"5\"]");
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    testBuilder()
        .sqlQuery("select * from cp.\"store/json/json_simple_with_null.json\" where 1=0")
        .expectsEmptyResultSet()
        .build()
        .run();

    assertThatThrownBy(
            () ->
                testBuilder()
                    .sqlQuery("select * from cp.\"store/json/json_simple_with_null.json\"")
                    .expectsEmptyResultSet()
                    .build()
                    .run())
        .hasMessageContaining("Different number of records returned - expected:<0> but was:<4>");
  }

  @Test
  public void testCSVVerificationTypeMap() throws Throwable {
    Map<SchemaPath, MajorType> typeMap = new HashMap<>();
    typeMap.put(TestBuilder.parsePath("first_name"), Types.optional(MinorType.VARCHAR));
    typeMap.put(TestBuilder.parsePath("employee_id"), Types.optional(MinorType.INT));
    typeMap.put(TestBuilder.parsePath("last_name"), Types.optional(MinorType.VARCHAR));
    testBuilder()
        .sqlQuery(
            "select cast(columns[0] as int) employee_id, columns[1] as first_name, columns[2] as last_name from cp.\"testframework/small_test_data_reordered.tsv\"")
        .unOrdered()
        .csvBaselineFile("testframework/small_test_data.tsv")
        .baselineColumns("employee_id", "first_name", "last_name")
        // This should work without this line because of the default type casts added based on the
        // types that come out of the test query.
        // To write a test that enforces strict typing you must pass type information using a CSV
        // with a list of types,
        // or any format with a Map of types like is constructed above and include the call to pass
        // it into the test, which is commented out below
        // .baselineTypes(typeMap)
        .build()
        .run();

    typeMap.clear();
    typeMap.put(TestBuilder.parsePath("first_name"), Types.optional(MinorType.VARCHAR));
    // This is the wrong type intentionally to ensure failures happen when expected
    typeMap.put(TestBuilder.parsePath("employee_id"), Types.optional(MinorType.VARCHAR));
    typeMap.put(TestBuilder.parsePath("last_name"), Types.optional(MinorType.VARCHAR));

    assertThatThrownBy(
            () ->
                testBuilder()
                    .sqlQuery(
                        "select cast(columns[0] as int) employee_id, columns[1] as first_name, columns[2] as last_name from cp.\"testframework/small_test_data_reordered.tsv\"")
                    .unOrdered()
                    .csvBaselineFile("testframework/small_test_data.tsv")
                    .baselineColumns("employee_id", "first_name", "last_name")
                    .baselineTypes(typeMap)
                    .build()
                    .run())
        .isInstanceOf(Exception.class);
  }
}
