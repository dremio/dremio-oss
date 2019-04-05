/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.TestBuilder;
import com.dremio.common.util.TestTools;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.CatalogService.UpdateType;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Strings;

public class TestInfoSchemaOnHiveStorage extends HiveTestBase {

  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  private static final String[] baselineCols = new String[] {"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"};
  private static final Object[] expVal1 = new Object[] {"key", "INTEGER", "YES"};
  private static final Object[] expVal2 = new Object[] {"value", "CHARACTER VARYING", "YES"};

  @Before
  public void ensureFullMetadataRead() throws NamespaceException{
    getSabotContext().getCatalogService().refreshSource(new NamespaceKey("hive"), CatalogService.REFRESH_EVERYTHING_NOW, UpdateType.FULL);
  }

  @Test
  public void showTablesFromDb() throws Exception{
    testBuilder()
        .sqlQuery("SHOW TABLES FROM hive.\"default\"")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("hive.default", "partition_pruning_test")
        .baselineValues("hive.default", "readtest")
        .baselineValues("hive.default", "readtest_parquet")
        .baselineValues("hive.default", "readtest_orc")
        .baselineValues("hive.default", "empty_table")
        .baselineValues("hive.default", "partitioned_empty_table")
        .baselineValues("hive.default", "infoschematest")
        .baselineValues("hive.default", "kv")
        .baselineValues("hive.default", "kv_parquet")
        .baselineValues("hive.default", "kv_sh")
        .baselineValues("hive.default", "kv_mixedschema")
        .baselineValues("hive.default", "simple_json")
        .baselineValues("hive.default", "partition_with_few_schemas")
        .baselineValues("hive.default", "parquet_timestamp_nulls")
        .baselineValues("hive.default", "dummy")
        .baselineValues("hive.default", "sorted_parquet")
        .baselineValues("hive.default", "parquet_region")
        .baselineValues("hive.default", "parquet_with_two_files")
        .baselineValues("hive.default", "orc_region")
        .baselineValues("hive.default", "parquet_mult_rowgroups")
        .baselineValues("hive.default", "parquetschemalearntest")
        .baselineValues("hive.default", "tinyint_orc")
        .baselineValues("hive.default", "smallint_orc")
        .baselineValues("hive.default", "int_orc")
        .baselineValues("hive.default", "bigint_orc")
        .baselineValues("hive.default", "float_orc")
        .baselineValues("hive.default", "double_orc")
        .baselineValues("hive.default", "decimal_orc")
        .baselineValues("hive.default", "string_orc")
        .baselineValues("hive.default", "varchar_orc")
        .baselineValues("hive.default", "timestamp_orc")
        .baselineValues("hive.default", "date_orc")
        .baselineValues("hive.default", "tinyint_to_smallint_orc_ext")
        .baselineValues("hive.default", "tinyint_to_int_orc_ext")
        .baselineValues("hive.default", "tinyint_to_bigint_orc_ext")
        .baselineValues("hive.default", "tinyint_to_float_orc_ext")
        .baselineValues("hive.default", "tinyint_to_double_orc_ext")
        .baselineValues("hive.default", "tinyint_to_decimal_orc_ext")
        .baselineValues("hive.default", "tinyint_to_string_orc_ext")
        .baselineValues("hive.default", "tinyint_to_varchar_orc_ext")
        .baselineValues("hive.default", "smallint_to_int_orc_ext")
        .baselineValues("hive.default", "smallint_to_bigint_orc_ext")
        .baselineValues("hive.default", "smallint_to_float_orc_ext")
        .baselineValues("hive.default", "smallint_to_double_orc_ext")
        .baselineValues("hive.default", "smallint_to_decimal_orc_ext")
        .baselineValues("hive.default", "smallint_to_string_orc_ext")
        .baselineValues("hive.default", "smallint_to_varchar_orc_ext")
        .baselineValues("hive.default", "int_to_bigint_orc_ext")
        .baselineValues("hive.default", "int_to_float_orc_ext")
        .baselineValues("hive.default", "int_to_double_orc_ext")
        .baselineValues("hive.default", "int_to_decimal_orc_ext")
        .baselineValues("hive.default", "int_to_string_orc_ext")
        .baselineValues("hive.default", "int_to_varchar_orc_ext")
        .baselineValues("hive.default", "bigint_to_float_orc_ext")
        .baselineValues("hive.default", "bigint_to_double_orc_ext")
        .baselineValues("hive.default", "bigint_to_decimal_orc_ext")
        .baselineValues("hive.default", "bigint_to_string_orc_ext")
        .baselineValues("hive.default", "bigint_to_varchar_orc_ext")
        .baselineValues("hive.default", "float_to_double_orc_ext")
        .baselineValues("hive.default", "float_to_decimal_orc_ext")
        .baselineValues("hive.default", "float_to_string_orc_ext")
        .baselineValues("hive.default", "float_to_varchar_orc_ext")
        .baselineValues("hive.default", "double_to_decimal_orc_ext")
        .baselineValues("hive.default", "double_to_string_orc_ext")
        .baselineValues("hive.default", "double_to_varchar_orc_ext")
        .baselineValues("hive.default", "decimal_to_string_orc_ext")
        .baselineValues("hive.default", "decimal_to_varchar_orc_ext")
        .baselineValues("hive.default", "timestamp_to_string_orc_ext")
        .baselineValues("hive.default", "timestamp_to_varchar_orc_ext")
        .baselineValues("hive.default", "date_to_string_orc_ext")
        .baselineValues("hive.default", "date_to_varchar_orc_ext")
        .baselineValues("hive.default", "string_to_double_orc_ext")
        .baselineValues("hive.default", "string_to_decimal_orc_ext")
        .baselineValues("hive.default", "string_to_varchar_orc_ext")
        .baselineValues("hive.default", "varchar_to_double_orc_ext")
        .baselineValues("hive.default", "varchar_to_decimal_orc_ext")
        .baselineValues("hive.default", "varchar_to_string_orc_ext")
        .baselineValues("hive.default", "decimal_conversion_test_orc")
        .baselineValues("hive.default", "decimal_conversion_test_orc_ext")
        .baselineValues("hive.default", "decimal_conversion_test_orc_ext_2")
        .baselineValues("hive.default", "decimal_conversion_test_orc_rev")
        .baselineValues("hive.default", "decimal_conversion_test_orc_rev_ext")
        .baselineValues("hive.default", "orc_more_columns")
        .baselineValues("hive.default", "orc_more_columns_ext")
        .go();

    testBuilder()
        .sqlQuery("SHOW TABLES IN hive.db1")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("hive.db1", "kv_db1")
        .baselineValues("hive.db1", "avro")
        .baselineValues("hive.db1", "impala_parquet")
        .go();

    testBuilder()
        .sqlQuery("SHOW TABLES IN hive.skipper")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("hive.skipper", "kv_text_small")
        .baselineValues("hive.skipper", "kv_text_large")
        .baselineValues("hive.skipper", "kv_incorrect_skip_header")
        .baselineValues("hive.skipper", "kv_incorrect_skip_footer")
        .baselineValues("hive.skipper", "kv_rcfile_large")
        .baselineValues("hive.skipper", "kv_parquet_large")
        .baselineValues("hive.skipper", "kv_sequencefile_large")
        .go();
  }

  @Test
  public void showDatabases() throws Exception{
    test("show databases");
    testBuilder()
        .sqlQuery("SHOW DATABASES")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues("cp")
        .baselineValues("dfs")
        .baselineValues("dfs_test")
        .baselineValues("dfs_root")
        .baselineValues("dacfs")
        .baselineValues("hive")
        .baselineValues("hive.default")
        .baselineValues("hive.db1")
        .baselineValues("hive.skipper")
        .baselineValues("sys")
        .baselineValues("INFORMATION_SCHEMA")
        .go();
  }

  private void describeHelper(final String options, final String describeCmd) throws Exception {
    final TestBuilder builder = testBuilder();

    if (!Strings.isNullOrEmpty(options)) {
      builder.optionSettingQueriesForTestQuery(options);
    }

    builder.sqlQuery(describeCmd)
        .unOrdered()
        .baselineColumns(baselineCols)
        .baselineValues(expVal1)
        .baselineValues(expVal2)
        .go();
  }

  // When table name is fully qualified with schema name (sub-schema is default schema)
  @Test
  public void describeTable1() throws Exception{
    describeHelper(null, "DESCRIBE hive.\"default\".kv");
  }

  // When table name is fully qualified with schema name (sub-schema is non-default schema)
  @Test
  public void describeTable2() throws Exception{
    testBuilder()
        .sqlQuery("DESCRIBE hive.\"db1\".kv_db1")
        .unOrdered()
        .baselineColumns(baselineCols)
        .baselineValues("key", "CHARACTER VARYING", "YES")
        .baselineValues("value", "CHARACTER VARYING", "YES")
        .go();
  }

  // When table is qualified with just the top level schema. It should look for the table in default sub-schema within
  // the top level schema.
  @Test
  public void describeTable3() throws Exception {
    describeHelper(null, "DESCRIBE hive.kv");
  }

  // When table name is qualified with multi-level schema (sub-schema is default schema) given as single level schema name.
  @Test
  public void describeTable4() throws Exception {
    describeHelper(null, "DESCRIBE \"hive.default\".kv");
  }

  // When table name is qualified with multi-level schema (sub-schema is non-default schema)
  // given as single level schema name.
  @Test
  public void describeTable5() throws Exception {
    testBuilder()
        .sqlQuery("DESCRIBE \"hive.db1\".kv_db1")
        .unOrdered()
        .baselineColumns(baselineCols)
        .baselineValues("key", "CHARACTER VARYING", "YES")
        .baselineValues("value", "CHARACTER VARYING", "YES")
        .go();
  }

  // When current default schema is just the top-level schema name and the table has no schema qualifier. It should
  // look for the table in default sub-schema within the top level schema.
  @Test
  public void describeTable6() throws Exception {
    describeHelper("USE hive", "DESCRIBE kv");
  }

  // When default schema is fully qualified with schema name and table is not qualified with a schema name
  @Test
  public void describeTable7() throws Exception {
    describeHelper("USE hive.\"default\"", "DESCRIBE kv");
  }

  // When default schema is qualified with multi-level schema.
  @Test
  public void describeTable8() throws Exception {
    describeHelper("USE \"hive\".\"default\"", "DESCRIBE kv");
  }

  // When default schema is top-level schema and table is qualified with sub-schema
  @Test
  public void describeTable9() throws Exception {
    describeHelper("USE \"hive\"", "DESCRIBE \"default\".kv");
  }

  @Test
  public void varCharMaxLengthAndDecimalPrecisionInInfoSchema() throws Exception{
    final String query =
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
        "       NUMERIC_PRECISION_RADIX, NUMERIC_PRECISION, NUMERIC_SCALE " +
        "FROM INFORMATION_SCHEMA.\"COLUMNS\" " +
        "WHERE TABLE_SCHEMA = 'hive.default' AND TABLE_NAME = 'infoschematest' AND " +
        "(COLUMN_NAME = 'stringtype' OR COLUMN_NAME = 'varchartype' OR COLUMN_NAME = 'chartype' OR " +
        "COLUMN_NAME = 'inttype' OR COLUMN_NAME = 'decimaltype')";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .optionSettingQueriesForTestQuery("USE hive")
        .baselineColumns("COLUMN_NAME",
                         "DATA_TYPE",
                         "CHARACTER_MAXIMUM_LENGTH",
                         "NUMERIC_PRECISION_RADIX",
                         "NUMERIC_PRECISION",
                         "NUMERIC_SCALE")
        .baselineValues("inttype",     "INTEGER",            null,    2,   32,    0)
        .baselineValues("decimaltype", "DECIMAL",            null,   10,   38,    2)
        .baselineValues("stringtype",  "CHARACTER VARYING", 65536, null, null, null)
        .baselineValues("varchartype", "CHARACTER VARYING",    65536, null, null, null)
        .baselineValues("chartype", "CHARACTER VARYING", 65536, null, null, null)
        .go();
  }

  @Test
  public void defaultSchemaHive() throws Exception{
    testBuilder()
        .sqlQuery("SELECT * FROM kv LIMIT 2")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE hive")
        .baselineColumns("key", "value")
        .baselineValues(1, " key_1")
        .baselineValues(2, " key_2")
        .go();
  }

  @Test
  public void defaultTwoLevelSchemaHive() throws Exception{
    testBuilder()
        .sqlQuery("SELECT * FROM kv_db1 LIMIT 2")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE hive.db1")
        .baselineColumns("key", "value")
        .baselineValues("1", " key_1")
        .baselineValues("2", " key_2")
        .go();
  }
}
