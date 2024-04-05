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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.insertRows;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.TestBuilder;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.exec.planner.sql.DmlQueryTestUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileContent;
import org.junit.Test;

/** Test class for Iceberg TABLE_FILES Function select * from table(table_files('table')) */
public class TestIcebergTableFilesFunction extends IcebergMetadataTestTable {

  @Test
  public void testTableFilesColumnValuesWithLongValue() throws Exception {
    insertOneRecord();

    String query =
        String.format(
            "SELECT content FROM table(table_files('\"%s\".\"%s\"')) limit 1",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    String[] expectedColumns = {"content"};
    Object[] expectedValues = {FileContent.DATA.name()};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    // Match count of data files. -> ONE
    query =
        String.format(
            "SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    expectedColumns = new String[] {"file_count"};
    expectedValues = new Object[] {1L};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    insertOneLongRecord();

    // Match count of data files. -> TWO
    query =
        String.format(
            "SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    expectedColumns = new String[] {"file_count"};
    expectedValues = new Object[] {2L};
    queryAndMatchResults(query, expectedColumns, expectedValues);
  }

  @Test
  public void testTableFilesColumnValues() throws Exception {
    insertOneRecord();

    String query =
        String.format(
            "SELECT content FROM table(table_files('\"%s\".\"%s\"')) limit 1",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    String[] expectedColumns = {"content"};
    Object[] expectedValues = {FileContent.DATA.name()};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    // Match count of data files. -> ONE
    query =
        String.format(
            "SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    expectedColumns = new String[] {"file_count"};
    expectedValues = new Object[] {1L};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    insertOneRecord();

    // Match count of data files. -> TWO
    query =
        String.format(
            "SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    expectedColumns = new String[] {"file_count"};
    expectedValues = new Object[] {2L};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    insertTwoRecords();

    // Match count for record_count=2
    query =
        String.format(
            "SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"')) where record_count=2",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME);
    expectedColumns = new String[] {"file_count"};
    expectedValues = new Object[] {1L};
    queryAndMatchResults(query, expectedColumns, expectedValues);

    // Match count with snapshot
    query =
        String.format(
            "SELECT count(*) as file_count FROM table(table_files('\"%s\".\"%s\"')) AT SNAPSHOT '%s'",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME, FIRST_SNAPSHOT);
    expectedColumns = new String[] {"file_count"};
    expectedValues = new Object[] {0L};
    queryAndMatchResults(query, expectedColumns, expectedValues);
  }

  private void insertOneLongRecord() throws Exception {
    String longString =
        " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " with email as (select lower(email_address) as email_address, lower(username) as username, source_pk1, source_type\n"
            + " order by e.email_address";
    String insertCommandSql =
        String.format(
            "insert into %s.%s VALUES(1,'%s', 2.0)",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME, longString);
    test(insertCommandSql);
    Thread.sleep(1001);
  }

  @Test
  public void testTableFilesSchema() throws Exception {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("content"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("file_path"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("file_format"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("partition"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("record_count"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("file_size_in_bytes"),
            Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("column_sizes"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("value_counts"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("null_value_counts"),
            Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("nan_value_counts"),
            Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("lower_bounds"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("upper_bounds"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("split_offsets"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("equality_ids"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("sort_order_id"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("spec_id"), Types.required(TypeProtos.MinorType.INT)));
    expectedSchema(
        expectedSchema,
        "SELECT * FROM table(table_files('\"%s\".\"%s\"')) limit 1",
        TEMP_SCHEMA_HADOOP,
        METADATA_TEST_TABLE_NAME);
  }

  @Test
  public void testInvalidColumnTypeTableFilesSchema() {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("content"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("file_path"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("file_format"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("partition"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("record_count"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("file_size_in_bytes"),
            Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("column_sizes"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("value_counts"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("null_value_counts"),
            Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("nan_value_counts"),
            Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("lower_bounds"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("upper_bounds"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("split_offsets"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("equality_ids"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("sort_order_id"),
            Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("spec_id"), Types.required(TypeProtos.MinorType.INT)));
    assertThatThrownBy(
            () ->
                expectedSchema(
                    expectedSchema,
                    "SELECT * FROM table(table_files('\"%s\".\"%s\"'))",
                    TEMP_SCHEMA_HADOOP,
                    METADATA_TEST_TABLE_NAME))
        .hasMessageContaining("Schema path or type mismatch for")
        .isInstanceOf(Exception.class);
  }

  @Test
  public void incorrectTableName() {
    String query = "SELECT count(*) as k FROM table(table_files('blah'))";
    assertThatThrownBy(() -> runSQL(query)).hasMessageContaining("not found");
  }

  @Test
  public void testPartitionData() throws Exception {
    try (DmlQueryTestUtils.Table table =
        createBasicTable(TEMP_SCHEMA_HADOOP, 0, 2, 0, ImmutableSet.of(1))) {
      insertRows(table, 1);
      new TestBuilder(allocator)
          .sqlQuery("SELECT \"partition\" FROM TABLE(table_files('%s'))", table.fqn)
          .unOrdered()
          .baselineColumns("partition")
          .baselineValues("{column_0=0_0}")
          .build()
          .run();
    }
  }

  @Test
  public void testV2TableWithDeleteFiles() throws Exception {
    String testRootPath = "/tmp/iceberg";
    safeCopy("iceberg/table_with_delete", testRootPath);
    final String tableName = "dfs_hadoop.tmp.iceberg";
    runSQL(String.format("alter table %s refresh metadata", tableName));

    new TestBuilder(allocator)
        .sqlQuery(
            "select content, file_path, \"file_format\", \"partition\", record_count, file_size_in_bytes, spec_id from table(table_files('%s'))",
            tableName)
        .unOrdered()
        .baselineColumns(
            "content",
            "file_path",
            "file_format",
            "partition",
            "record_count",
            "file_size_in_bytes",
            "spec_id")
        .baselineRecords(v2TableFiles())
        .build()
        .run();
  }

  private List<Map<String, Object>> v2TableFiles() throws FileNotFoundException {
    List<Map<String, Object>> tableFileRecords = new ArrayList<>();
    Scanner scanner = new Scanner(new File("/tmp/iceberg/v2_table_files_out.csv"));
    List<String> headers =
        Arrays.stream(scanner.nextLine().split(","))
            .map(s -> "`" + s + "`")
            .collect(Collectors.toList());

    while (scanner.hasNextLine()) {
      Map<String, Object> record = new LinkedHashMap<>();
      String[] values = scanner.nextLine().split(",");
      record.put(headers.get(0), values[0]);
      record.put(headers.get(1), values[1]);
      record.put(headers.get(2), values[2]);
      record.put(headers.get(3), values[3]);
      record.put(headers.get(4), Long.parseLong(values[4]));
      record.put(headers.get(5), Long.parseLong(values[5]));
      record.put(headers.get(6), Integer.parseInt(values[6]));
      tableFileRecords.add(record);
    }
    return tableFileRecords;
  }

  private static void safeCopy(String src, String testRoot) throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(testRoot);
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    fs.mkdirs(path);

    copyFromJar(src, Paths.get(testRoot));
  }
}
