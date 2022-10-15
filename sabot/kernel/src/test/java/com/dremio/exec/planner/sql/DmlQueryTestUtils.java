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
package com.dremio.exec.planner.sql;

import static com.dremio.BaseTestQuery.test;
import static com.dremio.BaseTestQuery.testRunAndReturn;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Rule;

import com.dremio.TestBuilder;
import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.test.TemporarySystemProperties;
import com.dremio.test.UserExceptionAssert;
import com.google.common.base.Strings;

/**
 * DML test utilities.
 */
public class DmlQueryTestUtils {

  @Rule
  public static TemporarySystemProperties PROPERTIES = new TemporarySystemProperties();

  public static final Object[][] EMPTY_EXPECTED_DATA = new Object[0][];
  public static final Set<Integer> PARTITION_COLUMN_ONE_INDEX_SET = new HashSet<Integer>() {{
    add(1);
  }};
  public static final String[] EMPTY_PATHS = new String[0];

  public static class ColumnInfo {
    public String name;
    public SqlTypeName typeName;
    public boolean partitionColumn;
    public String extra;

    public ColumnInfo(String name, SqlTypeName typeName, boolean partitionColumn, String extra) {
      this.name = name;
      this.typeName = typeName;
      this.partitionColumn = partitionColumn;
      this.extra = extra;
    }

    public ColumnInfo(String name, SqlTypeName typeName, boolean partitionColumn) {
      this(name, typeName, partitionColumn, null);
    }
  }

  public static class Table implements AutoCloseable {

    public final String name;
    public final String[] paths;
    public final String fqn;
    public final String[] columns;
    public final Object[][] originalData;

    public Table(String name, String[] paths, String fqn, String[] columns, Object[][] originalData) {
      this.name = name;
      this.paths = paths;
      this.fqn = fqn;
      this.columns = columns;
      this.originalData = originalData;
    }

    @Override
    public void close() throws Exception {
      test("DROP TABLE %s", fqn);
    }
  }

  public static class Tables implements AutoCloseable {

    public Table[] tables;

    public Tables(Table[] tables) {
      this.tables = tables;
    }

    @Override
    public void close() throws Exception {
      for (Table table : tables) {
        table.close();
      }
    }
  }

  /**
   * Creates a table with the given name, schema, data, and the source
   *
   * @param source where the table belongs
   * @param paths paths
   * @param name table name
   * @param schema schema / column data type info
   * @param data data to insert
   * @return table that's created with data inserted
   */
  public static Table createTable(String source, String[] paths, String name, ColumnInfo[] schema, Object[][] data) throws Exception {
    String fullPath = String.join(".", paths);
    String fqn = source + (fullPath.isEmpty() ? "" : "." + fullPath) + "." + name;
    String createTableSql = getCreateTableSql(Arrays.stream(schema).filter(
      columnInfo -> columnInfo.partitionColumn).map(
        columnInfo -> columnInfo.name).collect(Collectors.toList()));
    String schemaSql = Arrays.stream(schema).map(column ->
        String.format("%s %s%s", column.name, column.typeName, Strings.isNullOrEmpty(column.extra) ? "" : " " + column.extra))
      .collect(Collectors.joining(", "));
    test(createTableSql, fqn, schemaSql);

    String insertIntoSql = "INSERT INTO %s (%s) VALUES %s";
    String columnSql = Arrays.stream(schema).map(column -> column.name).collect(Collectors.joining(", "));
    String dataSql = data == null ? ""
      : Arrays.stream(data).map(
      row -> String.format("(%s)", Arrays.stream(row).map(
          cell -> cell != null ? cell instanceof String ? String.format("'%s'", cell): cell.toString() : "NULL")
        .collect(Collectors.joining(", ")))).collect(Collectors.joining(", "));
    if (!dataSql.isEmpty()) {
      test(insertIntoSql, fqn, columnSql, dataSql);
    }

    return new Table(name, paths, fqn, Arrays.stream(schema).map(column -> column.name).toArray(String[]::new), data);
  }

  public static Table createTable(String source, String name, ColumnInfo[] schema, Object[][] data) throws Exception {
    return createTable(source, EMPTY_PATHS, name, schema, data);
  }

  private static String getCreateTableSql(List<String> partitionColumns) {
    return String.format("CREATE TABLE %%s (%%s)%sSTORE AS (type => 'Iceberg')",
      CollectionUtils.isEmpty(partitionColumns)
        ? " " : String.format(" PARTITION BY (%s) ", String.join(", ", partitionColumns)));
  }

  public static String createRandomId() {
    String id = UUID.randomUUID().toString().replace("-", "");
    // Calcite does not allow IDs that start with a number without '"'. On the flip side, it'll remove
    // unnecessary '"'. So doing this odd thing with the IDs we create to make it play nice.
    return id.charAt(0) >= '0' && id.charAt(0) <= '9' ? String.format("\"%s\"", id) : String.format("%s", id);
  }

  /**
   * Creates a table where the first column is `id` numbered from 0 to `rowCount` - 1.
   * For every extra columns, it'll create a `column_#` with the (row #)_(col #) string.
   * The table name it uses is a random UUID, and therefore, the tests can run in parallel.
   *
   * @param source where the table should exist
   * @param pathCount number of paths between the source and the table name
   * @param columnCount number of columns
   * @param rowCount number of rows
   * @param partitionColumnIndexes contains column indexes for partition columns
   */
  public static Table createBasicTable(String source, int pathCount, int columnCount, int rowCount, Set<Integer> partitionColumnIndexes) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, partitionColumnIndexes.contains(0));
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.VARCHAR, partitionColumnIndexes.contains(c + 1));
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = 0; r < rowCount; r++) {
      data[r][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r][c + 1] = String.format("%s_%s", r, c);
      }
    }

    String[] paths = pathCount > 0 ? new String[pathCount] : EMPTY_PATHS;
    for (int i = 0; i < pathCount; i++) {
      paths[i] = createRandomId();
    }

    return createTable(
      source,
      paths,
      createRandomId(),
      schema,
      data);
  }

  public static Table createBasicTable(String source, int pathCount, int columnCount, int rowCount) throws Exception {
    return createBasicTable(source, pathCount, columnCount, rowCount, Collections.emptySet());
  }

  public static Table createBasicTable(String source, int columnCount, int rowCount) throws Exception {
    return createBasicTable(source, 0, columnCount, rowCount, Collections.emptySet());
  }

  public static Tables createBasicNonPartitionedAndPartitionedTables(String source, int columnCount, int rowCount, Set<Integer> partitionColumnIndexes) throws Exception {
    return new Tables(new Table[]{
      createBasicTable(source, columnCount, rowCount),
      createBasicTable(source, 0, columnCount, rowCount, partitionColumnIndexes)
    });
  }

  /**
   * Similar to createBasicTable except `column_#` columns have Decimal(38,0) type.
   *
   * @param source where the table should exist
   * @param columnCount number of columns
   * @param rowCount number of rows
   */
  public static Table createBasicTableWithDecimals(String source, int columnCount, int rowCount) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.DECIMAL, false);
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = 0; r < rowCount; r++) {
      data[r][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {

        data[r][c + 1] = new BigDecimal(String.format("%s", c * 1000 + r));
      }
    }

    return createTable(
      source,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      data);
  }

  /**
   * Similar to createBasicTable except `column_#` columns have Double type.
   *
   * @param source where the table should exist
   * @param columnCount number of columns
   * @param rowCount number of rows
   */
  public static Table createBasicTableWithDoubles(String source, int columnCount, int rowCount) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.DOUBLE, false);
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = 0; r < rowCount; r++) {
      data[r][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r][c + 1] = c * 1000.0 + r;
      }
    }

    return createTable(
      source,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      data);
  }

  /**
   * Similar to createBasicTable except `column_#` columns have Float type.
   *
   * @param source where the table should exist
   * @param columnCount number of columns
   * @param rowCount number of rows
   */
  public static Table createBasicTableWithFloats(String source, int columnCount, int rowCount) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.FLOAT, false);
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = 0; r < rowCount; r++) {
      data[r][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r][c + 1] = c * 1000.0f + r;
      }
    }

    return createTable(
      source,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      data);
  }

  public static AutoCloseable createView(String source, String name) throws Exception {
    return createViewFromTable(source, name, "INFORMATION_SCHEMA.CATALOGS");
  }

  public static AutoCloseable createViewFromTable(String source, String viewName, String tableName) throws Exception {
    PROPERTIES.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
    test("CREATE VIEW %s.%s AS SELECT * FROM %s", source, viewName, tableName);

    return () -> {
      test("DROP VIEW %s.%s", source, viewName);
      PROPERTIES.clear(DremioConfig.LEGACY_STORE_VIEWS_ENABLED);
    };
  }

  public static void testMalformedDmlQueries(Object[] tables, String... malformedQueries) {
    for (String malformedQuery : malformedQueries) {
      String fullQuery = String.format(malformedQuery, tables);
      UserExceptionAssert.assertThatThrownBy(() -> test(fullQuery))
        .withFailMessage("Query failed to generate the expected error:\n" + fullQuery)
        .satisfiesAnyOf(
          ex -> assertThat(ex).hasMessageContaining("Failure parsing the query."),
          ex -> assertThat(ex).hasMessageContaining("VALIDATION ERROR:"));
    }
  }

  /**
   * Runs the (DML) query and verifies the impacted records matches against `records`.
   * Furthermore, if `expectedData` is non-null (EMPTY_EXPECTED_DATA is allowed), then
   * it'll run a SELECT query such that you can verify data.
   */
  public static void testDmlQuery(BufferAllocator allocator, String query, Object[] args, Table table, long records, Object[]... expectedData) throws Exception {
    // Run the DML query and verify the impacted records is correct.
    new TestBuilder(allocator)
      .sqlQuery(query, args)
      .unOrdered()
      .baselineColumns("Records")
      .baselineValues(records)
      .go();

    // Run the SELECT query to verify some data.
    if (expectedData != null) {
      TestBuilder selectQuery = new TestBuilder(allocator)
        .sqlQuery("SELECT * FROM %s", table.fqn)
        .unOrdered()
        .baselineColumns(table.columns);
      if (expectedData.length == 0) {
        selectQuery.expectsEmptyResultSet();
      } else {
        for (Object[] expectedDatum : expectedData) {
          selectQuery.baselineValues(expectedDatum);
        }
      }

      // Actually verifies the number of rows returned.
      selectQuery.go();
    }
  }

  public static void verifyData(BufferAllocator allocator, Table table, Object[]... expectedData) throws Exception {
    TestBuilder builder = new TestBuilder(allocator)
      .sqlQuery("SELECT * FROM %s", table.fqn)
      .unOrdered()
      .baselineColumns(table.columns);

    for (Object[] expectedDatum : expectedData) {
      builder.baselineValues(expectedDatum);
    }

    builder.go();
  }

  public static AutoCloseable setContext(BufferAllocator allocator, String context) throws Exception {
    final StringBuilder resultBuilder = new StringBuilder();
    final RecordBatchLoader loader = new RecordBatchLoader(allocator);
    QueryDataBatch result = testRunAndReturn(UserBitShared.QueryType.SQL, "SELECT CURRENT_SCHEMA").get(0);
    loader.load(result.getHeader().getDef(), result.getData());
    VectorUtil.appendVectorAccessibleContent(loader, resultBuilder, "", false);
    loader.clear();
    result.release();

    String previousContext = resultBuilder.deleteCharAt(resultBuilder.length() - 1).toString();
    test(String.format("USE %s", context));

    return () -> test(String.format("USE %s", previousContext));
  }
}
