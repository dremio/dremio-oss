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

import static com.dremio.BaseTestQuery.getDfsTestTmpSchemaLocation;
import static com.dremio.BaseTestQuery.runSQL;
import static com.dremio.BaseTestQuery.test;
import static com.dremio.BaseTestQuery.testRunAndReturn;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.joda.time.Duration;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Rule;

import com.dremio.TestBuilder;
import com.dremio.common.utils.SqlUtils;
import com.dremio.config.DremioConfig;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
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

  private static final int NUMBER_OF_SECONDS_PER_DAY = 86400;

  private static final HadoopTables hadoopTables = new HadoopTables(new Configuration());

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

  /**
   * Creates a empty table with the given name, schema, and the source
   *
   * @param source where the table belongs
   * @param paths paths
   * @param name table name
   * @return table that's created with data inserted
   */
  public static Table createEmptyTable(String source, String[] paths, String name, int columnCount) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.VARCHAR, false);
    }

    String fullPath = String.join(".", paths);
    String fqn = source + (fullPath.isEmpty() ? "" : "." + fullPath) + "." + name;
    String createTableSql = getCreateTableSql(Arrays.stream(schema).filter(
      columnInfo -> columnInfo.partitionColumn).map(
      columnInfo -> columnInfo.name).collect(Collectors.toList()));
    String schemaSql = Arrays.stream(schema).map(column ->
        String.format("%s %s%s", column.name, column.typeName, Strings.isNullOrEmpty(column.extra) ? "" : " " + column.extra))
      .collect(Collectors.joining(", "));

    test(createTableSql, fqn, schemaSql);

    return new Table(name, paths, fqn, Arrays.stream(schema).map(column -> column.name).toArray(String[]::new), null);
  }

  /**
   * Create an iceberg table, directly using the APIs. This method simulates the table creation from engines,
   * that use Iceberg OSS. Engines such as spark, hive etc.
   */
  public static Table createStockIcebergTable(String source, String[] paths, String name, ColumnInfo[] schema) throws Exception {
    String fullPath = String.join(".", paths);
    String fqn = source + (fullPath.isEmpty() ? "" : "." + fullPath) + "." + name;

    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    AtomicInteger columnIdx = new AtomicInteger(0);
    Types.NestedField[] icebergFields = Stream.of(schema).map(
      col -> Types.NestedField.optional(columnIdx.getAndIncrement(), col.name, toIcebergType(col.typeName)))
      .toArray(Types.NestedField[]::new);
    Schema icebergSchema = new Schema(icebergFields);
    String tableLocation = getDfsTestTmpSchemaLocation() + "/" +
      fullPath.replaceAll("\"", "").replaceAll("\\.", "/")
      + "/" + name;
    hadoopTables.create(icebergSchema, partitionSpec, tableLocation);
    String autoPromoteSql = "ALTER TABLE %s REFRESH METADATA AUTO PROMOTION";
    test(autoPromoteSql, fqn);

    return new Table(name, paths, fqn, Arrays.stream(schema).map(column -> column.name).toArray(String[]::new), null);
  }

  private static Type toIcebergType(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
      // Including only primitive types for sanity tests, extend as per need
      case INTEGER:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
      case VARCHAR:
        return Types.StringType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case DATE:
        return Types.DateType.get();
      case TIME:
        return Types.TimeType.get();
      case BOOLEAN:
        return Types.BooleanType.get();
      default:
        throw new IllegalArgumentException("Unsppported type " + sqlTypeName);
    }
  }

  private static Table createTableWithFormattedDateTime(String source, String[] paths, String name, ColumnInfo[] schema, Object[][] data, String dateTimeOutputFormat) throws Exception {
    String fullPath = String.join(".", paths);
    String fqn = source + (fullPath.isEmpty() ? "" : "." + fullPath) + "." + name;
    String createTableSql = getCreateTableSql(Arrays.stream(schema).filter(
      columnInfo -> columnInfo.partitionColumn).map(
      columnInfo -> columnInfo.name).collect(Collectors.toList()));
    String schemaSql = Arrays.stream(schema).map(column ->
        String.format("%s %s%s", column.name, column.typeName, Strings.isNullOrEmpty(column.extra) ? "" : " " + column.extra))
      .collect(Collectors.joining(", "));
    test(createTableSql, fqn, schemaSql);

    DateTimeFormatter dateTimeOutputFormatter = DateFunctionsUtils.getISOFormatterForFormatString(dateTimeOutputFormat);
    String insertIntoSql = "INSERT INTO %s (%s) VALUES %s";
    String columnSql = Arrays.stream(schema).map(column -> column.name).collect(Collectors.joining(", "));
    String dataSql = data == null ? ""
      : Arrays.stream(data).map(
      row -> String.format("(%s)", Arrays.stream(row).map(
          cell -> cell != null ? cell instanceof String ? String.format("'%s'", cell): cell instanceof LocalDateTime ? String.format("'%s'", ((LocalDateTime) cell).toString(dateTimeOutputFormatter)) : cell.toString() : "NULL")
        .collect(Collectors.joining(", ")))).collect(Collectors.joining(", "));
    if (!dataSql.isEmpty()) {
      test(insertIntoSql, fqn, columnSql, dataSql);
    }

    return new Table(name, paths, fqn, Arrays.stream(schema).map(column -> column.name).toArray(String[]::new), data);
  }

  public static void insertRows(Table table, int rowCount) throws Exception {
    String insertIntoSql = "INSERT INTO %s (%s) VALUES %s";
    String columnSql = String.join(",", table.columns);

    int columnCount = table.columns.length;
    // Consistent with data preparation logic in other places
    Object[][] data = new Object[rowCount][columnCount];
    for (int r = 0; r < rowCount; r++) {
      data[r][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r][c + 1] = String.format("%s_%s", r, c);
      }
    }

    String dataSql = data == null ? ""
      : Arrays.stream(data).map(
      row -> String.format("(%s)", Arrays.stream(row).map(
          cell -> cell != null ? cell instanceof String ? String.format("'%s'", cell): cell.toString() : "NULL")
        .collect(Collectors.joining(", ")))).collect(Collectors.joining(", "));
    if (!dataSql.isEmpty()) {
      test(insertIntoSql, table.fqn, columnSql, dataSql);
    }
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

  /***
   * Add quote to passed input if it does not have quotes already
   * One use case is for adding quote for id generated from createRandomId(),
   * which contains quotes already if it starts with numbers.
   * @param identity
   * @return
   */
  protected static String addQuotes(String identity) {
    // return if the input already has the quotes
    if (identity.charAt(0) == SqlUtils.QUOTE && identity.charAt(identity.length() - 1) == SqlUtils.QUOTE) {
      return identity;
    }

    return String.format("\"%s\"", identity);
  }

  /**
   * Creates a table where the first column is `id` numbered from 0 to `rowCount` - 1.
   * For every extra columns, it'll create a `column_#` with the (row #)_(col #) string.
   * The table name it uses is a random UUID, and therefore, the tests can run in parallel.
   *
   * @param source                 where the table should exist
   * @param pathCount              number of paths between the source and the table name
   * @param columnCount            number of columns
   * @param rowCount               number of rows
   * @param startingRowId          starting row id
   * @param partitionColumnIndexes contains column indexes for partition columns
   * @param tableName
   */
  public static Table createBasicTable(String source, int pathCount, int columnCount, int rowCount, int startingRowId, Set<Integer> partitionColumnIndexes, String tableName) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, partitionColumnIndexes.contains(0));
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.VARCHAR, partitionColumnIndexes.contains(c + 1));
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = startingRowId; r < startingRowId + rowCount; r++) {
      data[r - startingRowId][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r - startingRowId][c + 1] = String.format("%s_%s", r, c);
      }
    }

    String[] paths = pathCount > 0 ? new String[pathCount] : EMPTY_PATHS;
    for (int i = 0; i < pathCount; i++) {
      paths[i] = createRandomId();
    }

    return createTable(
      source,
      paths,
      Objects.isNull(tableName) ? createRandomId() : tableName,
      schema,
      data);
  }

  public static Table createStockIcebergTable(String source, int pathCount, int columnCount, String tableName) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.VARCHAR, false);
    }

    String[] paths = pathCount > 0 ? new String[pathCount] : EMPTY_PATHS;
    for (int i = 0; i < pathCount; i++) {
      paths[i] = createRandomId();
    }

    return createStockIcebergTable(
      source,
      paths,
      Objects.isNull(tableName) ? createRandomId() : tableName,
      schema);
  }

  /**
   * Add more rows in the table, which helps to increase Iceberg table's snapshots.
   */
  public static Table addRows(DmlQueryTestUtils.Table table, int rowCount) throws Exception {
    if (rowCount <= 0) {
      return table;
    }

    String insertIntoSql = "INSERT INTO %s (%s) VALUES %s";
    String columnSql = String.join(", ", table.columns);

    int existRowCount = table.originalData != null ? table.originalData.length : 0;
    int columnCount = table.originalData != null ? table.originalData[0].length : table.columns.length;
    Object[][] newData = new Object[rowCount][columnCount];
    for (int r = 0; r < rowCount; r++) {
      newData[r][0] = r + columnCount;
      for (int c = 0; c < columnCount - 1; c++) {
        newData[r][c + 1] = String.format("%s_%s", r + existRowCount, c);
      }
    }

    String dataSql = newData == null ? ""
      : Arrays.stream(newData).map(
      row -> String.format("(%s)", Arrays.stream(row).map(
          cell -> cell != null ? cell instanceof String ? String.format("'%s'", cell): cell.toString() : "NULL")
        .collect(Collectors.joining(", ")))).collect(Collectors.joining(", "));
    if (!dataSql.isEmpty()) {
      test(insertIntoSql, table.fqn, columnSql, dataSql);
    }

    int totalRowCount = existRowCount + rowCount;
    Object[][] allData = new Object[totalRowCount][columnCount];
    for (int r = 0; r < totalRowCount; r++) {
      for (int c = 0; c < columnCount; c++) {
        if (table.originalData != null && r < table.originalData.length) {
          allData[r][c] = table.originalData[r][c];
        } else {
          allData[r][c] = newData[r - existRowCount][c];
        }
      }
    }

    return new Table(table.name, table.paths, table.fqn, table.columns, allData);
  }

  public static Table createBasicTable(String source, int pathCount, int columnCount, int rowCount, Set<Integer> partitionColumnIndexes) throws Exception {
    return createBasicTable(source, pathCount, columnCount, rowCount, 0, partitionColumnIndexes, null);
  }

  public static Table createBasicTable(String source, int pathCount, int columnCount, int rowCount) throws Exception {
    return createBasicTable(source, pathCount, columnCount, rowCount, Collections.emptySet());
  }

  public static Table createBasicTable(int startingRowId, String source, int columnCount, int rowCount) throws Exception {
    return createBasicTable(source, 0, columnCount, rowCount, startingRowId, Collections.emptySet(), null);
  }

  public static Table createBasicTable(String source, int columnCount, int rowCount) throws Exception {
    return createBasicTable(source, 0, columnCount, rowCount, Collections.emptySet());
  }

  public static Table createBasicTable(int startingRowId, String source, int columnCount, int rowCount, String name) throws Exception {
    return createBasicTable(source, 0, columnCount, rowCount, startingRowId, Collections.emptySet(), name);
  }

  public static Tables createBasicNonPartitionedAndPartitionedTables(String source, int columnCount, int rowCount, Set<Integer> partitionColumnIndexes) throws Exception {
    return createBasicNonPartitionedAndPartitionedTables(source, columnCount, rowCount, 0, partitionColumnIndexes);
  }

  public static Tables createBasicNonPartitionedAndPartitionedTables(String source, int columnCount, int rowCount, int startingRowId, Set<Integer> partitionColumnIndexes) throws Exception {
    return new Tables(new Table[]{
      createBasicTable(startingRowId, source, columnCount, rowCount),
      createBasicTable(source, 0, columnCount, rowCount, startingRowId, partitionColumnIndexes, null)
    });
  }

  public static Table createBasicTableWithDecimals(String source, int columnCount, int rowCount) throws Exception {
    return createBasicTableWithDecimals(source, columnCount, rowCount, 0);
  }

  /**
   * Similar to createBasicTable except `column_#` columns have Decimal(38,0) type.
   *
   * @param source where the table should exist
   * @param columnCount number of columns
   * @param rowCount number of rows
   * @param startingRowId starting row id
   */
  public static Table createBasicTableWithDecimals(String source, int columnCount, int rowCount, int startingRowId) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.DECIMAL, false);
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = startingRowId; r < startingRowId + rowCount; r++) {
      data[r - startingRowId][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {

        data[r - startingRowId][c + 1] = new BigDecimal(String.format("%s", c * 1000 + r));
      }
    }

    return createTable(
      source,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      data);
  }

  public static Table createBasicTableWithDoubles(String source, int columnCount, int rowCount) throws Exception {
    return createBasicTableWithDoubles(source, columnCount, rowCount, 0);
  }

  /**
   * Similar to createBasicTable except `column_#` columns have Double type.
   *
   * @param source where the table should exist
   * @param columnCount number of columns
   * @param rowCount number of rows
   * @param startingRowId starting row id
   */
  public static Table createBasicTableWithDoubles(String source, int columnCount, int rowCount, int startingRowId) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.DOUBLE, false);
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = startingRowId; r < startingRowId + rowCount; r++) {
      data[r - startingRowId][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r - startingRowId][c + 1] = c * 1000.0 + r;
      }
    }

    return createTable(
      source,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      data);
  }

  public static Table createBasicTableWithFloats(String source, int columnCount, int rowCount) throws Exception {
    return createBasicTableWithFloats(source, columnCount, rowCount, 0);
  }

  /**
   * Similar to createBasicTable except `column_#` columns have Float type.
   *
   * @param source where the table should exist
   * @param columnCount number of columns
   * @param rowCount number of rows
   * @param startingRowId starting row id
   */
  public static Table createBasicTableWithFloats(String source, int columnCount, int rowCount, int startingRowId) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.FLOAT, false);
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = startingRowId; r < startingRowId + rowCount; r++) {
      data[r - startingRowId][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r - startingRowId][c + 1] = c * 1000.0f + r;
      }
    }

    return createTable(
      source,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      data);
  }

  public static Table createBasicTableWithDates(String source, int columnCount, int rowCount, int startingRowId, String startDateString, String dateFormat) throws Exception {
    LocalDateTime startDate = LocalDateTime.parse(startDateString, DateFunctionsUtils.getISOFormatterForFormatString(dateFormat));
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.DATE, false);
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = startingRowId; r < startingRowId + rowCount; r++) {
      data[r - startingRowId][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r - startingRowId][c + 1] = startDate.withDurationAdded(new Duration((r + 1) * NUMBER_OF_SECONDS_PER_DAY * 1_000L), c + 1);
      }
    }

    return createTableWithFormattedDateTime(
      source,
      EMPTY_PATHS,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      data,
      dateFormat);
  }

  public static Table createEmptyTableWithListOfType(String source, int columnCount, String listMemberTypeName) throws Exception {
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.ARRAY, false, "(" + listMemberTypeName + ")");
    }

    // Create object array of size 0 and pass that instead of passing null.
    Object[][] emptyData = new Object[0][columnCount];

    return createTable(
      source,
      EMPTY_PATHS,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      emptyData
    );
  }

  public static Table createBasicTableWithTimes(String source, int columnCount, int rowCount, int startingRowId, String startTimeString, String timeFormat) throws Exception {
    LocalDateTime startTime = LocalDateTime.parse(startTimeString, DateFunctionsUtils.getISOFormatterForFormatString(timeFormat));
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.TIME, false);
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = startingRowId; r < startingRowId + rowCount; r++) {
      data[r - startingRowId][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r - startingRowId][c + 1] = startTime.withDurationAdded(new Duration((r + 1) * 1_000_000L), c + 1);
      }
    }

    return createTableWithFormattedDateTime(
      source,
      EMPTY_PATHS,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      data,
      timeFormat);
  }

  public static Table createBasicTableWithTimestamps(String source, int columnCount, int rowCount, int startingRowId, String startTimestampString, String timestampFormat) throws Exception {
    LocalDateTime startTime = LocalDateTime.parse(startTimestampString, DateFunctionsUtils.getISOFormatterForFormatString(timestampFormat));
    ColumnInfo[] schema = new ColumnInfo[columnCount];
    schema[0] = new ColumnInfo("id", SqlTypeName.INTEGER, false);
    for (int c = 0; c < columnCount - 1; c++) {
      schema[c + 1] = new ColumnInfo("column_" + c, SqlTypeName.TIMESTAMP, false);
    }

    Object[][] data = new Object[rowCount][columnCount];
    for (int r = startingRowId; r < startingRowId + rowCount; r++) {
      data[r - startingRowId][0] = r;
      for (int c = 0; c < columnCount - 1; c++) {
        data[r - startingRowId][c + 1] = startTime.withDurationAdded(new Duration((r + 1) * NUMBER_OF_SECONDS_PER_DAY * 1_000L), c + 1);
      }
    }

    return createTableWithFormattedDateTime(
      source,
      EMPTY_PATHS,
      String.format("\"%s\"", UUID.randomUUID().toString().replace("-", "")),
      schema,
      data,
      timestampFormat);
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
          ex -> assertThat(ex).hasMessageContaining("PARSE ERROR:"),
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
      testSelectQuery(allocator, table, expectedData);
    }
  }

  public static void testSelectQuery(BufferAllocator allocator, Table table, Object[]... expectedData) throws Exception {
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

  /**
   * Runs the query and verifies the impacted records matches against `ok` and `summary`.
   * Furthermore, if `expectedData` is non-null (EMPTY_EXPECTED_DATA is allowed), then
   * it'll run a SELECT query such that you can verify data.
   */
  public static void testQueryValidateStatusSummary(BufferAllocator allocator, String query, Object[] args, Table table, boolean status, String summaryMsg, Object[]... expectedData) throws Exception {
    // Run the query and verify the impacted records is correct.
    new TestBuilder(allocator)
      .sqlQuery(query, args)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(status, summaryMsg)
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

  public static org.apache.iceberg.Table loadTable(Table table) {
    String tablePath = getDfsTestTmpSchemaLocation() + "/" + table.name.replaceAll("\"", "");
    return hadoopTables.load(tablePath);
  }

  public static org.apache.iceberg.Table loadTable(String tablePath) {
    return hadoopTables.load(tablePath);
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

  /**
   * Runs the query and verifies the number of snapshots in an iceberg table.
   */
  public static void verifyCountSnapshotQuery(BufferAllocator allocator, String tablePath, Long expectedCnt) throws Exception {
    // Run the query and verify the impacted records is correct.
    new TestBuilder(allocator)
      .sqlQuery("SELECT COUNT(*) AS cnt FROM TABLE(TABLE_SNAPSHOT('%s'))", tablePath)
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(expectedCnt)
      .build()
      .run();
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

  public static long waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }
    return current;
  }

  public static void addColumn(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column, String dataType) throws Exception {
    new TestBuilder(allocator)
      .sqlQuery("ALTER TABLE %s ADD COLUMNS (%s %s)", table.fqn, column, dataType)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "New columns added.")
      .go();
  }

  public static void addIdentityPartition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s add PARTITION FIELD IDENTITY(%s)", table.fqn, column));
  }

  public static void dropIdentityPartition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s drop PARTITION FIELD IDENTITY(%s)", table.fqn, column));
  }

  public static void addBucketPartition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s add PARTITION FIELD BUCKET(10,%s)", table.fqn, column));
  }

  public static void dropBucketPartition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s drop PARTITION FIELD BUCKET(10,%s)", table.fqn, column));
  }

  public static void addTruncate2Partition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s add PARTITION FIELD TRUNCATE(2,%s)", table.fqn, column));
  }

  public static void dropTruncate2Partition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s drop PARTITION FIELD TRUNCATE(2,%s)", table.fqn, column));
  }

  public static void addYearPartition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s add PARTITION FIELD YEAR(%s)", table.fqn, column));
  }

  public static void addDayPartition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s add PARTITION FIELD DAY(%s)", table.fqn, column));
  }

  public static void addMonthPartition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s add PARTITION FIELD MONTH(%s)", table.fqn, column));
  }

  public static void dropYearPartition(DmlQueryTestUtils.Table table, BufferAllocator allocator, String column) throws Exception {
    runSQL(String.format("ALTER TABLE %s drop PARTITION FIELD YEAR(%s)", table.fqn, column));
  }

  public static void insertIntoTable(String tableName, String columns, String values) throws Exception {
    runSQL(String.format("INSERT INTO %s%s VALUES%s", tableName, columns, values));
  }
}
