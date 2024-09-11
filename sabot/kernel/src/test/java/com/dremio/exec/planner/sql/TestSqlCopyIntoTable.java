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

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.exec.planner.sql.parser.SqlCopyIntoTable;
import com.dremio.exec.util.ColumnUtils;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class TestSqlCopyIntoTable {

  private final ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100);

  @Test
  public void testExtendTableWithErrorColumn() {
    String query = "COPY INTO target_table FROM '@S3/tmp/'";
    SqlCopyIntoTable sqlNode = parseQuery(query);
    sqlNode.extendTableWithDataFileSystemColumns();
    assertThat(sqlNode.isTableExtended()).isFalse();
    assertThat(sqlNode.getTargetTable().getKind()).isEqualTo(SqlKind.IDENTIFIER);

    query = "COPY INTO target_table FROM '@S3/tmp/' (ON_ERROR 'continue')";
    sqlNode = parseQuery(query);
    sqlNode.extendTableWithDataFileSystemColumns();
    assertThat(sqlNode.isTableExtended()).isTrue();
    assertThat(sqlNode.getTargetTable().getKind()).isEqualTo(SqlKind.EXTEND);
  }

  @Test
  public void testStorageLocation() {
    String location = "@S3/tmp/dir/";
    String query = String.format("COPY INTO target_table FROM '%s'", location);
    SqlCopyIntoTable sqlNode = parseQuery(query);
    assertThat(sqlNode.getStorageLocation()).isEqualTo(location);
  }

  @Test
  public void testFiles() {
    final List<String> files = ImmutableList.of("file1", "file2", "file3", "file4", "file5");
    String query =
        String.format(
            "COPY INTO target_table FROM '@S3/tmp/dir' FILES (%s)",
            files.stream().map(f -> String.format("'%s'", f)).collect(Collectors.joining(",")));
    SqlCopyIntoTable sqlNode = parseQuery(query);
    assertThat(sqlNode.getFiles()).isEqualTo(files);
  }

  @Test
  public void testFilePattern() {
    String regex = "^a\\d*\\.csv$";
    String query = String.format("COPY INTO target_table from '@S3/tmp/dir' REGEX '%s'", regex);
    SqlCopyIntoTable sqlNode = parseQuery(query);
    assertThat(sqlNode.getFilePattern().isPresent()).isTrue();
    assertThat(sqlNode.getFilePattern().get()).isEqualTo(regex);
  }

  @Test
  public void testFileFormat() {
    String fileFormat = "json";
    String query =
        String.format("COPY INTO target_table from '@S3/tmp/dir' FILE_FORMAT '%s'", fileFormat);
    SqlCopyIntoTable sqlNode = parseQuery(query);
    assertThat(sqlNode.getFileFormat().isPresent()).isTrue();
    assertThat(sqlNode.getFileFormat().get()).isEqualTo(fileFormat);
  }

  @Test
  public void testFormatOptions() {
    final List<String> options =
        ImmutableList.of(
            "TRIM_SPACE",
            "EMPTY_AS_NULL",
            "RECORD_DELIMITER",
            "FIELD_DELIMITER",
            "DATE_FORMAT",
            "TIME_FORMAT",
            "TIMESTAMP_FORMAT",
            "QUOTE_CHAR",
            "ESCAPE_CHAR");
    final List<String> values =
        ImmutableList.of(
            "true",
            "true",
            "\n",
            "\t",
            "DD-MM-YYYY",
            "HH24:MI:SS",
            "DD-MM-YYYY HH24:MI:SS",
            "\"",
            "|");
    String query =
        String.format(
            "COPY INTO target_table from '@S3/tmp/dir' (%s)",
            IntStream.range(0, options.size())
                .mapToObj(i -> String.format("%s '%s'", options.get(i), values.get(i)))
                .collect(Collectors.joining(",")));
    SqlCopyIntoTable sqlNode = parseQuery(query);
    assertThat(sqlNode.getOptionsList()).isEqualTo(options);
    assertThat(sqlNode.getOptionsValueList()).isEqualTo(values);
  }

  @Test
  public void testTransformations() {
    String query =
        "COPY INTO target_table FROM (SELECT isnottrue(e), length(b), concat(b, '_test') FROM '@S3/tmp/dir')";
    SqlCopyIntoTable sqlNode = parseQuery(query);
    assertThat(sqlNode.getMappings()).isEmpty();
    List<SqlNode> selectNodes = sqlNode.getSelectNodes();
    assertThat(selectNodes).hasSize(3);
    assertThat(selectNodes.stream().allMatch(n -> n.getKind().equals(SqlKind.OTHER_FUNCTION)))
        .isTrue();
    assertThat(selectNodes)
        .extracting(n -> ((SqlBasicCall) n).getOperator().getName())
        .containsExactly("isnottrue", "length", "concat");
    assertThat(
            selectNodes.stream()
                .map(n -> ((SqlBasicCall) n).getOperandList())
                .flatMap(List::stream)
                .filter(o -> o.getKind().equals(SqlKind.IDENTIFIER))
                .map(o -> ((SqlIdentifier) o).getSimple())
                .collect(Collectors.toList()))
        .containsExactly(
            ColumnUtils.VIRTUAL_COLUMN_PREFIX + "E",
            ColumnUtils.VIRTUAL_COLUMN_PREFIX + "B",
            ColumnUtils.VIRTUAL_COLUMN_PREFIX + "B");

    query = "COPY INTO target_table (a, b, c) FROM (SELECT e, d, c FROM '@S3/tmp/dir')";
    sqlNode = parseQuery(query);
    assertThat(sqlNode.getMappings()).hasSize(3);
    assertThat(sqlNode.getMappings())
        .extracting(n -> ((SqlIdentifier) n).getSimple())
        .containsExactly("a", "b", "c");
    selectNodes = sqlNode.getSelectNodes();
    assertThat(selectNodes).hasSize(3);
    assertThat(selectNodes.stream().allMatch(n -> n.getKind().equals(SqlKind.IDENTIFIER))).isTrue();
    assertThat(selectNodes)
        .extracting(n -> ((SqlIdentifier) n).getSimple())
        .containsExactly(
            ColumnUtils.VIRTUAL_COLUMN_PREFIX + "E",
            ColumnUtils.VIRTUAL_COLUMN_PREFIX + "D",
            ColumnUtils.VIRTUAL_COLUMN_PREFIX + "C");
  }

  private SqlCopyIntoTable parseQuery(@NotNull String query) {
    return (SqlCopyIntoTable) SqlConverter.parseSingleStatementImpl(query, parserConfig, false);
  }
}
