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
package com.dremio.exec.planner.sql.parser;

import static com.dremio.exec.calcite.SqlNodes.DREMIO_DIALECT;
import static com.dremio.exec.planner.sql.parser.SqlVacuum.MAX_FILE_AGE_MS_DEFAULT;
import static com.dremio.exec.planner.sql.parser.TestParserUtil.parse;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.exec.catalog.VacuumOptions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.assertj.core.data.Percentage;
import org.junit.Test;

/** Validates VACUUM TABLE sql syntax */
public class TestSqlVacuumTable {
  private final SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);

  @Test
  public void testDefaultOptions() throws SqlParseException {
    SqlNode parsed = parse("VACUUM TABLE a.b.c EXPIRE SNAPSHOTS");
    assertThat(parsed).isInstanceOf(SqlVacuumTable.class);

    SqlVacuumTable sqlVacuumTable = (SqlVacuumTable) parsed;
    VacuumOptions vacuumOptions = sqlVacuumTable.getVacuumOptions();

    long expectedSnapshotCutoff = System.currentTimeMillis() - MAX_SNAPSHOT_AGE_MS_DEFAULT;
    assertThat(vacuumOptions.getOlderThanInMillis())
        .isCloseTo(expectedSnapshotCutoff, Percentage.withPercentage(0.1));
    assertThat(vacuumOptions.getRetainLast()).isEqualTo(MIN_SNAPSHOTS_TO_KEEP_DEFAULT);

    assertThat(vacuumOptions.isExpireSnapshots()).isTrue();
    assertThat(vacuumOptions.isRemoveOrphans()).isFalse();
  }

  @Test
  public void testNonDefaultOptions() throws SqlParseException {
    SqlNode parsed =
        parse(
            "VACUUM TABLE a.b.c EXPIRE SNAPSHOTS OLDER_THAN '2023-01-01 00:00:00.000' RETAIN_LAST 5");
    assertThat(parsed).isInstanceOf(SqlVacuumTable.class);

    SqlVacuumTable sqlVacuumTable = (SqlVacuumTable) parsed;
    VacuumOptions vacuumOptions = sqlVacuumTable.getVacuumOptions();

    assertThat(sqlVacuumTable.getTable().names).isEqualTo(ImmutableList.of("a", "b", "c"));
    assertThat(vacuumOptions.getOlderThanInMillis()).isEqualTo(1672531200000L);
    assertThat(vacuumOptions.getRetainLast()).isEqualTo(5);

    assertThat(vacuumOptions.isExpireSnapshots()).isTrue();
    assertThat(vacuumOptions.isRemoveOrphans()).isFalse();
  }

  @Test
  public void testUnparseAllOptions() throws SqlParseException {
    SqlNode parsed =
        parse(
            "VACUUM TABLE a.b.c EXPIRE SNAPSHOTS OLDER_THAN '2023-01-01 00:00:00.000' RETAIN_LAST 5");
    parsed.unparse(writer, 0, 0);

    assertThat(writer.toString())
        .isEqualTo(
            "VACUUM TABLE \"a\".\"b\".\"c\" EXPIRE SNAPSHOTS \"older_than\" '2023-01-01 00:00:00.000' \"retain_last\" 5");
  }

  @Test
  public void testRemoveOrphanFilesDefaultOptions() throws SqlParseException {
    SqlNode parsed = parse("VACUUM TABLE a.b.c REMOVE ORPHAN FILES");
    assertThat(parsed).isInstanceOf(SqlVacuumTable.class);

    SqlVacuumTable sqlVacuumTable = (SqlVacuumTable) parsed;
    VacuumOptions vacuumOptions = sqlVacuumTable.getVacuumOptions();

    long expectedSnapshotCutoff = System.currentTimeMillis() - MAX_FILE_AGE_MS_DEFAULT;
    assertThat(vacuumOptions.getOlderThanInMillis())
        .isCloseTo(expectedSnapshotCutoff, Percentage.withPercentage(0.1));

    assertThat(vacuumOptions.isExpireSnapshots()).isFalse();
    assertThat(vacuumOptions.isRemoveOrphans()).isTrue();
  }
}
