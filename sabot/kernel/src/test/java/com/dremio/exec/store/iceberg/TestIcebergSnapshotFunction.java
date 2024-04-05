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

import static com.dremio.exec.ExecConstants.TARGET_BATCH_RECORDS_MAX;
import static com.dremio.exec.ExecConstants.TARGET_BATCH_RECORDS_MIN;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.TestBuilder;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.util.JodaDateUtility;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.iceberg.model.IcebergBaseCommand;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Snapshot;
import org.junit.Test;

/** Test class for iceberg snapshot functions select * from table(table_snapshot('table')) */
public class TestIcebergSnapshotFunction extends IcebergMetadataTestTable {

  @Test
  public void testTableSnapshotSchema() throws Exception {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("committed_at"),
            Types.required(TypeProtos.MinorType.TIMESTAMP)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("snapshot_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("parent_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("operation"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("manifest_list"),
            Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(SchemaPath.getSimplePath("summary"), Types.required(TypeProtos.MinorType.LIST)));
    expectedSchema(
        expectedSchema,
        String.format(
            "SELECT * FROM table(table_snapshot('\"%s\".\"%s\"')) limit 1",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME));
  }

  @Test
  public void testInvalidColumnTypeTableSnapshotSchema() {
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("committed_at"),
            Types.required(TypeProtos.MinorType.TIMESTAMP)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("snapshot_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("parent_id"), Types.required(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("operation"), Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("manifest_list"),
            Types.required(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(
        Pair.of(
            SchemaPath.getSimplePath("summary"),
            Types.required(TypeProtos.MinorType.STRUCT))); // Struct instead of List
    assertThatThrownBy(
            () ->
                expectedSchema(
                    expectedSchema,
                    String.format(
                        "SELECT * FROM table(table_snapshot('\"%s\".\"%s\"')) limit 1",
                        TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME)))
        .hasMessageContaining("Schema path or type mismatch for")
        .isInstanceOf(Exception.class);
  }

  @Test
  public void testTableSnapshots() throws Exception {
    Iterable<Snapshot> snapshots = getSnapshots();
    Snapshot snapshot1 = snapshots.iterator().next();
    LocalDateTime dateTime =
        Instant.ofEpochMilli(snapshot1.timestampMillis())
            .atZone(ZoneId.systemDefault()) // default zone
            .toLocalDateTime();
    String[] expectedColumns = {
      "committed_at", "snapshot_id", "parent_id", "operation", "manifest_list"
    };
    Object[] values = {
      JodaDateUtility.javaToJodaLocalDateTime(dateTime),
      snapshot1.snapshotId(),
      snapshot1.parentId(),
      snapshot1.operation(),
      snapshot1.manifestListLocation()
    };
    queryAndMatchResults(
        String.format(
            "SELECT committed_at,snapshot_id,parent_id,operation,manifest_list FROM table(table_snapshot('\"%s\".\"%s\"')) limit 1",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME),
        expectedColumns,
        values);
  }

  @Test
  public void testTableSnapshotsCount() throws Exception {
    queryAndMatchResults(
        String.format(
            "SELECT count(*) as snapshot_count FROM table(table_snapshot('\"%s\".\"%s\"'))",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME),
        new String[] {"snapshot_count"},
        new Object[] {1L});
  }

  @Test
  public void testTableSnapshotsCountWithMultipleBatches() throws Exception {
    int numberOfNewSnapshots = 10;
    int maxBatchSize = 8;
    int minBatchSize = 4;

    try (AutoCloseable closeableForMinSetting =
        setSystemOptionWithAutoReset(
            TARGET_BATCH_RECORDS_MIN.getOptionName(), Integer.toString(minBatchSize))) {
      try (AutoCloseable closeableForMaxSetting =
          setSystemOptionWithAutoReset(
              TARGET_BATCH_RECORDS_MAX.getOptionName(), Integer.toString(maxBatchSize))) {
        for (int i = 0; i < numberOfNewSnapshots; ++i) {
          // note: table is reset after each test case
          String insertCommand =
              String.format(
                  "insert into %s.%s values (%d, '%s', %f)",
                  TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME, i, "row number " + i, i + 0.5);
          runSQL(insertCommand);
        }
        queryAndMatchResults(
            String.format(
                "SELECT count(*) as snapshot_count FROM table(table_snapshot('\"%s\".\"%s\"'))",
                TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME),
            new String[] {"snapshot_count"},
            new Object[] {(long) numberOfNewSnapshots + 1});
      }
    }
  }

  @Test
  public void incorrectName() {
    String query = "SELECT count(*) as k FROM table(table_snapshot('blah'))";
    assertThatThrownBy(() -> runSQL(query)).hasMessageContaining("not found");
  }

  @Test
  public void testJobIdStoredInTableMetadata() throws Exception {
    String appendQuery =
        String.format(
            "insert into %s.%s values (%d, '%s', %f)",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME, 0, "append", 0.1);
    String overwriteQuery =
        String.format(
            "update %s.%s set c2='%s' where c1=%d",
            TEMP_SCHEMA_HADOOP, METADATA_TEST_TABLE_NAME, "overwrite", 0);

    String appendJobId = runQueryAndReturnJobId(appendQuery);
    String overwriteJobId = runQueryAndReturnJobId(overwriteQuery);

    String jobIdPropertyQuery =
        String.format(
            "SELECT operation, t.f.\"key\", t.f.\"value\" FROM ("
                + "SELECT operation, flatten(summary) as f FROM table(table_snapshot('\"%s\".\"%s\"'))"
                + ") t where t.f.\"key\" like '%s' ",
            TEMP_SCHEMA_HADOOP,
            METADATA_TEST_TABLE_NAME,
            IcebergBaseCommand.DREMIO_JOB_ID_ICEBERG_PROPERTY);

    new TestBuilder(allocator)
        .sqlQuery(jobIdPropertyQuery)
        .unOrdered()
        .baselineColumns("operation", "key", "value")
        .baselineValues("append", IcebergBaseCommand.DREMIO_JOB_ID_ICEBERG_PROPERTY, appendJobId)
        .baselineValues(
            "overwrite", IcebergBaseCommand.DREMIO_JOB_ID_ICEBERG_PROPERTY, overwriteJobId)
        .go();
  }

  private static String runQueryAndReturnJobId(String query) throws Exception {
    List<QueryDataBatch> queryResult = testSqlWithResults(query);
    UserBitShared.QueryId queryId = queryResult.get(0).getHeader().getQueryId();
    return QueryIdHelper.getQueryId(queryId);
  }
}
