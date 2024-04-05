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
import static com.dremio.exec.planner.sql.parser.TestParserUtil.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Test;

public class TestSqlIngestion {
  private SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);

  @Test
  public void testAlterPipe() throws SqlParseException {
    SqlNode parsed = parse("ALTER PIPE p1 AS COPY INTO tb1 FROM 'anywhere'");
    assertTrue(parsed instanceof SqlAlterPipe);
    SqlAlterPipe sqlAlterPipe = (SqlAlterPipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "ALTER PIPE \"p1\" AS COPY INTO \"tb1\" FROM 'anywhere'";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlAlterPipe.getPipeName().toString());
    assertEquals(
        "Dedup period should not be set.", Optional.empty(), sqlAlterPipe.getDedupLookbackPeriod());
    assertEquals(
        "Target table name does not match.",
        "tb1",
        sqlAlterPipe.getSqlCopyInto().getTargetTable().toString());
  }

  @Test
  public void testAlterPipeWithDedupPeriod() throws SqlParseException {
    SqlNode parsed =
        parse("ALTER PIPE p1 DEDUPE_LOOKBACK_PERIOD 4 AS COPY INTO tb1 FROM 'anywhere'");
    assertTrue(parsed instanceof SqlAlterPipe);
    SqlAlterPipe sqlCreatePipe = (SqlAlterPipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "ALTER PIPE \"p1\" DEDUPE_LOOKBACK_PERIOD 4 AS COPY INTO \"tb1\" FROM 'anywhere'";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlCreatePipe.getPipeName().toString());
    assertEquals(
        "Dedupe period does not match.",
        Integer.valueOf(4),
        sqlCreatePipe.getDedupLookbackPeriod().get());
    assertEquals(
        "Target table name does not match.",
        "tb1",
        sqlCreatePipe.getSqlCopyInto().getTargetTable().toString());
  }

  @Test
  public void testAlterPipeWithFullCopyInto() throws SqlParseException {
    SqlNode parsed =
        parse(
            "ALTER PIPE p1 AS COPY INTO sourcename1.targettable1 FROM 'somewhere' FILE_FORMAT \'csv\' (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true')");
    assertTrue(parsed instanceof SqlAlterPipe);
    SqlAlterPipe sqlCreatePipe = (SqlAlterPipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "ALTER PIPE \"p1\" AS COPY INTO \"sourcename1\".\"targettable1\" FROM 'somewhere' FILE_FORMAT \'csv\' ('RECORD_DELIMITER' '\n', 'EMPTY_AS_NULL' 'true', 'TRIM_SPACE' 'true')";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlCreatePipe.getPipeName().toString());
    assertEquals(
        "Dedupe period should not be set.",
        Optional.empty(),
        sqlCreatePipe.getDedupLookbackPeriod());
    assertEquals(
        "COPY INTO Target table name does not match.",
        "sourcename1.targettable1",
        sqlCreatePipe.getSqlCopyInto().getTargetTable().toString());
    assertEquals(
        "COPY INTO Storage location does not match.",
        "somewhere",
        sqlCreatePipe.getSqlCopyInto().getStorageLocation());
    assertEquals(
        "COPY INTO File format does not match.",
        "csv",
        sqlCreatePipe.getSqlCopyInto().getFileFormat().get());
    assertEquals(
        "COPY INTO Files do not match.",
        Arrays.asList(),
        sqlCreatePipe.getSqlCopyInto().getFiles());
    assertEquals(
        "COPY INTO Options list does not match.",
        Arrays.asList("RECORD_DELIMITER", "EMPTY_AS_NULL", "TRIM_SPACE"),
        sqlCreatePipe.getSqlCopyInto().getOptionsList());
    assertEquals(
        "COPY INTO Options value list does not match.",
        Arrays.asList("\n", "true", "true"),
        sqlCreatePipe.getSqlCopyInto().getOptionsValueList());
  }

  @Test
  public void testAlterPipeWithMalformedCopyInto() throws SqlParseException {
    assertThrows(
        "FILES and REGEX options are not allowed for PIPE commands.",
        SqlParseException.class,
        () ->
            parse(
                "ALTER PIPE \"p1\" AS COPY INTO \"sourcename1\".\"targettable1\" FROM 'somewhere' FILES('a.csv', 'b.csv') FILE_FORMAT \'csv\' ('RECORD_DELIMITER' '\n', 'EMPTY_AS_NULL' 'true', 'TRIM_SPACE' 'true')"));
  }

  @Test
  public void testCreatePipe() throws SqlParseException {
    SqlNode parsed = parse("CREATE PIPE p1 AS COPY INTO tb1 FROM 'anywhere'");
    assertTrue(parsed instanceof SqlCreatePipe);
    SqlCreatePipe sqlCreatePipe = (SqlCreatePipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "CREATE PIPE \"p1\" AS COPY INTO \"tb1\" FROM 'anywhere'";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlCreatePipe.getPipeName().toString());
    assertEquals(
        "Dedupe period should not be set.",
        Optional.empty(),
        sqlCreatePipe.getDedupLookbackPeriod());
    assertEquals(
        "Target table name does not match.",
        "tb1",
        sqlCreatePipe.getSqlCopyInto().getTargetTable().toString());
  }

  @Test
  public void testCreatePipeWithDedupPeriod() throws SqlParseException {
    SqlNode parsed =
        parse("CREATE PIPE p1 DEDUPE_LOOKBACK_PERIOD 4 AS COPY INTO tb1 FROM 'anywhere'");
    assertTrue(parsed instanceof SqlCreatePipe);
    SqlCreatePipe sqlCreatePipe = (SqlCreatePipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "CREATE PIPE \"p1\" DEDUPE_LOOKBACK_PERIOD 4 AS COPY INTO \"tb1\" FROM 'anywhere'";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlCreatePipe.getPipeName().toString());
    assertEquals(
        "Dedupe period does not match.",
        Integer.valueOf(4),
        sqlCreatePipe.getDedupLookbackPeriod().get());
    assertEquals(
        "Target table name does not match.",
        "tb1",
        sqlCreatePipe.getSqlCopyInto().getTargetTable().toString());
  }

  @Test
  public void testCreatePipeWithIfNotExists() throws SqlParseException {
    SqlNode parsed = parse("CREATE PIPE IF NOT EXISTS p1 AS COPY INTO tb1 FROM 'anywhere'");
    assertTrue(parsed instanceof SqlCreatePipe);
    SqlCreatePipe sqlCreatePipe = (SqlCreatePipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "CREATE PIPE IF NOT EXISTS \"p1\" AS COPY INTO \"tb1\" FROM 'anywhere'";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlCreatePipe.getPipeName().toString());
    assertEquals(
        "Dedupe period should not be set.",
        Optional.empty(),
        sqlCreatePipe.getDedupLookbackPeriod());
    assertEquals(
        "Target table name does not match.",
        "tb1",
        sqlCreatePipe.getSqlCopyInto().getTargetTable().toString());
  }

  @Test
  public void testCreatePipeWithNotificationProvider() throws SqlParseException {
    SqlNode parsed =
        parse(
            "CREATE PIPE p1 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere'");
    assertTrue(parsed instanceof SqlCreatePipe);
    SqlCreatePipe sqlCreatePipe = (SqlCreatePipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "CREATE PIPE \"p1\" NOTIFICATION_PROVIDER \"AWS_SQS\" NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO \"tb1\" FROM 'anywhere'";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlCreatePipe.getPipeName().toString());
    assertEquals(
        "Dedupe period should not be set.",
        Optional.empty(),
        sqlCreatePipe.getDedupLookbackPeriod());
    assertEquals(
        "Notification provider does not match.",
        "AWS_SQS",
        sqlCreatePipe.getNotificationProvider().get().toString());
    assertEquals(
        "Notification queue reference does not match.",
        "arn:aws:sqs:us-east-2:444455556666:queue1",
        sqlCreatePipe.getNotificationQueueRef().get().toString());
    assertEquals(
        "Target table name does not match.",
        "tb1",
        sqlCreatePipe.getSqlCopyInto().getTargetTable().toString());
  }

  @Test
  public void testCreatePipeWithDedupPeriodAndNotificationProvider() throws SqlParseException {
    SqlNode parsed =
        parse(
            "CREATE PIPE p1 DEDUPE_LOOKBACK_PERIOD 5 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere'");
    assertTrue(parsed instanceof SqlCreatePipe);
    SqlCreatePipe sqlCreatePipe = (SqlCreatePipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "CREATE PIPE \"p1\" DEDUPE_LOOKBACK_PERIOD 5 NOTIFICATION_PROVIDER \"AWS_SQS\" NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO \"tb1\" FROM 'anywhere'";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlCreatePipe.getPipeName().toString());
    assertEquals(
        "Dedupe period should not be set.",
        Integer.valueOf(5),
        sqlCreatePipe.getDedupLookbackPeriod().get());
    assertEquals(
        "Notification provider does not match.",
        "AWS_SQS",
        sqlCreatePipe.getNotificationProvider().get().toString());
    assertEquals(
        "Notification queue reference does not match.",
        "arn:aws:sqs:us-east-2:444455556666:queue1",
        sqlCreatePipe.getNotificationQueueRef().get().toString());
    assertEquals(
        "Target table name does not match.",
        "tb1",
        sqlCreatePipe.getSqlCopyInto().getTargetTable().toString());
  }

  @Test
  public void testCreatePipeWithFullCopyInto() throws SqlParseException {
    SqlNode parsed =
        parse(
            "CREATE PIPE p1 AS COPY INTO sourcename1.targettable1 FROM 'somewhere' FILE_FORMAT \'csv\' (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true')");
    assertTrue(parsed instanceof SqlCreatePipe);
    SqlCreatePipe sqlCreatePipe = (SqlCreatePipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString =
        "CREATE PIPE \"p1\" AS COPY INTO \"sourcename1\".\"targettable1\" FROM 'somewhere' FILE_FORMAT \'csv\' ('RECORD_DELIMITER' '\n', 'EMPTY_AS_NULL' 'true', 'TRIM_SPACE' 'true')";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlCreatePipe.getPipeName().toString());
    assertEquals(
        "Dedupe period should not be set.",
        Optional.empty(),
        sqlCreatePipe.getDedupLookbackPeriod());
    assertEquals(
        "COPY INTO Target table name does not match.",
        "sourcename1.targettable1",
        sqlCreatePipe.getSqlCopyInto().getTargetTable().toString());
    assertEquals(
        "COPY INTO Storage location does not match.",
        "somewhere",
        sqlCreatePipe.getSqlCopyInto().getStorageLocation());
    assertEquals(
        "COPY INTO File format does not match.",
        "csv",
        sqlCreatePipe.getSqlCopyInto().getFileFormat().get());
    assertEquals(
        "COPY INTO Files do not match.",
        Arrays.asList(),
        sqlCreatePipe.getSqlCopyInto().getFiles());
    assertEquals(
        "COPY INTO Options list does not match.",
        Arrays.asList("RECORD_DELIMITER", "EMPTY_AS_NULL", "TRIM_SPACE"),
        sqlCreatePipe.getSqlCopyInto().getOptionsList());
    assertEquals(
        "COPY INTO Options value list does not match.",
        Arrays.asList("\n", "true", "true"),
        sqlCreatePipe.getSqlCopyInto().getOptionsValueList());
  }

  @Test
  public void testCreatePipeWithMalformedCopyInto() throws SqlParseException {
    assertThrows(
        "FILES and REGEX options are not allowed for PIPE commands.",
        SqlParseException.class,
        () ->
            parse(
                "CREATE PIPE p1 AS COPY INTO sourcename1.targettable1 FROM 'somewhere' FILES('a.csv', 'b.csv') FILE_FORMAT \'csv\' (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true')"));
  }

  @Test
  public void testDropPipe() throws SqlParseException {
    SqlNode parsed = parse("DROP PIPE p1");
    assertTrue(parsed instanceof SqlDropPipe);
    SqlDropPipe sqlDescribePipe = (SqlDropPipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "DROP PIPE \"p1\"";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlDescribePipe.getPipeName().toString());
  }

  @Test
  public void testTriggerPipe() throws SqlParseException {
    SqlNode parsed = parse("TRIGGER PIPE p1");
    assertTrue(parsed instanceof SqlTriggerPipe);
    SqlTriggerPipe sqlTriggerPipe = (SqlTriggerPipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "TRIGGER PIPE \"p1\"";
    assertTrue(
        actualString.startsWith(expectedUnparsedString)); // TRIGGER PIPE adds a generated batchId.

    assertEquals("Pipe name does not match.", "p1", sqlTriggerPipe.getPipeName().toString());
  }

  @Test
  public void testTriggerPipeWithBatchId() throws SqlParseException {
    SqlNode parsed = parse("TRIGGER PIPE p1 FOR BATCH batchid1");
    assertTrue(parsed instanceof SqlTriggerPipe);
    SqlTriggerPipe sqlTriggerPipe = (SqlTriggerPipe) parsed;

    parsed.unparse(writer, 0, 0);
    String actualString = writer.toString();
    String expectedUnparsedString = "TRIGGER PIPE \"p1\" FOR BATCH \"batchid1\"";
    assertEquals(expectedUnparsedString, actualString);

    assertEquals("Pipe name does not match.", "p1", sqlTriggerPipe.getPipeName().toString());
    assertEquals("Batch ID does not match.", "batchid1", sqlTriggerPipe.getBatchId().toString());
  }
}
