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

import static com.dremio.exec.planner.sql.parser.TestParserUtil.parse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestIngestionParse {
  public static Stream<Arguments> data() {
    return Stream.of(
        // ALTER PIPE Statements
        Arguments.of("ALTER PIPE p1 AS COPY INTO tb1 FROM 'anywhere'", true),
        Arguments.of(
            "ALTER PIPE p1 DEDUPE_LOOKBACK_PERIOD 5 AS COPY INTO tb1 FROM 'anywhere'", true),
        Arguments.of(
            "ALTER PIPE p1 AS COPY INTO s1.tb1 FROM 'somewhere' FILE_FORMAT \'csv\' (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true')",
            true),
        Arguments.of("ALTER PIPE p1 SET PIPE_EXECUTION_RUNNING = TRUE", true),
        Arguments.of("ALTER PIPE p1 SET PIPE_EXECUTION_RUNNING = FALSE", true),
        Arguments.of("ALTER a", false), // No PIPE keyword
        Arguments.of("ALTER PIPE a", false), // No COPY INTO command
        Arguments.of("ALTER PIPE p1 AS COPY INTO tb1", false), // Invalid COPY INTO SqlNode
        Arguments.of("ALTER PIPE p1 SET PIPE_EXECUTION_RUNNING = VALUE", false),
        Arguments.of(
            "ALTER PIPE p1 AS COPY INTO tb1 FROM 'anywhere' DEDUPE_LOOKBACK_PERIOD 5",
            false), // DEDUPE_LOOKBACK_PERIOD must be defined before COPY INTO
        Arguments.of(
            "ALTER PIPE p1 DEDUPE_LOOKBACK_PERIOD = 5 AS COPY INTO tb1 FROM 'anywhere'",
            false), // '=' Symbol is not allowed
        Arguments.of(
            "ALTER PIPE p1 DEDUPE_LOOKBACK_PERIOD 5 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere'",
            false), // NOTIFICATION_PROVIDER or NOTIFICATION_QUEUE_REF cannot be changed using ALTER
        // PIPE
        Arguments.of(
            "ALTER PIPE p1 AS COPY INTO s1.tb1 FROM 'somewhere' Files('1', '2') (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true')",
            false),
        Arguments.of(
            "ALTER PIPE p1 AS COPY INTO s1.tb1 FROM 'somewhere' Files('1', '2') FILE_FORMAT \'csv\' (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true')",
            false),
        Arguments.of(
            "ALTER PIPE p1 AS COPY INTO s1.tb1 FROM 'somewhere' FILE_FORMAT \'csv\' (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true', ON_ERROR 'ABORT')",
            false),
        // CREATE PIPE Statements
        Arguments.of("CREATE PIPE p1 AS COPY INTO tb1 FROM 'anywhere'", true),
        Arguments.of("CREATE PIPE IF NOT EXISTS p1 AS COPY INTO tb1 FROM 'anywhere'", true),
        Arguments.of(
            "CREATE PIPE p1 DEDUPE_LOOKBACK_PERIOD 5 AS COPY INTO tb1 FROM 'anywhere'", true),
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 DEDUPE_LOOKBACK_PERIOD 5 AS COPY INTO tb1 FROM 'anywhere'",
            true),
        Arguments.of(
            "CREATE PIPE p1 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere'",
            true),
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere'",
            true),
        Arguments.of(
            "CREATE PIPE p1 DEDUPE_LOOKBACK_PERIOD 5 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere'",
            true),
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 DEDUPE_LOOKBACK_PERIOD 5 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere'",
            true),
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 DEDUPE_LOOKBACK_PERIOD 5 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere' FILE_FORMAT \'csv\' (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true')",
            true),
        Arguments.of("CREATE a", false), // No PIPE keyword
        Arguments.of("CREATE PIPE a", false), // No COPY INTO command
        Arguments.of(
            "CREATE PIPE source.folder.p1 AS COPY INTO tb1 FROM 'anywhere'",
            false), // Compound Identifiers are not allowed for pipe name
        Arguments.of(
            "CREATE OR REPLACE PIPE p1 AS COPY INTO tb1 FROM 'anywhere'",
            false), // OR REPLACE cannot be used with CREATE PIPE
        Arguments.of(
            "CREATE PIPE p1 DEDUPE_LOOKBACK_PERIOD -2 AS COPY INTO tb1 FROM 'anywhere'",
            false), // Negative DEDUPE_LOOKBACK_PERIOD can not be set
        Arguments.of(
            "CREATE PIPE p1 NOTIFICATION_PROVIDER = AWS_SQS NOTIFICATION_QUEUE_REFERENCE = \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere'",
            false), // '=' Symbol is not allowed
        Arguments.of(
            "CREATE PIPE p1 NOTIFICATION_PROVIDER AWS_SQS AS COPY INTO tb1 FROM 'anywhere'",
            false), // NOTIFICATION_PROVIDER must be followed by NOTIFICATION_QUEUE_REFERENCE
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 NOTIFICATION_PROVIDER AWS_SQS DEDUPE_LOOKBACK_PERIOD 5 AS COPY INTO tb1 FROM 'anywhere'",
            false), // NOTIFICATION_PROVIDER must be after DEDUPE_LOOKBACK_PERIOD
        Arguments.of(
            "CREATE PIPE p1 NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere'",
            false), // NOTIFICATION_PROVIDER must be provided before NOTIFICATION_QUEUE_REFERENCE
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" DEDUPE_LOOKBACK_PERIOD 5 AS COPY INTO tb1 FROM 'anywhere'",
            false), // NOTIFICATION_QUEUE_REFERENCE must be after NOTIFICATION_PROVIDER
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" NOTIFICATION_PROVIDER AWS_SQS AS COPY INTO tb1 FROM 'anywhere'",
            false), // NOTIFICATION_QUEUE_REFERENCE must be after NOTIFICATION_PROVIDER
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 DEDUPE_LOOKBACK_PERIOD 5 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere' Files('1', '2') (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true')",
            false),
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 DEDUPE_LOOKBACK_PERIOD 5 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere' Files('1', '2') FILE_FORMAT \'csv\' (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true')",
            false),
        Arguments.of(
            "CREATE PIPE IF NOT EXISTS p1 DEDUPE_LOOKBACK_PERIOD 5 NOTIFICATION_PROVIDER AWS_SQS NOTIFICATION_QUEUE_REFERENCE \"arn:aws:sqs:us-east-2:444455556666:queue1\" AS COPY INTO tb1 FROM 'anywhere' FILE_FORMAT \'csv\' (RECORD_DELIMITER '\n', EMPTY_AS_NULL 'true', TRIM_SPACE 'true', ON_ERROR 'ABORT')",
            false),
        // DESCRIBE PIPE Statements
        Arguments.of("DESCRIBE PIPE p1", true),
        Arguments.of("DESC PIPE p1", true),
        Arguments.of("DESC PIPE ", false),
        // DROP PIPE Statements
        Arguments.of("DROP PIPE p1", true),
        Arguments.of("DROP p1", false),
        // SHOW PIPES Statements
        Arguments.of("SHOW PIPES", false),
        // TRIGGER PIPE Statements
        Arguments.of("TRIGGER PIPE p1", true),
        Arguments.of("TRIGGER PIPE p1 FOR BATCH batchid1", true),
        Arguments.of("TRIGGER PIPE", false),
        Arguments.of("TRIGGER PIPE p1 FOR batchid1", false));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCreatePipeParseVariants(String query, boolean shouldSucceed)
      throws SqlParseException {
    if (!shouldSucceed) {
      assertThrows(SqlParseException.class, () -> parse(query));
    } else {
      parse(query);
    }
  }
}
