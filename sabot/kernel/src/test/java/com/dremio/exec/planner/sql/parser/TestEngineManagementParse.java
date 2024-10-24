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

public class TestEngineManagementParse {
  public static Stream<Arguments> testAlterEngineParseVariants() {
    return Stream.of(
        // ALTER ENGINE Statements
        // Check syntax with quote
        Arguments.of("ALTER ENGINE \"e1\" SET (MIN_REPLICAS = 1, MAX_REPLICAS = 2)", true),
        Arguments.of("ALTER ENGINE e1 SET (MIN_REPLICAS = 1, MAX_REPLICAS = 2)", true),
        Arguments.of("ALTER ENGINE \"e 1\" SET (MIN_REPLICAS = 1, MAX_REPLICAS = 2)", true),
        Arguments.of("ALTER ENGINE \"e 1 e 1\" SET (MIN_REPLICAS = 1, MAX_REPLICAS = 2)", true),
        Arguments.of("ALTER ENGINE \"e1\" SET (MIN_REPLICAS = 0)", true),
        Arguments.of("ALTER ENGINE \"e1\" SET (MAX_REPLICAS = 2)", true),
        Arguments.of("ALTER ENGINE \"e1\" SET", false),
        Arguments.of("ALTER ENGINE \"e1\" SET()", true),
        Arguments.of("ALTER ENGINE e 1 SET (MIN_REPLICAS = 1, MAX_REPLICAS = 2)", false),

        // check correct syntax without quote
        Arguments.of("ALTER ENGINE e1 SET (MIN_REPLICAS = 1, MAX_REPLICAS = 2)", true),
        Arguments.of("ALTER ENGINE e1 SET (MAX_REPLICAS = 5, MIN_REPLICAS = 2)", true),
        Arguments.of("ALTER ENGINE e1 SET (MIN_REPLICAS = 0)", true),
        Arguments.of("ALTER ENGINE e1 SET (MAX_REPLICAS = 2)", true),
        Arguments.of("ALTER ENGINE e1 SET", false),
        Arguments.of("ALTER ENGINE e1 SET()", true),

        // You can't finish with a comma
        Arguments.of("ALTER ENGINE e1 SET (MIN_REPLICAS = 0,)", false),
        Arguments.of("ALTER ENGINE e1 SET (MAX_REPLICAS = 0,)", false),

        // No comma
        Arguments.of("ALTER ENGINE e1 SET (MIN_REPLICAS = 1 MAX_REPLICAS = 2)", false),
        Arguments.of("ALTER ENGINE e1 SET (MAX_REPLICAS = 2 MIN_REPLICAS = 2)", false),

        // Missing mandatory SET
        Arguments.of("ALTER ENGINE e1 MIN_REPLICAS = 1 MAX_REPLICAS = 2)", false),
        Arguments.of("ALTER ENGINE e1 MIN_REPLICAS = 0)", false),
        Arguments.of("ALTER ENGINE e1 MAX_REPLICAS = 2)", false),
        Arguments.of("ALTER ENGINE e1", false),

        // Missing = around MIN_REPLICAS / MAX_REPLICAS
        Arguments.of("ALTER ENGINE e1 SET (MIN_REPLICAS 1 MAX_REPLICAS 2)", false),
        Arguments.of("ALTER ENGINE e1 SET (MIN_REPLICAS 0)", false),
        Arguments.of("ALTER ENGINE e1 SET (MAX_REPLICAS 2)", false),
        Arguments.of("ALTER ENGINE", false), // No engine id
        Arguments.of("ALTER ENGINE \"e1\" MIN_REPLICAS MAX_REPLICAS", false), // No value
        Arguments.of("ALTER ENGINE \"e1\" MIN_REPLICAS", false), // No value
        Arguments.of("ALTER ENGINE \"e1\" MAX_REPLICAS", false) // No value
        );
  }

  @ParameterizedTest
  @MethodSource("testAlterEngineParseVariants")
  public void testAlterEngine(String query, boolean shouldSucceed) throws SqlParseException {
    if (!shouldSucceed) {
      assertThrows(SqlParseException.class, () -> parse(query));
    } else {
      parse(query);
    }
  }
}
