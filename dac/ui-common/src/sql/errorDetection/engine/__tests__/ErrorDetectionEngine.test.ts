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

import { runGoldenTests } from "../../../../test/GoldenTest";
import { LiveEditParsingEngine } from "../../../parser/engine/LiveEditParsingEngine";
import { SQLError } from "../../types/SQLError";
import { ErrorDetectionEngine } from "../ErrorDetectionEngine";
import testExpecteds from "./resources/ErrorDetectionEngine.test.json";

type Expecteds = {
  [testName: string]: TestResult;
};

type TestResult = {
  // We omit rawMessage since it could change often as new keywords are introduced
  // and is not necessary to validate our functionality
  sqlErrors: Omit<SQLError, "rawMessage">[];
};

const generateTestResult = (linesContent: string[]): TestResult => {
  const liveEditParsingEngine = new LiveEditParsingEngine();
  const errorDetectionEngine = new ErrorDetectionEngine(liveEditParsingEngine);
  const sqlErrorsNoRawMessage = errorDetectionEngine
    .detectSqlErrors(linesContent)
    .map((error) => {
      const { rawMessage, ...rest } = error;
      return rest;
    });
  return {
    sqlErrors: sqlErrorsNoRawMessage,
  };
};

describe("ErrorDetectionEngine", () => {
  describe("detectSqlErrors", () => {
    const testCases = {
      eofEmptyQuery: [""],
      eofNonEmptyQuery: ["DROP VIEW"],
      eofNoViableAlt: ["DROP"],
      noViableAltMulti: ["EXPLAIN PLAN WITH AS XML"],
      noViableAltMulti2: ["GRANT OWNERSHIP TO"],
      identifierOnly: ["DROP VIEW IF EXISTS"],
      identifierOrExpression: ["SELECT * FROM tbl WHERE"],
      noSelectList: ["SELECT"],
      missingToken: ["DELETE tbl"],
      extraneousToken: ["DELETE FROM TABLE tbl"],
      badToken: ["SELECT 1 # wrong comment style"],
      singleTokenMultiLine: ["ALTER 't", "bl' FROM"],
      multiTokenMultiLine: ["EXPLAIN PLAN WITH ", "AS XML"],
    };
    runGoldenTests(
      testCases,
      testExpecteds as Expecteds,
      __filename,
      generateTestResult,
    );
  });
});
