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

import { LiveEditParser } from "../../../../../target/generated-sources/antlr/LiveEditParser";
import { runGoldenTests } from "../../../../test/GoldenTest";
import { TokenSuggestions } from "../TokenSuggestions";
import type { SQLFunction } from "../../types/SQLFunction";
import {
  getAutocompleteParseTree,
  prepareQuery,
} from "../../__tests__/__utils__/testUtils";
import testExpecteds from "./resources/TokenSuggestions.test.json";

const TEST_FUNCTIONS: SQLFunction[] = [
  {
    name: "ABS",
    description: "Computes the absolute value of a numeric expression.\n",
    label: "(NUMERIC numeric_expression) → NUMERIC",
    snippet: "($1)",
  }, // reservedFunctionName
  {
    name: "EXTRACT",
    description:
      "Extracts the specified date or time part from the date or timestamp.",
    label: "(DATEANDTIME dateTimeValue) → NUMERIC",
    snippet: "($1)",
  }, // builtInFunctionCall
  {
    name: "CURRENT_SCHEMA",
    description: "Returns the path/schema in use by the current session.",
    label: "(DATEANDTIME dateTimeValue) → CHARACTERS",
    snippet: "()",
  }, // contextVariable
  {
    name: "FROM_HEX",
    description: "Returns a BINARY value for the given hexadecimal STRING",
    label: "(CHARACTERS input) → BYTES",
    snippet: "($1)",
  }, // identifier
];

type Expecteds = {
  [testName: string]: TestResult;
};

type TestResult = {
  keywords: (string | string[])[];
  functions: boolean;
  identifiers: boolean;
};

const tokenTypeToKeyword = (tokenType: number): string => {
  return LiveEditParser.VOCABULARY.getSymbolicName(tokenType)!;
};

const generateTestResult = (queryText: string): TestResult => {
  const [query, caretPosition] = prepareQuery(queryText);
  const { parseTree, parser } = getAutocompleteParseTree(query);
  const suggestions = new TokenSuggestions(
    parseTree,
    parser,
    caretPosition,
    TEST_FUNCTIONS,
  ).get();

  const keywords = suggestions.keywords;
  const functions = suggestions.functions.length > 0;
  const identifiers = !!suggestions.identifiers;
  return {
    keywords: keywords.map((keywordSuggestion) =>
      keywordSuggestion.requiredFollowTokenTypes.length > 0
        ? [
            tokenTypeToKeyword(keywordSuggestion.tokenType),
            ...keywordSuggestion.requiredFollowTokenTypes.map(
              tokenTypeToKeyword,
            ),
          ]
        : tokenTypeToKeyword(keywordSuggestion.tokenType),
    ),
    functions,
    identifiers,
  };
};

describe("TokenSuggestions", () => {
  describe("get", () => {
    const testCases = [
      "^",
      "S^",
      "SELEC^",
      "^SELECT",
      "SELECT SELECT^",
      "SELECT ^ FROM abc",
      "SELECT * FROM ^",
      "SELECT FROM ^",
      "FROM ^",
      "SELECT * FROM tbl ^",
      "SELECT ABS(^",
      "SELECT * FROM (^)",
      "CREATE ^",
      "DELETE ^",
      "SHOW ^",
      "DESCRIBE ^",
      "USE ^",
      "MERGE ^",
      "UPDATE ^",
      "TRUNCATE ^",
      "ALTER ^",
      "REFRESH ^",
      "GRANT ^",
      "REVOKE ^",
      "SET ^",
      "RESET ^",
      "WITH ^",
      "VALUES ^",
      "TABLE ^",
      "CALL ^",
      "SHOW TABLES AT ^",
      "COPY INTO tbl FROM '' (^)",
      "ALTER TABLE tbl ^",
      "SELECT * FROM tbl WHERE col ^",
      "SELECT * FROM tbl WHERE col1 NOT LIKE 'test' AND col2 IS ^",
    ];
    runGoldenTests(
      testCases,
      testExpecteds as Expecteds,
      __filename,
      generateTestResult,
    );
  });
});
