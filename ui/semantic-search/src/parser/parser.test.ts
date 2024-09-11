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
import assert from "node:assert/strict";
import test, { describe } from "node:test";
import { testTree } from "@lezer/generator/test";
import { parser } from "./index.js";

const strictParser = parser.configure({ strict: true });

const passExamples = [
  "sample", // unquoted searchText
  `"sample"`, // quoted searchText
  "sample in: catalog", // searchText with filter
  `"sample" in: catalog`, // quoted searchText with filter
  "sample in: catalog type: view", // multiple filters,
  "sample unknownFilter: test", // allow anything which matches the filterkeyword:filtervalue syntax,
  " sample", // leading unquoted whitespace should pass
];

const failExamples = [
  "", // No empty string
  "sample:", // searchText is required first before a filter condition
  "sample in:", // filter value is required
  `"sample" abc`, // don't mix quoted and unquoted searchText
  "sample: in: catalog", // unquoted searchText with a `:`
];

describe("grammar tests", () => {
  for (const example of passExamples) {
    test(`PASS: \`${example}\``, () => {
      strictParser.parse(example);
    });
  }

  for (const example of failExamples) {
    test(`FAIL: \`${example}\``, () => {
      assert.throws(() => {
        strictParser.parse(example);
      });
    });
  }

  test("unquoted search text with multiple filters", () => {
    const tree = strictParser.parse(
      `sample data in: NYC Taxi Trips type: view`
    );
    testTree(
      tree,
      `DremioSemanticSearch(
        SearchText (
          UnquotedString
        )
        Filter(
          FilterKeyword
          FilterValue (
            UnquotedString
          )
        )
        Filter(
          FilterKeyword
          FilterValue (
            UnquotedString
          )
        )
      )`
    );
  });

  test("quoted search text with multiple filters", () => {
    const tree = strictParser.parse(
      `"sample data" in: NYC Taxi Trips type: view`
    );
    testTree(
      tree,
      `DremioSemanticSearch(
        SearchText (
          QuotedString
        )
        Filter(
          FilterKeyword
          FilterValue (
            UnquotedString
          )
        )
        Filter(
          FilterKeyword
          FilterValue (
            UnquotedString
          )
        )
      )`
    );
  });

  test("simple unquoted search text", () => {
    const tree = strictParser.parse(`sample`);
    testTree(
      tree,
      `DremioSemanticSearch(
        SearchText (
          UnquotedString
        )
      )`
    );
  });

  test("simple quoted search text", () => {
    const tree = strictParser.parse(`"sample"`);
    testTree(
      tree,
      `DremioSemanticSearch(
        SearchText (
          QuotedString
        )
      )`
    );
  });

  test("keywords in unquoted search text", () => {
    const tree = strictParser.parse(`in in: in`);
    testTree(
      tree,
      `DremioSemanticSearch(
        SearchText (
          UnquotedString
        )
        Filter(
          FilterKeyword
          FilterValue (
            UnquotedString
          )
        )
      )`
    );
  });
});
