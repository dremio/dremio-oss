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
import { parseQueryText } from "./parseQueryText";

describe("parseQuery tests", () => {
  test("with no filters", () => {
    assert.deepStrictEqual(parseQueryText("sample data"), {
      searchText: "sample data",
      filters: [],
    });
  });

  test("with quoted searchText", () => {
    assert.deepStrictEqual(parseQueryText(`"sample data"`), {
      searchText: "sample data",
      filters: [],
    });
  });

  test("with multiple filters", () => {
    assert.deepStrictEqual(parseQueryText("sample data in: NYC Taxi Trips"), {
      searchText: "sample data",
      filters: [
        {
          keyword: "in",
          value: "NYC Taxi Trips",
        },
      ],
    });
  });
});
