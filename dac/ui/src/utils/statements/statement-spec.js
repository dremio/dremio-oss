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

import { extractSql } from "./statement";

describe("statement", () => {
  it("should extract subquery from sql based on a statement entry", () => {
    const query = "01234test9";
    // points to test
    const statement = {
      from: {
        line: 1,
        column: 6,
        index: 5,
        isLineBreak: false,
      },
      to: {
        line: 1,
        column: 9,
        index: 8,
        isLineBreak: false,
      },
    };
    expect(extractSql(query, statement)).to.eql("test");
  });
});
