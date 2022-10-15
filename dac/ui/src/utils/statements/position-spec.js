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

import { advancePosition } from "./position";
import { skip, value } from "./skippable";

describe("advance position", () => {
  it("should point to the next symbol and increase column for a non new line character", () => {
    const initialPosition = {
      line: 1,
      column: 1,
      index: 1, // points to 1
    };

    expect(advancePosition("0123456789", initialPosition, 6)).to.deep.equal(
      value({
        line: 1,
        column: 7,
        index: 7,
      })
    );
  });

  it("should point to the next symbol, increase column for new line", () => {
    const initialPosition = {
      line: 1,
      column: 8,
      index: 8, // points to 8
    };

    expect(
      advancePosition("0123456789\n0123456789", initialPosition, 2)
    ).to.deep.equal(
      value({
        line: 1,
        column: 10,
        index: 10,
      })
    );
  });

  it("should drop to the next line if advancing new line character", () => {
    const initialPosition = {
      line: 1,
      column: 8,
      index: 8, // points to 8
    };

    expect(
      advancePosition("0123456789\n0123456789", initialPosition, 4)
    ).to.deep.equal(
      value({
        line: 2,
        column: 2,
        index: 12,
      })
    );
  });

  it("should point to the last character if trying to advance beyond the string bounds", () => {
    const initialPosition = {
      line: 1,
      column: 9,
      index: 8, // points to 8
    };

    expect(advancePosition("0123456789", initialPosition, 4)).to.deep.equal(
      skip({
        line: 1,
        column: 10,
        index: 9,
      })
    );
  });
});
