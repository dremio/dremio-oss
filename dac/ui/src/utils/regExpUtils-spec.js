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
import { escapeDblQuotes } from "./regExpUtils";

describe("escapeDblQuotes", () => {
  it("should replace double quotes", () => {
    expect(escapeDblQuotes('a " b c')).to.equal('a "" b c');
    expect(escapeDblQuotes('a "b" c')).to.equal('a ""b"" c');
    expect(escapeDblQuotes('"a b c"')).to.equal('""a b c""');
  });
  it("should keep string w/o double quotes", () => {
    expect(escapeDblQuotes("a b c")).to.equal("a b c");
  });
  it("should return empty string if argument not defined", () => {
    expect(escapeDblQuotes()).to.equal("");
  });
});
