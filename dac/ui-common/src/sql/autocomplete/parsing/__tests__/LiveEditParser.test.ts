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

import fs from "fs";
import path from "path";

/*
 * LiveEditParser overrides rules from DremioParser. In order to prevent the overrides from becoming out of sync
 * with the "upstream" changes, we validate that the DremioParser rules being overriden match exactly what we expect.
 *
 * If any of these tests fail, it means that the DremioParser grammar has changed and we must determine how to change
 * LiveEditParser to port these changes, if necessary. After making any necessary changes, these test expecteds should
 * be updated.
 */
describe("LiveEditParser", () => {
  const grammarFile = path.join(
    __dirname,
    "../../../../../",
    "sql-grammar",
    "DremioParser.g4",
  );
  const dremioGrammar = fs.readFileSync(grammarFile, "utf8");

  it("sqlSelect", () => {
    const expectedOriginalRule =
      "sqlSelect : SELECT (HINT_BEG commaSepatatedSqlHints COMMENT_END)? sqlSelectKeywords STREAM? (DISTINCT | ALL)? selectList (FROM fromClause whereOpt groupByOpt havingOpt windowOpt qualifyOpt)?  ;";

    expect(dremioGrammar.includes(expectedOriginalRule)).toBe(true);
  });

  it("selectList", () => {
    const expectedOriginalRule =
      "selectList : selectItem (COMMA selectItem)*  ;";

    expect(dremioGrammar.includes(expectedOriginalRule)).toBe(true);
  });

  it("selectItem", () => {
    const expectedOriginalRule =
      "selectItem : selectExpression (AS? simpleIdentifier)?  ;";

    expect(dremioGrammar.includes(expectedOriginalRule)).toBe(true);
  });
});
