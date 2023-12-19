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

import { type CursorInfo, computeCursorInfo } from "../cursorInfo";
import {
  getAutocompleteParseTree,
  prepareQuery,
} from "../../__tests__/__utils__/testUtils";
import { LiveEditParser } from "../../../../../target/generated-sources/antlr/LiveEditParser";

describe("cursorInfo", () => {
  type At<T> = T extends { kind: "at" } ? T : never;
  const expectKindAt: <T extends CursorInfo>(
    cursorInfo: T
  ) => asserts cursorInfo is At<T> = (cursorInfo) => {
    expect(cursorInfo.kind).toEqual("at");
  };

  describe("computeCursorInfo", () => {
    it("^", () => {
      const [query, caretPosition] = prepareQuery("^");
      const { parseTree } = getAutocompleteParseTree(query);
      let actual = computeCursorInfo(parseTree, caretPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(0);
      expect(actual.priorTerminals.length).toBe(0);
    });

    it("S^", () => {
      const [query, caretPosition] = prepareQuery("S^");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expectKindAt(actual);
      expect(actual.tokenIndex).toBe(0);
      expect(actual.priorTerminals.length).toBe(0);
      expect(actual.terminal.symbol.type).toBe(LiveEditParser.IDENTIFIER);
      expect(actual.prefix).toEqual("S");
      expect(actual.isQuoted).toBe(false);
    });

    it("SELEC^", () => {
      const [query, caretPosition] = prepareQuery("SELEC^");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expectKindAt(actual);
      expect(actual.tokenIndex).toBe(0);
      expect(actual.priorTerminals.length).toBe(0);
      expect(actual.terminal.symbol.type).toBe(LiveEditParser.IDENTIFIER);
      expect(actual.prefix).toEqual("SELEC");
      expect(actual.isQuoted).toBe(false);
    });

    it("SELECT^", () => {
      const [query, caretPosition] = prepareQuery("SELECT^");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expectKindAt(actual);
      expect(actual.tokenIndex).toBe(0);
      expect(actual.priorTerminals.length).toBe(0);
      expect(actual.terminal.symbol.type).toBe(LiveEditParser.SELECT);
      expect(actual.prefix).toEqual("SELECT");
      expect(actual.isQuoted).toBe(false);
    });

    it("SELECT ^", () => {
      const [query, caretPosition] = prepareQuery("SELECT ^");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(1);
      expect(actual.priorTerminals.length).toBe(1);
    });

    it("SELECT *^ FROM", () => {
      const [query, caretPosition] = prepareQuery("SELECT *^ FROM ");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expectKindAt(actual);
      expect(actual.tokenIndex).toBe(1);
      expect(actual.priorTerminals.length).toBe(1);
      expect(actual.terminal.symbol.type).toBe(LiveEditParser.STAR);
      expect(actual.prefix).toEqual("*");
      expect(actual.isQuoted).toBe(false);
    });

    it("SELECT ^ FROM", () => {
      const [query, caretPosition] = prepareQuery("SELECT ^ FROM ");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(1);
      expect(actual.priorTerminals.length).toBe(1);
    });

    it("SELECT FROM ^", () => {
      const [query, caretPosition] = prepareQuery("SELECT FROM ^");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(2);
      expect(actual.priorTerminals.length).toBe(2);
    });

    it("SELECT * FR^", () => {
      const [query, caretPosition] = prepareQuery("SELECT * FR^");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expectKindAt(actual);
      expect(actual.tokenIndex).toBe(2);
      expect(actual.priorTerminals.length).toBe(2);
      expect(actual.terminal.symbol.type).toBe(LiveEditParser.IDENTIFIER);
      expect(actual.prefix).toEqual("FR");
      expect(actual.isQuoted).toBe(false);
    });

    it("SELECT * FROM ^", () => {
      const [query, caretPosition] = prepareQuery("SELECT * FROM ^");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(3);
      expect(actual.priorTerminals.length).toBe(3);
    });

    it("SELECT *\n  FROM ^", () => {
      const [query, caretPosition] = prepareQuery("SELECT *\n  FROM ^");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(3);
      expect(actual.priorTerminals.length).toBe(3);
    });

    it("SELECT    col   \n  FROM tbl   \n  WHERE ^", () => {
      const [query, caretPosition] = prepareQuery(
        "SELECT    col   \n  FROM tbl   \n  WHERE ^"
      );
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, caretPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(5);
      expect(actual.priorTerminals.length).toBe(5);
    });

    it("SELECT * FROM tbl.^", () => {
      const [query, cursorQueryPosition] = prepareQuery("SELECT * FROM tbl.^");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, cursorQueryPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(5);
      expect(actual.priorTerminals.length).toBe(5);
    });

    it("SELECT AVG(^)", () => {
      const [query, cursorQueryPosition] = prepareQuery("SELECT AVG(^)");
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, cursorQueryPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(3);
      expect(actual.priorTerminals.length).toBe(3);
    });

    // Validates handling of missing inserted token ("JOIN")
    it("SELECT * FROM tbl LEFT OUTER tbl2 ^", () => {
      const [query, cursorQueryPosition] = prepareQuery(
        "SELECT * FROM tbl LEFT OUTER tbl2 ^"
      );
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, cursorQueryPosition)!;
      expect(actual.kind).toEqual("before");
      expect(actual.tokenIndex).toBe(7);
      expect(actual.priorTerminals.length).toBe(7);
    });

    it('SELECT * FROM "^"', () => {
      const [query, cursorQueryPosition] = prepareQuery('SELECT * FROM "^"');
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, cursorQueryPosition)!;
      expectKindAt(actual);
      expect(actual.tokenIndex).toBe(3);
      expect(actual.priorTerminals.length).toBe(3);
      expect(actual.terminal.symbol.type).toBe(
        LiveEditParser.QUOTED_IDENTIFIER
      );
      expect(actual.prefix).toEqual("");
      expect(actual.isQuoted).toBe(true);
    });

    it('SELECT * FROM "S^"', () => {
      const [query, cursorQueryPosition] = prepareQuery('SELECT * FROM "S^"');
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, cursorQueryPosition)!;
      expectKindAt(actual);
      expect(actual.tokenIndex).toBe(3);
      expect(actual.priorTerminals.length).toBe(3);
      expect(actual.terminal.symbol.type).toBe(
        LiveEditParser.QUOTED_IDENTIFIER
      );
      expect(actual.prefix).toEqual("S");
      expect(actual.isQuoted).toBe(true);
    });

    it('UPDATE tbl AS aliased S^"', () => {
      const [query, cursorQueryPosition] = prepareQuery(
        "UPDATE tbl AS aliased S^"
      );
      const { parseTree } = getAutocompleteParseTree(query);
      const actual = computeCursorInfo(parseTree, cursorQueryPosition)!;
      expectKindAt(actual);
      expect(actual.tokenIndex).toBe(4);
      expect(actual.priorTerminals.length).toBe(4);
    });
  });
});
