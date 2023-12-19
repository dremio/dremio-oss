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

import { TerminalNode } from "antlr4ts/tree/TerminalNode";

import { LiveEditParser } from "../../../../../target/generated-sources/antlr/LiveEditParser";
import {
  getAutocompleteParseTree,
  prepareQuery,
} from "../../__tests__/__utils__/testUtils";
import { LiveEditQueryParser } from "../../../parser/liveEditQueryParser";
import { computeCursorInfo, type CursorInfo } from "../../parsing/cursorInfo";
import {
  getIdentifierInfo as _getIdentifierInfo,
  getUnquotedSimpleIdentifiers,
  needsQuoting,
  IdentifierInfo,
} from "../identifierUtils";

// Separates the terminal from the rest of the IdentifierInfo, because the terminal node object
// is circularly-referential and jest toEqual hangs when comparing such objects (https://github.com/jestjs/jest/issues/10577)
function getIdentifierInfoForTest(cursorInfo: CursorInfo): {
  identifierInfo: Exclude<IdentifierInfo, "terminal">;
  terminal: TerminalNode | undefined;
} {
  const { terminal, ...identifierInfo } = _getIdentifierInfo(cursorInfo);
  return { identifierInfo, terminal };
}

describe("identifierUtils", () => {
  describe("getIdentifierInfo", () => {
    it("colA", () => {
      const parser = new LiveEditQueryParser(
        "colA",
        undefined,
        undefined,
        true
      ).getParser();
      const simpleIdentifier = parser.simpleIdentifier();
      const cursorInfo: CursorInfo = {
        priorTerminals: [], // unused
        tokenIndex: 0,
        kind: "at",
        terminal: simpleIdentifier
          .identifierSegment()
          .getToken(LiveEditParser.IDENTIFIER, 0),
        prefix: "colA",
        isQuoted: false,
      };
      const { identifierInfo, terminal } = getIdentifierInfoForTest(cursorInfo);
      expect(identifierInfo).toEqual({
        prefix: "colA",
        prefixQuoted: false,
        parentPath: [],
        completable: true,
      });
    });

    it('"col-A"', () => {
      const parser = new LiveEditQueryParser(
        '"col-A"',
        undefined,
        undefined,
        true
      ).getParser();
      const simpleIdentifier = parser.simpleIdentifier();
      const cursorInfo: CursorInfo = {
        priorTerminals: [], // unused
        tokenIndex: 0,
        kind: "at",
        terminal: simpleIdentifier
          .identifierSegment()
          .getToken(LiveEditParser.QUOTED_IDENTIFIER, 0),
        prefix: "col-A",
        isQuoted: true,
      };
      const { identifierInfo, terminal } = getIdentifierInfoForTest(cursorInfo);
      expect(identifierInfo).toEqual({
        prefix: "col-A",
        prefixQuoted: true,
        parentPath: [],
        completable: true,
      });
      expect(terminal).toBe(cursorInfo.terminal);
    });

    it("tbl.colA", () => {
      const parser = new LiveEditQueryParser(
        "tbl.colA",
        undefined,
        undefined,
        true
      ).getParser();
      const compoundIdentifier = parser.compoundIdentifier();
      const cursorInfo: CursorInfo = {
        priorTerminals: [], // unused
        tokenIndex: 2,
        kind: "at",
        terminal: compoundIdentifier
          .simpleIdentifier()[1]
          .identifierSegment()
          .getToken(LiveEditParser.IDENTIFIER, 0),
        prefix: "colA",
        isQuoted: false,
      };
      const { identifierInfo, terminal } = getIdentifierInfoForTest(cursorInfo);
      expect(identifierInfo).toEqual({
        prefix: "colA",
        prefixQuoted: false,
        parentPath: ["tbl"],
        completable: true,
      });
      expect(terminal).toBe(cursorInfo.terminal);
    });

    it('"tbl-A"."col-A"', () => {
      const parser = new LiveEditQueryParser(
        '"tbl-A"."col-A"',
        undefined,
        undefined,
        true
      ).getParser();
      const compoundIdentifier = parser.compoundIdentifier();
      const cursorInfo: CursorInfo = {
        priorTerminals: [], // unused
        tokenIndex: 2,
        kind: "at",
        terminal: compoundIdentifier
          .simpleIdentifier()[1]
          .identifierSegment()
          .getToken(LiveEditParser.QUOTED_IDENTIFIER, 0),
        prefix: "col-A",
        isQuoted: true,
      };
      const { identifierInfo, terminal } = getIdentifierInfoForTest(cursorInfo);
      expect(identifierInfo).toEqual({
        prefix: "col-A",
        prefixQuoted: true,
        parentPath: ["tbl-A"],
        completable: true,
      });
      expect(terminal).toBe(cursorInfo.terminal);
    });

    it("tbl.", () => {
      const [query, caretPosition] = prepareQuery("tbl.^");
      const { parseTree } = getAutocompleteParseTree(query);
      const cursorInfo = computeCursorInfo(parseTree, caretPosition)!;
      const { identifierInfo, terminal } = getIdentifierInfoForTest(cursorInfo);
      expect(identifierInfo).toEqual({
        prefix: "",
        prefixQuoted: false,
        parentPath: ["tbl"],
        completable: true,
      });
      expect(terminal).toBe(
        cursorInfo.priorTerminals[cursorInfo.priorTerminals.length - 2]
      );
    });

    it("tbl.*", () => {
      const parser = new LiveEditQueryParser(
        "tbl.*",
        undefined,
        undefined,
        true
      ).getParser();
      const compoundIdentifier = parser.compoundIdentifier();
      const cursorInfo: CursorInfo = {
        priorTerminals: [], // unused
        tokenIndex: 2,
        kind: "at",
        terminal: compoundIdentifier.getToken(LiveEditParser.STAR, 0),
        prefix: "*",
        isQuoted: false,
      };
      const { identifierInfo, terminal } = getIdentifierInfoForTest(cursorInfo);
      expect(identifierInfo).toEqual({
        prefix: "*",
        prefixQuoted: false,
        parentPath: ["tbl"],
        completable: true,
      });
      expect(terminal).toBe(cursorInfo.terminal);
    });

    it("tbl.colA[0]", () => {
      const parser = new LiveEditQueryParser(
        "tbl.colA[0]",
        undefined,
        undefined,
        true
      ).getParser();
      const compoundIdentifier = parser.compoundIdentifier();
      const cursorInfo: CursorInfo = {
        priorTerminals: [], // unused
        tokenIndex: 5,
        kind: "at",
        terminal: compoundIdentifier.getToken(LiveEditParser.RBRACKET, 0),
        prefix: "]",
        isQuoted: false,
      };
      const { identifierInfo, terminal } = getIdentifierInfoForTest(cursorInfo);
      expect(identifierInfo).toEqual({
        prefix: "colA",
        prefixQuoted: false,
        parentPath: ["tbl"],
        completable: false,
      });
      expect(terminal).toBe(cursorInfo.terminal);
    });

    it("tbl.colA[0][1]", () => {
      const parser = new LiveEditQueryParser(
        "tbl.colA[0][1]",
        undefined,
        undefined,
        true
      ).getParser();
      const compoundIdentifier = parser.compoundIdentifier();
      const cursorInfo: CursorInfo = {
        priorTerminals: [], // unused
        tokenIndex: 8,
        kind: "at",
        terminal: compoundIdentifier.getToken(LiveEditParser.RBRACKET, 0),
        prefix: "]",
        isQuoted: false,
      };
      const { identifierInfo, terminal } = getIdentifierInfoForTest(cursorInfo);
      expect(identifierInfo).toEqual({
        prefix: "colA",
        prefixQuoted: false,
        parentPath: ["tbl"],
        completable: false,
      });
      expect(terminal).toBe(cursorInfo.terminal);
    });
  });

  describe("getUnquotedSimpleIdentifiers", () => {
    it('colA, "col-B", colC', () => {
      const parser = new LiveEditQueryParser(
        'colA, "col-B", colC',
        undefined,
        undefined,
        true
      ).getParser();
      const list = parser.simpleIdentifierCommaList();
      expect(getUnquotedSimpleIdentifiers(list)).toEqual([
        "colA",
        "col-B",
        "colC",
      ]);
    });
  });

  describe("needsQuoting", () => {
    it("COL_9", () => expect(needsQuoting("COL_9")).toBe(false));
    it("EXPR$0", () => expect(needsQuoting("EXPR$0")).toBe(false));
    it('"col"', () => expect(needsQuoting('"col"')).toBe(true));
    it("_col", () => expect(needsQuoting("_col")).toBe(true));
  });
});
