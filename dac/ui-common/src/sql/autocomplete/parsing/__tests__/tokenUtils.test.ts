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
import {
  prepareQuery,
  getAutocompleteParseTree,
} from "../../__tests__/__utils__/testUtils";
import { LiveEditQueryParser } from "../../../parser/liveEditQueryParser";
import { getPriorToken, getText } from "../tokenUtils";

describe("tokenUtils", () => {
  describe("getText", () => {
    it("colA", () => {
      const parser = new LiveEditQueryParser(
        "colA",
        undefined,
        undefined,
        true
      ).getParser();
      const simpleIdentifier = parser.simpleIdentifier();
      expect(getText(simpleIdentifier)).toEqual({
        text: "colA",
        isQuoted: false,
      });
      const terminal = simpleIdentifier
        .identifierSegment()
        .getToken(LiveEditParser.IDENTIFIER, 0);
      expect(getText(terminal)).toEqual({
        text: "colA",
        isQuoted: false,
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
      expect(getText(simpleIdentifier)).toEqual({
        text: "col-A",
        isQuoted: true,
      });
      const terminal = simpleIdentifier
        .identifierSegment()
        .getToken(LiveEditParser.QUOTED_IDENTIFIER, 0);
      expect(getText(terminal)).toEqual({
        text: "col-A",
        isQuoted: true,
      });
    });

    it("SE^LECT", () => {
      const parser = new LiveEditQueryParser(
        "SELECT",
        undefined,
        undefined,
        true
      ).getParser();
      const select = parser.sqlSelect();
      const terminal = select.getToken(LiveEditParser.SELECT, 0);
      expect(getText(terminal, 2)).toEqual({
        text: "SE",
        isQuoted: false,
      });
    });

    it('"@dremio"', () => {
      const parser = new LiveEditQueryParser(
        '"@dremio"',
        undefined,
        undefined,
        true
      ).getParser();
      const simpleIdentifier = parser.simpleIdentifier();
      const terminal = simpleIdentifier
        .identifierSegment()
        .getToken(LiveEditParser.QUOTED_IDENTIFIER, 0);
      expect(getText(terminal, 3)).toEqual({
        text: "@dr",
        isQuoted: true,
      });
    });

    it("<EOF>", () => {
      const parser = new LiveEditQueryParser(
        "",
        undefined,
        undefined,
        true
      ).getParser();
      const eof = parser.simpleIdentifier();
      expect(getText(eof)).toEqual({
        text: "",
        isQuoted: false,
      });
    });
  });

  describe("getPriorToken", () => {
    it("should ignore interleaving comments", () => {
      const [query] = prepareQuery("SELECT /* comment */ * ^");
      const { commonTokenStream } = getAutocompleteParseTree(query);
      expect(commonTokenStream.get(2).type).toEqual(LiveEditParser.STAR);
      expect(getPriorToken(commonTokenStream, 2)?.type).toEqual(
        LiveEditParser.SELECT
      );
    });

    it("should ignore starting comments", () => {
      const [query] = prepareQuery("/* comment */ SELECT ^");
      const { commonTokenStream } = getAutocompleteParseTree(query);
      expect(commonTokenStream.get(1).type).toEqual(LiveEditParser.SELECT);
      expect(getPriorToken(commonTokenStream, 1)).toBeUndefined();
    });
  });
});
