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

import {
  prepareQuery,
  getAutocompleteParseTree,
} from "../../__tests__/__utils__/testUtils";
import { isInComment } from "../comments";

describe("comments", () => {
  describe("isInComment", () => {
    it("SELECT ^", () => {
      const [query, caretPosition] = prepareQuery("SELECT ^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(false);
    });

    it("SELECT /* comment */ ^", () => {
      const [query, caretPosition] = prepareQuery("SELECT /* comment */ ^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(false);
    });

    it('SELECT "/* ^ */"', () => {
      const [query, caretPosition] = prepareQuery('SELECT "/* ^ */"');
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(false);
    });

    it('SELECT "--^"', () => {
      const [query, caretPosition] = prepareQuery('SELECT "--^"');
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(false);
    });

    it("SELECT /* comment ^ */", () => {
      // We don't want to suggest right at the comment end without leading space
      const [query, caretPosition] = prepareQuery("SELECT /* comment ^ */");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(true);
    });

    it("SELECT /* comment */^", () => {
      // We don't want to suggest right at the comment end without leading space so it's considered
      // inside the comment
      const [query, caretPosition] = prepareQuery("SELECT /* comment */^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(true);
    });

    it("SELECT /* comment */ ^", () => {
      const [query, caretPosition] = prepareQuery("SELECT /* comment */ ^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(false);
    });

    it("SELECT /* comment \n ^ */", () => {
      const [query, caretPosition] = prepareQuery("SELECT /* comment \n ^ */");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(true);
    });

    it("SELECT /* comment ^ ", () => {
      const [query, caretPosition] = prepareQuery("SELECT /* comment ^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(true);
    });

    it("SELECT /*^", () => {
      const [query, caretPosition] = prepareQuery("SELECT /*^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(true);
    });

    it("SELECT --^", () => {
      const [query, caretPosition] = prepareQuery("SELECT --^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(true);
    });

    it("SELECT -- comment ^", () => {
      const [query, caretPosition] = prepareQuery("SELECT -- comment ^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(true);
    });

    it("SELECT -- comment /* */ ^", () => {
      const [query, caretPosition] = prepareQuery("SELECT -- comment /* */ ^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(true);
    });

    it("SELECT -- comment \n^", () => {
      const [query, caretPosition] = prepareQuery("SELECT -- comment\n^");
      const { commonTokenStream, lexer } = getAutocompleteParseTree(query);
      expect(
        isInComment(caretPosition, commonTokenStream.getTokens(), lexer._mode)
      ).toBe(false);
    });
  });
});
