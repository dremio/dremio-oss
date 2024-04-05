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
  BufferedTokenStream,
  CommonToken,
  InputMismatchException,
  LexerNoViableAltException,
  NoViableAltException,
  RecognitionException,
  Token,
} from "antlr4ts";
import { IntervalSet } from "antlr4ts/misc/IntervalSet";

import { QueryParseError } from "../../parser/errorListener";
import { LiveEditParsingEngine } from "../../parser/engine/LiveEditParsingEngine";
import { LiveEditParseInfo } from "../../parser/types/ParseResult";
import {
  getExpressionTokens,
  getIdentifierTokens,
} from "../../parser/utils/liveEditQueryParserUtils";
import { SQLError } from "../types/SQLError";
import { LiveEditParser } from "../../../../target/generated-sources/antlr/LiveEditParser";
import { toReadableString } from "../../../utilities/array";

type ExpectedTokenType = "identifier" | "expression";

export class ErrorDetectionEngine {
  private liveEditParsingEngine: LiveEditParsingEngine;

  private static EXPECTED_TOKENS_NAME_FILTER = [
    "HINT_BEG", // Don't suggest in select clause
  ];

  private static EOF_ERROR = "Unexpected end of query.";
  private static GENERIC_MSG = "Unexpected token.";
  private static NO_TOKEN_MSG = "<no token>";
  private static EXTRA_TOKEN_MSG = (extraToken: string) =>
    `Extraneous token '${extraToken}'.`;
  private static MISSING_TOKEN_MSG = (missingToken: string) =>
    `Missing token '${missingToken}'.`;
  private static UNEXPECTED_TOKEN_MSG = (unexpectedToken: string) =>
    `Unexpected token '${unexpectedToken}'.`;
  private static UNEXPECTED_TOKENS_MSG = (unexpectedTokens: string[]) =>
    `Unexpected tokens '${unexpectedTokens.join(" ")}'.`;
  private static EXPECTED_TOKEN_MSG = (expectedTokens: string) =>
    `Expecting ${expectedTokens}.`;
  private static UNRECOGNIZED_TOKEN_MSG = (unrecognizedToken: string) =>
    `Unrecognized token '${unrecognizedToken}'.`;

  constructor(liveEditParsingEngine: LiveEditParsingEngine) {
    this.liveEditParsingEngine = liveEditParsingEngine;
  }

  detectSqlErrors(linesContent: string[]): SQLError[] {
    const parseInfo: LiveEditParseInfo =
      this.liveEditParsingEngine.run(linesContent);
    const parseErrors = parseInfo.errors
      .map((error) => this.constructParseError(error)!)
      .filter((error) => error != undefined);
    return parseErrors;
  }

  private constructParseError(error: QueryParseError): SQLError | undefined {
    const offendingSymbol = error.offendingSymbol;
    const range = this.getErrorRange(
      offendingSymbol,
      error.e,
      error.line,
      error.charPositionInLine,
    );
    if (
      this.isEofError(error.offendingSymbol) &&
      offendingSymbol!.tokenIndex === 0
    ) {
      // Ignore blank query errors
      return undefined;
    }
    const message = this.getErrorMessage(error);
    const rawMessage = error.msg;

    return {
      range,
      message,
      rawMessage,
    };
  }

  /**
   * Convert to:
   * {
   *   range: {
   *     startLine // 1-indexed inclusive
   *     startColumn // 1-indexed inclusive
   *     endLine // 1-indexed inclusive
   *     endColumn // 1-indexed *exclusive*
   *   }
   *   isEofError: boolean
   * }
   */
  private getErrorRange(
    offendingSymbol: CommonToken | undefined,
    e: RecognitionException | undefined,
    line: number,
    column: number,
  ): SQLError["range"] {
    const isEofError = this.isEofError(offendingSymbol);
    const offendingTokens = offendingSymbol
      ? this.getOffendingTokens(e, offendingSymbol)
      : undefined;
    const startBadToken = offendingTokens?.[0];
    const startLineNumber: number = startBadToken ? startBadToken.line : line;
    const startColumn: number = isEofError
      ? column // Error should be on last character of the last real parsed token
      : startBadToken
        ? startBadToken.charPositionInLine + 1
        : column + 1;
    const endTokenTextLines: string[] | undefined =
      offendingSymbol?.text?.split("\n");
    let endLineNumber: number;
    let endColumn: number;
    if (isEofError || !offendingSymbol || !endTokenTextLines) {
      endLineNumber = line;
      endColumn = startColumn + 1;
    } else {
      endLineNumber = offendingSymbol.line + endTokenTextLines.length - 1;
      endColumn =
        endTokenTextLines.length > 1
          ? endTokenTextLines[endTokenTextLines.length - 1].length + 1
          : offendingSymbol.charPositionInLine +
            endTokenTextLines[0].length +
            1;
    }

    return {
      startLineNumber,
      startColumn,
      endLineNumber,
      endColumn,
    };
  }

  private getErrorMessage(error: QueryParseError): string {
    const { e, offendingSymbol, msg } = error;

    if (e instanceof LexerNoViableAltException) {
      const unrecognizedToken =
        msg.match(/token recognition error at: '(.*)'/)?.[1] ??
        ErrorDetectionEngine.NO_TOKEN_MSG;
      return ErrorDetectionEngine.UNRECOGNIZED_TOKEN_MSG(unrecognizedToken);
    }

    if (!offendingSymbol) {
      return ErrorDetectionEngine.GENERIC_MSG;
    }

    // See DefaultErrorStrategy.recoverInline
    if (!e && msg.startsWith("extraneous input")) {
      // Means single token deletion recovery so badToken will be a single token
      return ErrorDetectionEngine.EXTRA_TOKEN_MSG(offendingSymbol.text!);
    } else if (!e && msg.startsWith("missing")) {
      // Single token insertion recovery
      const missingToken =
        msg.match(/missing (.*?) at/)?.[1] ?? ErrorDetectionEngine.NO_TOKEN_MSG;
      return ErrorDetectionEngine.MISSING_TOKEN_MSG(missingToken);
    }

    const offendingTokens = this.getOffendingTokens(e, offendingSymbol);

    if (
      e instanceof InputMismatchException ||
      e instanceof NoViableAltException
    ) {
      const { types, otherTokens } = this.getExpectedTokens(e, offendingSymbol);
      const expectingStrParts: string[] = [...types];
      if (!otherTokens.isNil) {
        expectingStrParts.push(
          otherTokens.toStringVocabulary(LiveEditParser.VOCABULARY),
        );
      }
      const expectingTokens = toReadableString(expectingStrParts, "or");
      const expectingTokenStr = expectingTokens
        ? ` ${ErrorDetectionEngine.EXPECTED_TOKEN_MSG(expectingTokens)}`
        : "";
      const unexpectedTokensStr = this.isEofError(offendingSymbol)
        ? ErrorDetectionEngine.EOF_ERROR
        : offendingTokens.length > 1
          ? ErrorDetectionEngine.UNEXPECTED_TOKENS_MSG(
              offendingTokens.map((token) => token.text!),
            )
          : ErrorDetectionEngine.UNEXPECTED_TOKEN_MSG(offendingTokens[0].text!);
      return unexpectedTokensStr.concat(expectingTokenStr);
    } else {
      return ErrorDetectionEngine.GENERIC_MSG;
    }
  }

  private getExpectedTokens(
    e: InputMismatchException | NoViableAltException,
    offendingSymbol: CommonToken | undefined,
  ): {
    types: Set<ExpectedTokenType>;
    otherTokens: IntervalSet;
  } {
    const types: Set<ExpectedTokenType> = new Set();
    if (!e.expectedTokens || e instanceof NoViableAltException) {
      // NoViableAltException occurs during lookahead when we couldn't decide which choice to make.
      // We don't use the expected tokens since they represent the possibilities at the first token, but the first
      // token alone isn't enough to produce a "viable alternative" so it is highly likely the "bad" token is one of
      // the expected tokens we would otherwise show. The error isn't a single token issue but a sequence which cannot
      // be unambiguously parsed without further input. There isn't really a way to get these possible valid sequences
      // easily with ANTLR without doing a bunch of ATN traversal sorcery.
      return { types, otherTokens: IntervalSet.EMPTY_SET };
    }
    let otherTokens = new IntervalSet().addAll(e.expectedTokens);
    const ruleCandidates: Record<
      ExpectedTokenType,
      ReturnType<typeof this.isRulePossible>
    > = {
      identifier: this.isRulePossible(getIdentifierTokens, otherTokens),
      expression: this.isRulePossible(getExpressionTokens, otherTokens),
    };
    for (const [name, candidate] of Object.entries(ruleCandidates)) {
      if (candidate.possible) {
        otherTokens = candidate.removeFromSet(otherTokens);
        types.add(name as ExpectedTokenType);
      }
    }
    this.removeBlocklistedExpectedTokens(
      otherTokens,
      offendingSymbol,
      this.getInputStream(e),
    );
    return { types, otherTokens };
  }

  private getOffendingTokens(
    e: RecognitionException | undefined,
    offendingSymbol: Token,
  ): Token[] {
    if (e instanceof NoViableAltException) {
      return this.getNoViableAltTokens(e);
    } else {
      return [offendingSymbol];
    }
  }

  private isRulePossible(
    getRuleTokens: () => IntervalSet,
    tokens: IntervalSet,
  ): {
    possible: boolean;
    removeFromSet: (tokens: IntervalSet) => IntervalSet;
  } {
    const ruleTokens = getRuleTokens();
    const tokensMatch = tokens.and(ruleTokens).size === ruleTokens.size;
    const removeFromSet = (tokens: IntervalSet) => tokens.subtract(ruleTokens);
    return { possible: tokensMatch, removeFromSet };
  }

  /** Remove expected tokens that should not be displayed */
  private removeBlocklistedExpectedTokens(
    expectedTokens: IntervalSet,
    offendingSymbol: CommonToken | undefined,
    inputStream: BufferedTokenStream | undefined,
  ): void {
    const offendingSymbolIdx = offendingSymbol?.tokenIndex;
    // Account for fact that LiveEditParser grammar allows SELECT FROM ... queries (with missing select list) to parse
    if (
      expectedTokens.contains(LiveEditParser.FROM) &&
      offendingSymbolIdx &&
      inputStream &&
      inputStream.get(offendingSymbolIdx - 1).type === LiveEditParser.SELECT
    ) {
      expectedTokens.remove(LiveEditParser.FROM);
    }

    // Prevent displaying <EOF, ;> set of expected tokens since they are very rarely the only possible tokens and there
    // is almost always an optional clause (e.g. from clause), expression or token that can follow - the expected set
    // from ANTLR only includes the set of possible tokens that *must* follow, not all the ones that *can* follow
    if (
      expectedTokens.size == 2 &&
      expectedTokens.contains(LiveEditParser.EOF) &&
      expectedTokens.contains(LiveEditParser.SEMICOLON)
    ) {
      expectedTokens.clear();
    }

    const tokenTypes = expectedTokens.toArray();
    for (const tokenType of tokenTypes) {
      const tokenName = LiveEditParser.VOCABULARY.getSymbolicName(tokenType);
      if (
        ErrorDetectionEngine.EXPECTED_TOKENS_NAME_FILTER.some(
          (nameFilter) => tokenName?.match(nameFilter),
        )
      ) {
        expectedTokens.remove(tokenType);
      }
    }
  }

  private isEofError(offendingSymbol: Token | undefined) {
    return offendingSymbol?.type === Token.EOF; // <EOF> conjured token
  }

  private getInputStream(
    e: RecognitionException,
  ): BufferedTokenStream | undefined {
    return e.inputStream instanceof BufferedTokenStream
      ? e.inputStream
      : undefined;
  }

  private getNoViableAltTokens(e: NoViableAltException): Token[] {
    return this.getInputStream(e)!
      .getRange(e.startToken.tokenIndex, e.getOffendingToken()!.tokenIndex)
      .filter((token) => !this.isEofError(token));
  }
}
