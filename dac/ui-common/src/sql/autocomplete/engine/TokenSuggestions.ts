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

import { ParserRuleContext } from "antlr4ts";

import { LiveEditParser } from "../../../../target/generated-sources/antlr/LiveEditParser";
import { isKeyword } from "../../parser/utils/liveEditQueryParserUtils";
import {
  type CursorInfo,
  computeCursorInfo,
  getPriorTerminal,
} from "../parsing/cursorInfo";
import { KeywordSuggestion } from "../types/KeywordSuggestion";
import { IdentifierCandidate } from "../types/IdentifierCandidate";
import type { CursorQueryPosition } from "../types/CursorQueryPosition";
import type { SQLFunction } from "../types/SQLFunction";
import { collectCandidates } from "./collectCandidates";
import { timed } from "../../../utilities/timed";

export type FunctionSuggestion = SQLFunction;

export type SuggestionInfo = {
  keywords: KeywordSuggestion[];
  functions: FunctionSuggestion[];
  identifiers?: IdentifierCandidate;
  cursorInfo: CursorInfo;
};

export class TokenSuggestions {
  private readonly queryParseTree: ParserRuleContext;
  private readonly parser: LiveEditParser;
  private readonly cursorInfo: CursorInfo;
  private readonly sqlFunctions: SQLFunction[];

  constructor(
    queryParseTree: ParserRuleContext,
    parser: LiveEditParser,
    caretQueryPosition: CursorQueryPosition,
    sqlFunctions: SQLFunction[],
  ) {
    this.queryParseTree = queryParseTree;
    this.parser = parser;
    this.cursorInfo = computeCursorInfo(queryParseTree, caretQueryPosition)!;
    this.sqlFunctions = sqlFunctions;
  }

  @timed("TokenSuggestions.get")
  public get(): SuggestionInfo {
    const suggestions: SuggestionInfo = {
      keywords: [],
      functions: [],
      cursorInfo: this.cursorInfo,
    };
    const tokenIndex = this.cursorInfo.tokenIndex;
    const priorTerminal = getPriorTerminal(this.cursorInfo);
    const candidates = collectCandidates(
      this.parser,
      tokenIndex,
      priorTerminal,
      this.queryParseTree,
    );

    // Add keywords
    for (const [tokenType, tokenList] of candidates.tokens) {
      if (this.shouldSuggestToken(tokenType, true)) {
        this.removeNonSuggestableSubsequence(tokenList);
        suggestions.keywords.push(new KeywordSuggestion(tokenType, tokenList));
      }
    }

    // Add functions
    if (candidates.functions.isViable) {
      suggestions.functions = this.collectFunctionSuggestions();
    }

    // Add identifiers
    if (candidates.identifiers.isViable) {
      suggestions.identifiers = new IdentifierCandidate(
        candidates.identifiers.ruleIndex,
        candidates.identifiers.ruleList,
      );
    }

    return suggestions;
  }

  private shouldSuggestToken(
    tokenType: number,
    validatePrefix: boolean,
  ): boolean {
    const symbolicName = this.parser.vocabulary.getSymbolicName(tokenType);
    if (!symbolicName || !isKeyword(tokenType)) {
      return false;
    } else if (validatePrefix && !this.isViableCompletion(symbolicName)) {
      return false;
    } else {
      return true;
    }
  }

  private isViableCompletion(completionSymbolicName: string): boolean {
    if (this.cursorInfo.kind === "before") {
      return true;
    }
    const currentTokenText = this.cursorInfo.prefix;
    return completionSymbolicName
      .toLocaleLowerCase()
      .startsWith(currentTokenText.toLocaleLowerCase());
  }

  private collectFunctionSuggestions(): SQLFunction[] {
    return this.sqlFunctions.filter((sqlFunction) =>
      this.isViableCompletion(sqlFunction.name),
    );
  }

  private removeNonSuggestableSubsequence(tokenList: number[]): void {
    const firstNonSuggestableToken = tokenList.findIndex(
      (tokenType) => !this.shouldSuggestToken(tokenType, false),
    );
    if (firstNonSuggestableToken != -1) {
      tokenList.splice(firstNonSuggestableToken);
    }
  }
}
