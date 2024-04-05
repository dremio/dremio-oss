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

import type { CommonTokenStream, ParserRuleContext } from "antlr4ts";

import type { LiveEditParser } from "../../../../target/generated-sources/antlr/LiveEditParser";
import { SuggestionInfo, TokenSuggestions } from "./TokenSuggestions";
import type { AutocompleteApi } from "../apiClient/autocompleteApi";
import { isInComment } from "../parsing/comments";
import type { CursorInfo } from "../parsing/cursorInfo";
import type { CursorQueryPosition } from "../types/CursorQueryPosition";
import type { IdentifierCandidate } from "../types/IdentifierCandidate";
import type { SQLFunction } from "../types/SQLFunction";
import { analyzeIdentifier } from "../identifiers/identifierAnalyzer";
import {
  IdentifierSuggestions,
  GetIdentifierSuggestionsResult,
} from "./IdentifierSuggestions";
import {
  IContainerFetcher,
  ContainerFetcher,
} from "../apiClient/containerFetcher";
import { IColumnFetcher, ColumnFetcher } from "../apiClient/columnFetcher";
import { createMonacoCompletions } from "./createMonacoCompletions";
import { timedAsync } from "../../../utilities/timed";
import { LiveEditParsingEngine } from "../../parser/engine/LiveEditParsingEngine";

export class AutocompleteEngine {
  private liveEditParsingEngine: LiveEditParsingEngine;
  private containerFetcher: IContainerFetcher;
  private columnFetcher: IColumnFetcher;
  private sqlFunctions: SQLFunction[];

  constructor(
    liveEditParsingEngine: LiveEditParsingEngine,
    autocompleteApi: AutocompleteApi,
    sqlFunctions: SQLFunction[],
  ) {
    this.liveEditParsingEngine = liveEditParsingEngine;
    this.containerFetcher = new ContainerFetcher(autocompleteApi);
    this.columnFetcher = new ColumnFetcher(autocompleteApi);
    this.sqlFunctions = sqlFunctions;
  }

  @timedAsync("AutocompleteEngine.generateCompletionItems")
  async generateCompletionItems(
    linesContent: string[],
    queryPosition: CursorQueryPosition,
    queryContext: string[],
  ): Promise<monaco.languages.CompletionItem[]> {
    const { tokenStream, lexer, parseTree, parser, errors } =
      this.liveEditParsingEngine.run(linesContent);

    if (
      errors.find((error) => error.type === "lexer") ||
      isInComment(queryPosition, tokenStream.getTokens(), lexer._mode)
    ) {
      return [];
    }
    const tokenSuggestions: SuggestionInfo = this.getTokenSuggestions(
      parseTree,
      parser,
      queryPosition,
    );

    let identifierSuggestions: GetIdentifierSuggestionsResult | undefined =
      undefined;
    if (tokenSuggestions.identifiers) {
      identifierSuggestions = await this.getIdentifierSuggestions(
        tokenSuggestions.identifiers,
        tokenSuggestions.cursorInfo,
        tokenStream,
        queryContext,
      );
    }

    return createMonacoCompletions(tokenSuggestions, identifierSuggestions);
  }

  private getTokenSuggestions(
    queryParseTree: ParserRuleContext,
    parser: LiveEditParser,
    position: CursorQueryPosition,
  ): SuggestionInfo {
    return new TokenSuggestions(
      queryParseTree,
      parser,
      position,
      this.sqlFunctions,
    ).get();
  }

  private getIdentifierSuggestions(
    identifiers: IdentifierCandidate,
    cursorInfo: CursorInfo,
    tokenStream: CommonTokenStream,
    queryContext: string[],
  ): Promise<GetIdentifierSuggestionsResult | undefined> {
    const analyzedIdentifier = analyzeIdentifier(identifiers, cursorInfo);
    if (!analyzedIdentifier) {
      return Promise.resolve(undefined);
    }
    return new IdentifierSuggestions(
      analyzedIdentifier,
      cursorInfo,
      tokenStream,
      this.containerFetcher,
      this.columnFetcher,
      queryContext,
    ).get();
  }
}
