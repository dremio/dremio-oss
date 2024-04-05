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

import type { ParserRuleContext } from "antlr4ts";
import type { CommonTokenStream } from "antlr4ts/CommonTokenStream";

import {
  createParserErrorListeners,
  noOpLogger,
} from "../../../parser/errorListener";
import type { CursorQueryPosition } from "../../types/CursorQueryPosition";
import { LiveEditQueryParser } from "../../../parser/liveEditQueryParser";
import type { LiveEditParser } from "../../../../../target/generated-sources/antlr/LiveEditParser";
import type { LiveEditLexer } from "../../../../../target/generated-sources/antlr/LiveEditLexer";

export function prepareQuery(
  queryWithCaret: string,
  replaceChar?: string,
): [query: string, caret: CursorQueryPosition] {
  const caretIndex = queryWithCaret.indexOf("^");
  const lineNum =
    (queryWithCaret.slice(0, caretIndex).match(/\n/g)?.length || 0) + 1;
  const colNum =
    caretIndex - queryWithCaret.slice(0, caretIndex).lastIndexOf("\n") - 1;
  const newQuery =
    queryWithCaret.slice(0, caretIndex) +
    (replaceChar ?? "") +
    queryWithCaret.slice(caretIndex + 1);
  return [newQuery, { line: lineNum, column: colNum }];
}

export function getAutocompleteParseTree(query: string): {
  parseTree: ParserRuleContext;
  commonTokenStream: CommonTokenStream;
  parser: LiveEditParser;
  lexer: LiveEditLexer;
} {
  const { lexerErrorListener, parserErrorListener } =
    createParserErrorListeners(noOpLogger);
  const {
    parseTree,
    tokenStream: commonTokenStream,
    parser,
    lexer,
  } = new LiveEditQueryParser(
    query,
    lexerErrorListener,
    parserErrorListener,
    true,
  ).parse();
  return { parseTree, commonTokenStream, parser, lexer };
}
