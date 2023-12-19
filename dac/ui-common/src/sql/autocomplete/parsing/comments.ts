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

import { Token } from "antlr4ts";

import { LiveEditLexer } from "../../../../target/generated-sources/antlr/LiveEditLexer";
import { LiveEditParser } from "../../../../target/generated-sources/antlr/LiveEditParser";
import type { CursorQueryPosition } from "../types/CursorQueryPosition";

type CursorRelativePosition =
  | "before"
  | "at"
  | "afterSameLine"
  | "afterLaterLine";

export function isInComment(
  queryPosition: CursorQueryPosition,
  tokens: Token[],
  lexerMode: number
): boolean {
  for (const token of tokens) {
    const relativePosition = cursorRelativePosition(queryPosition, token);
    if (relativePosition == "before") {
      break; // looking at tokens beyond the query position line
    } else if (relativePosition == "at") {
      return (
        isMultiLineCommentToken(token.type, lexerMode) ||
        isSingleLineCommentToken(token.type)
      );
    }
  }
  return false;
}

function isMultiLineCommentToken(
  tokenType: number,
  lexerMode: number
): boolean {
  return (
    // If we are after an unclosed multiline comment (e.g. /*  ^EOF) we need to check
    // the lexical mode since no token is yet lexed
    (tokenType == Token.EOF &&
      lexerMode == LiveEditLexer.IN_MULTI_LINE_COMMENT) ||
    tokenType == LiveEditParser.MULTI_LINE_COMMENT
  );
}

function isSingleLineCommentToken(tokenType: number): boolean {
  return tokenType == LiveEditParser.DQID_SINGLE_LINE_COMMENT;
}

function cursorRelativePosition(
  queryPosition: CursorQueryPosition,
  token: Token
): CursorRelativePosition {
  const tokenTextLines: string[] = token.text?.split("\n") || [];
  if (tokenTextLines[tokenTextLines.length - 1] == "") {
    tokenTextLines.pop();
  }
  const tokenEndLine =
    token.line + (tokenTextLines.length > 1 ? tokenTextLines.length - 1 : 0);
  const tokenEndCol =
    tokenTextLines.length > 1
      ? tokenTextLines[tokenTextLines.length - 1].length
      : token.charPositionInLine + (token.text?.length ?? 0);
  if (
    queryPosition.line < token.line ||
    (queryPosition.line == token.line &&
      queryPosition.column < token.charPositionInLine)
  ) {
    return "before";
  } else {
    if (
      queryPosition.line == tokenEndLine &&
      queryPosition.column > tokenEndCol
    ) {
      return "afterSameLine";
    } else if (queryPosition.line > tokenEndLine) {
      return "afterLaterLine";
    } else {
      return "at";
    }
  }
}
