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

import { Lexer, Token, TokenStream } from "antlr4ts";
import type { RuleNode } from "antlr4ts/tree/RuleNode";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";

export function getText(
  node: TerminalNode | RuleNode,
  stopAtColumn?: number
): {
  text: string;
  isQuoted: boolean;
} {
  if (node instanceof TerminalNode && node.symbol.type === Token.EOF) {
    return { text: "", isQuoted: false };
  }
  let text = node.text;
  let isQuoted = false;
  if (text.startsWith('"') && text.endsWith('"')) {
    text = text.slice(1, text.length - 1).replace(/""/g, '"');
    isQuoted = true;
  }
  if (stopAtColumn && node instanceof TerminalNode) {
    let numCharsInclude = stopAtColumn - node.payload.charPositionInLine;
    if (numCharsInclude < 0) {
      numCharsInclude = 0;
    }
    text = text.slice(0, numCharsInclude);
  }
  return { text, isQuoted };
}

/** Returns previous token that participated in parsing (not hidden) */
export function getPriorToken(
  tokenStream: TokenStream,
  index: number
): Token | undefined {
  if (index == 0) {
    return undefined;
  }
  let token: Token | undefined = tokenStream.get(index - 1);
  // Skip over tokens on hidden channel
  while (token && token.channel !== Lexer.DEFAULT_TOKEN_CHANNEL) {
    token = index > 0 ? tokenStream.get(--index) : undefined;
  }
  return token;
}
