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

import type { ParseTree } from "antlr4ts/tree/ParseTree";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";

import type { CursorQueryPosition } from "../types/CursorQueryPosition";
import { getText } from "./tokenUtils";
import { LiveEditParser } from "../../../../target/generated-sources/antlr/LiveEditParser";

export type CursorInfo = CursorPosition & {
  priorTerminals: TerminalNode[];
};

type CursorPosition = {
  tokenIndex: number;
} & (
  | {
      kind: "at";
      terminal: TerminalNode;
      prefix: string;
      isQuoted: boolean;
    }
  | {
      kind: "before";
    }
);

/**
 * These are a list of tokens that when the cursor is right after (e.g. tbl.^), suggestions should be
 * collected for what can appear after them, not at their position. In other cases, e.g. SELE^ we want
 * to suggest tokens that can appear at their position (a.k.a. continue the token). These listed tokens
 * can't be continued so the cursor is never allowed to be considered to be on them.
 */
const NON_WS_SEPARATED_TOKENS = [LiveEditParser.DOT, LiveEditParser.LPAREN];

/**
 * Normal cases
 * - ^ -> Before 0 (EOF)
 * - S^ -> At 0 (S), Prefix S
 * - SE^LECT -> At 0 (SELECT), Prefix SE
 * - SELECT ^ FROM -> Before 1 (FROM)
 * - ^SELECT -> Before 0 (SELECT)
 *
 * Special cases: If cursor is on a special symbol, it is treated as before the next token
 * - tbl.^ -> Before 2 (EOF)
 * - (^) -> Before 1 (")")
 */
export function computeCursorInfo(
  parseTree: ParseTree,
  caretQueryPosition: CursorQueryPosition,
): CursorInfo | undefined {
  const priorTerminals: TerminalNode[] = [];
  const cursorPosition = findCursorPositionFromTree(
    parseTree,
    caretQueryPosition,
    priorTerminals,
  );
  return cursorPosition ? { ...cursorPosition, priorTerminals } : undefined;
}

function findCursorPositionFromTree(
  parseTree: ParseTree,
  caretQueryPosition: CursorQueryPosition,
  priorTerminals: TerminalNode[],
): CursorPosition | undefined {
  for (let i = 0; i < parseTree.childCount; i++) {
    const child = parseTree.getChild(i);
    let cursorPosition: CursorPosition | undefined;
    if (child instanceof TerminalNode) {
      if (child.symbol.tokenIndex === -1) {
        // Skip this error terminal - it must have been created by ANTLR default error strategy and corresponds to a missing
        // inserted token rather than a real token created from the lexer
        // https://github.com/tunnelvisionlabs/antlr4ts/blob/4d58e24ff4b92366acb5b4f0c61c54306ba71e1f/src/DefaultErrorStrategy.ts#L610
        continue;
      }
      cursorPosition = findCursorPositionFromTerminal(
        child,
        caretQueryPosition,
      );
    } else {
      cursorPosition = findCursorPositionFromTree(
        child,
        caretQueryPosition,
        priorTerminals,
      );
    }
    // If we have found the correct terminal, stop, otherwise keep looking
    if (cursorPosition !== undefined) {
      return cursorPosition;
    }
    if (child instanceof TerminalNode) {
      priorTerminals.push(child);
    }
  }
  return undefined;
}

function findCursorPositionFromTerminal(
  terminal: TerminalNode,
  caretQueryPosition: CursorQueryPosition,
): CursorPosition | undefined {
  const startPos: number = terminal.symbol.charPositionInLine;
  const endPos: number = startPos + terminal.text.length;
  if (
    terminal.symbol.line > caretQueryPosition.line ||
    (terminal.symbol.line == caretQueryPosition.line &&
      startPos >= caretQueryPosition.column)
  ) {
    // The caret lies between terminals (e.g. on whitespace) or right before
    // the terminal (e.g. ^SELECT)
    return { tokenIndex: terminal.symbol.tokenIndex, kind: "before" };
  } else if (
    terminal.symbol.line == caretQueryPosition.line &&
    startPos < caretQueryPosition.column &&
    (endPos > caretQueryPosition.column ||
      (endPos == caretQueryPosition.column &&
        !NON_WS_SEPARATED_TOKENS.includes(terminal.symbol.type)))
  ) {
    // The caret lies in the terminal (e.g. SE^LECT) or—if it's not a
    // non-whitespace-separated token—at the end of the terminal (e.g. SELECT^)
    const terminalText = getText(terminal, caretQueryPosition.column);
    return {
      tokenIndex: terminal.symbol.tokenIndex,
      kind: "at",
      terminal,
      prefix: terminalText.text ?? "",
      isQuoted: terminalText.isQuoted,
    };
  } else {
    return undefined;
  }
}

export function getPriorTerminal(
  cursorInfo: CursorInfo,
  /** defaults to 0 (terminal directly prior) */
  offset?: number,
): TerminalNode | undefined {
  return cursorInfo.priorTerminals[
    cursorInfo.priorTerminals.length - (1 + (offset ?? 0))
  ];
}

export function getCursorTerminal(
  cursorInfo: CursorInfo,
): TerminalNode | undefined {
  return cursorInfo.kind == "at" ? cursorInfo.terminal : undefined;
}

export function getCursorPrefix(cursorInfo: CursorInfo): string {
  return cursorInfo.kind == "at" ? cursorInfo.prefix : "";
}

export function getCursorPrefixQuoted(cursorInfo: CursorInfo): boolean {
  return cursorInfo.kind == "at" ? cursorInfo.isQuoted : false;
}
