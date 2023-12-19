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
import type { RuleContext } from "antlr4ts";

import {
  LiveEditParser,
  CompoundIdentifierContext,
  IdentifierContext,
  SimpleIdentifierCommaListContext,
  SimpleIdentifierContext,
  UnsignedIntLiteralContext,
} from "../../../../target/generated-sources/antlr/LiveEditParser";
import { assertNever } from "../../../utilities/typeUtils";
import { getTokenType } from "../../parser/utils/liveEditQueryParserUtils";
import { findAncestorOfType } from "../../parser/utils/ruleUtils";
import {
  type CursorInfo,
  getCursorPrefix,
  getCursorPrefixQuoted,
  getCursorTerminal,
  getPriorTerminal,
} from "../parsing/cursorInfo";
import { getText } from "../parsing/tokenUtils";

/** Representation of a component of a compound identifier */
export type CompoundIdentifierPart = {
  bracketedIndex?: number[];
  startTokenIndex: number;
  /** defined if different from startTokenIndex */
  stopTokenIndex?: number;
} & (
  | {
      type: "text";
      text: string;
      isQuoted: boolean;
    }
  | {
      type: "star";
    }
);

export function getCompoundIdentifierParts(
  compoundIdentifier: CompoundIdentifierContext
): CompoundIdentifierPart[] {
  const parts: CompoundIdentifierPart[] = [];
  for (const child of compoundIdentifier.children || []) {
    if (child instanceof SimpleIdentifierContext) {
      const { text, isQuoted } = getText(child);
      parts.push({
        type: "text",
        text,
        isQuoted,
        startTokenIndex: child.start.tokenIndex,
      });
    } else if (child instanceof UnsignedIntLiteralContext) {
      const num = parseInt(child.text, 10);
      const part: CompoundIdentifierPart | undefined = parts[parts.length - 1];
      if (part?.bracketedIndex) {
        part.bracketedIndex.push(num);
      } else if (part) {
        part.bracketedIndex = [num];
      }
    } else if (child instanceof TerminalNode) {
      if (child.symbol.type === LiveEditParser.STAR) {
        parts.push({
          type: "star",
          startTokenIndex: child.symbol.tokenIndex,
        });
      } else if (child.symbol.type === LiveEditParser.RBRACKET) {
        const part: CompoundIdentifierPart | undefined =
          parts[parts.length - 1];
        if (part) {
          part.stopTokenIndex = child.symbol.tokenIndex;
        }
      }
    }
  }
  return parts;
}

export type IdentifierInfo = {
  prefix: string;
  prefixQuoted: boolean;
  parentPath: string[];
  completable: boolean;
  terminal?: TerminalNode;
};

export function getIdentifierInfo(cursorInfo: CursorInfo): IdentifierInfo {
  let prefix: string = getCursorPrefix(cursorInfo);
  let prefixQuoted = getCursorPrefixQuoted(cursorInfo);
  let parentPath: string[] = [];
  let completable = true;

  let terminal = getCursorTerminal(cursorInfo);
  let dotCursor = false;
  if (
    cursorInfo.kind == "before" &&
    getPriorTerminal(cursorInfo)?.symbol.type === LiveEditParser.DOT
  ) {
    // If we're at a DOT terminal (kind is "before" and prior is DOT -- see comment in cursorInfo.ts), we can't
    // necessarily use its ruleContext to locate the identifier that wraps it.
    // This is because in the case of an invalid input, ANTLR will only attribute the "bad" token (here, a trailing DOT)
    // to a rule context if it can unambigously parse it as part of that rule. Consider the following case:
    //   WHERE tbl.^
    // Here, the DOT cannot be attributed to a compoundIdentifier rule context because there are technically two parser
    // rules that could consume it: atomicRowExpression -> compoundIdentifier, and atomicRowExpression ->
    // namedFunctionCall -> functionName -> compoundIdentifier.
    // ANTLR employs the single token deletion error recovery strategy to try to remove a single token to make the input
    // valid: by removing the DOT it can recover to finish parsing by determining tbl is the entire compoundIdentifier
    // and is the atomicRowExpression -> compoundIdentifier rule context, because the namedFunctionCall option would
    // require additional subsequent tokens and a single token deletion alone wouldn't be able to make the query valid.
    // So, if there is a DOT at the cursor, we infer the identifier context by looking at the preceding token (which
    // should be a valid identifier segment and if not there's not much we can do..)
    terminal = getPriorTerminal(cursorInfo, 1);
    dotCursor = true;
  }

  if (!terminal) {
    return {
      prefix: "",
      prefixQuoted: false,
      parentPath: [],
      completable: true,
    };
  }

  let cursorTokenIndex = terminal.symbol.tokenIndex;
  const compoundIdentifier = findAncestorOfType(
    terminal,
    (ruleContext): ruleContext is CompoundIdentifierContext =>
      ruleContext instanceof CompoundIdentifierContext
  );
  if (compoundIdentifier) {
    const identifierParts = getCompoundIdentifierParts(compoundIdentifier);
    for (const identifierPart of identifierParts) {
      // Has to be at or before the cursor terminal
      if (identifierPart.startTokenIndex > cursorTokenIndex) {
        break;
      }
      let text: string;
      let isQuoted: boolean = false;
      const isLastPart =
        (identifierPart.startTokenIndex === cursorTokenIndex ||
          (identifierPart?.stopTokenIndex ?? -1) >= cursorTokenIndex) &&
        !dotCursor;
      switch (identifierPart.type) {
        case "text":
          text = identifierPart.text;
          isQuoted = identifierPart.isQuoted;
          break;
        case "star":
          text = "*";
          completable = isLastPart; // *.col is invalid
          break;
        default:
          assertNever(identifierPart);
      }
      if (isLastPart) {
        prefix = text;
        prefixQuoted = isQuoted;
        completable =
          completable &&
          (identifierPart.type == "star" || !identifierPart.bracketedIndex);
      } else {
        parentPath.push(text);
      }
    }
  } else if (dotCursor) {
    // todo add test SELECT reservedKeyword^ FROM tbl (only columns starting with reservedKeyword)
    // todo add test SELECT *^ FROM tbl (only columns starting with star)
    // todo add test SELECT ABS(tbl.^) FROM tbl
    parentPath.push(getText(terminal).text);
  }

  return { prefix, prefixQuoted, parentPath, completable, terminal };
}

export function getUnquotedSimpleIdentifiers(
  list: SimpleIdentifierCommaListContext
): string[] {
  return list.simpleIdentifier().map((identifier) => getText(identifier).text);
}

export function needsQuoting(text: string): boolean {
  // This is a bit stricter than the IDENTIFIER lexer rule for simplicity (ok if we add double quotes in some edge
  // cases where not strictly needed)
  return (
    !text.match(/^[a-zA-Z][a-zA-Z0-9_$]*$/) ||
    getTokenType(text) === "reservedKeyword"
  );
}

export function excludeTrailingIdentifiers(
  priorTerminals: TerminalNode[]
): TerminalNode[] {
  let i = priorTerminals.length - 1;
  let priorTerminal = priorTerminals[i];
  const isIdentifier = (
    ruleContext: TerminalNode | RuleContext
  ): ruleContext is
    | CompoundIdentifierContext
    | SimpleIdentifierContext
    | IdentifierContext =>
    ruleContext instanceof CompoundIdentifierContext ||
    ruleContext instanceof SimpleIdentifierContext ||
    ruleContext instanceof IdentifierContext;
  while (
    priorTerminal &&
    (priorTerminal.symbol.type == LiveEditParser.DOT ||
      isIdentifier(priorTerminal))
  ) {
    priorTerminal = priorTerminals[i--];
  }
  return priorTerminals.slice(0, i + 1);
}
