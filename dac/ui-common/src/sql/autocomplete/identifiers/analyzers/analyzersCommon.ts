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

import type { RuleContext, Token } from "antlr4ts";
import type { TerminalNode } from "antlr4ts/tree/TerminalNode";

import { LiveEditParser } from "../../../../../target/generated-sources/antlr/LiveEditParser";
import { findLastIndex, lastIndexOf } from "../../../../utilities/array";
import { assertNever } from "../../../../utilities/typeUtils";
import type { IdentifierCandidate } from "../../types/IdentifierCandidate";
import type { RuleIndex, RuleList } from "../../types/RuleIndex";

export type ChildRule = {
  ruleIndex: RuleIndex;
  applies: (priorToken: Token | undefined, ruleContext: RuleContext) => boolean;
};

export type RuleAnalyzers =
  | {
      type: "composite";
      analyzers: CompositeRuleAnalyzers;
    }
  | {
      type: "direct";
      analyzers: DirectRuleAnalyzers;
    };

export type DirectRuleAnalyzers = {
  [parentRuleIndex: number]: DirectRuleAnalyzer;
};

export type DirectRuleAnalyzer = ChildRule[] | "*";

export type CompositeRuleAnalyzers = {
  [parentRuleIndex: number]: CompositeRuleAnalyzer;
};

/**
 * Exclude rules:
 * Normally you would use this instead of includeIf for low-level rules that do not themselves define whether to include
 * child rules without some higher rule already including this parent rule itself.
 * E.g. for analyzing table names, tableFunctionCall includes namedRoutineCall, and namedRoutineCall must either
 */
export type CompositeRuleAnalyzer =
  | {
      type: "include-if" | "exclude-if" | "exclude-if-not";
      childRules: ChildRule[];
    }
  | {
      type: "include-all";
    }
  | {
      type: "exclude-all";
    };

export function validateCompositeRules(
  identifierCandidate: IdentifierCandidate,
  analyzers: CompositeRuleAnalyzers,
  priorTerminal: TerminalNode,
  fallbackRule?: (identifierRuleIndex: number) => boolean,
): boolean {
  const { ruleIndex, ruleList } = identifierCandidate;
  const ruleListWithIdentifier = [...ruleList, ruleIndex];
  const searchStartIndex = getSearchStartIndex(ruleListWithIdentifier);
  let isIdentifier: true | null = null;
  for (let i = searchStartIndex; i < ruleListWithIdentifier.length - 1; i++) {
    // To be an identifier of this type, there must be an ancestor rule with includeAll or matching include-if,
    // and no ancestors with excludeAll, matching exclude-if, or non-matching include-if
    const ruleAnalyzer = analyzers[ruleListWithIdentifier[i]];
    if (!ruleAnalyzer) {
      continue;
    }
    const nextRule = ruleListWithIdentifier[i + 1];
    if (ruleAnalyzer.type == "include-all") {
      isIdentifier = true;
    } else if (ruleAnalyzer.type == "exclude-all") {
      return false;
    } else if (ruleAnalyzer.type == "include-if") {
      if (
        ruleAnalyzer.childRules.some(
          (childRule) =>
            childRule.ruleIndex == nextRule &&
            childRule.applies(
              priorTerminal.symbol,
              priorTerminal.parent!.ruleContext,
            ),
        )
      ) {
        isIdentifier = true;
      } else if (isIdentifier) {
        return false;
      }
    } else if (ruleAnalyzer.type == "exclude-if") {
      if (
        ruleAnalyzer.childRules.some(
          (childRule) =>
            childRule.ruleIndex == nextRule &&
            childRule.applies(
              priorTerminal.symbol,
              priorTerminal.parent!.ruleContext,
            ),
        )
      ) {
        return false;
      }
    } else if (ruleAnalyzer.type == "exclude-if-not") {
      if (
        !ruleAnalyzer.childRules.some(
          (childRule) =>
            childRule.ruleIndex == nextRule &&
            childRule.applies(
              priorTerminal.symbol,
              priorTerminal.parent!.ruleContext,
            ),
        )
      ) {
        return false;
      }
    } else {
      assertNever(ruleAnalyzer.type);
    }
  }
  return isIdentifier || !!fallbackRule?.(ruleIndex);
}

export function validateDirectRules(
  identifierCandidate: IdentifierCandidate,
  analyzers: DirectRuleAnalyzers,
  priorTerminal: TerminalNode,
  fallbackRule?: (identifierRuleIndex: number) => boolean,
): boolean {
  const { ruleIndex, ruleList } = identifierCandidate;
  const ruleListWithIdentifier = [...ruleList, ruleIndex];
  const searchStartIndex = getSearchStartIndex(ruleListWithIdentifier);
  let isIdentifier: true | null = null;
  for (let i = searchStartIndex; i < ruleListWithIdentifier.length - 1; i++) {
    // To be an identifier of this type, we must be able to reach the identifier using only listed rules
    const ruleAnalyzer = analyzers[ruleListWithIdentifier[i]];
    if (!ruleAnalyzer) {
      return false;
    }
    const nextRule = ruleListWithIdentifier[i + 1];
    if (
      ruleAnalyzer == "*" ||
      ruleAnalyzer.some(
        (childRule) =>
          childRule.ruleIndex == nextRule &&
          childRule.applies(
            priorTerminal.symbol,
            priorTerminal.parent!.ruleContext,
          ),
      )
    ) {
      isIdentifier = true;
    } else {
      return false;
    }
  }
  return isIdentifier || !!fallbackRule?.(ruleIndex);
}

function getSearchStartIndex(ruleListWithIdentifier: RuleList): number {
  return (
    lastIndexOf(ruleListWithIdentifier, LiveEditParser.RULE_leafQuery) ??
    findLastIndex(
      ruleListWithIdentifier,
      ({ index }) =>
        index > 0 &&
        ruleListWithIdentifier[index - 1] == LiveEditParser.RULE_sqlStmt,
    ) ??
    0
  );
}

export function isTokenOfType(
  token: Token | undefined,
  type: number | number[],
) {
  const types: number[] = typeof type == "number" ? [type] : type;
  return !!token && types.includes(token.type);
}

export function areTokensOfType(tokens: Token[], priorTokenTypes: number[][]) {
  if (tokens.length != priorTokenTypes.length) {
    return false;
  }

  for (let i = 0; i < tokens.length; i++) {
    if (!priorTokenTypes[i].includes(tokens[i].type)) {
      return false;
    }
  }
  return true;
}

/** Include all listed child rules (that applies()) */
export function includeIf(childRules: ChildRule[]): CompositeRuleAnalyzer {
  return { type: "include-if" as const, childRules };
}

/** Exclude all listed child rules (that applies()) */
export function excludeIf(childRules: ChildRule[]): CompositeRuleAnalyzer {
  return { type: "exclude-if" as const, childRules };
}

/**
 * Exclude all child rules except the listed ones (that applies()). Equivalent to excludeIf of the complement set.
 */
export function excludeIfNot(childRules: ChildRule[]): CompositeRuleAnalyzer {
  return { type: "exclude-if-not" as const, childRules };
}

/** Include all child rules */
export function includeAll(): CompositeRuleAnalyzer {
  return { type: "include-all" as const };
}

/** Exclude all child rules */
export function excludeAll(): CompositeRuleAnalyzer {
  return { type: "exclude-all" as const };
}

export function child(
  ruleIndex: number,
  applies?: (
    priorToken: Token | undefined,
    ruleContext: RuleContext,
  ) => boolean,
): ChildRule {
  return { ruleIndex, applies: applies ?? (() => true) };
}
