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
import type { TerminalNode } from "antlr4ts/tree/TerminalNode";
import { CandidatesCollection, CodeCompletionCore } from "antlr4-c3";

import {
  ArrayConstructorContext,
  LiveEditParser,
  BinaryQueryOperatorContext,
  MapConstructorContext,
  MultisetConstructorContext,
  QueryOrExprContext,
  SqlCreateOrReplaceContext,
  SqlCreateTableContext,
} from "../../../../target/generated-sources/antlr/LiveEditParser";
import {
  type Logger,
  getLoggingContext,
} from "../../../contexts/LoggingContext";
import { indexOf } from "../../../utilities/array";
import type {
  IdentifierRuleIndex,
  RuleIndex,
  RuleList,
} from "../types/RuleIndex";

const FUNCTION_NAME_RULE = LiveEditParser.RULE_functionName;
const EXPRESSION_RULE = LiveEditParser.RULE_expression;
const IDENTIFIER_RULE = LiveEditParser.RULE_identifierSegment;
const DISCARD_TOKEN_RULES = new Set([
  LiveEditParser.RULE_trailingSelectComma,
  LiveEditParser.RULE_trailingSelectItemDot,
  LiveEditParser.RULE_invalidFrom,
  LiveEditParser.RULE_invalidAs,
  LiveEditParser.RULE_contextVariable, // either unsupported functions or present in sqlFunctions list
  LiveEditParser.RULE_unusedExtension, // artifact from javacc use gated behind parsing exception
]);
/** These tokens have matching sqlFunctions entries */
const EXCLUDED_FUNCTION_TOKENS = new Set([
  LiveEditParser.CAST,
  LiveEditParser.EXTRACT,
  LiveEditParser.POSITION,
  LiveEditParser.CONVERT,
  LiveEditParser.TRANSLATE,
  LiveEditParser.OVERLAY,
  LiveEditParser.FLOOR,
  LiveEditParser.CEILING,
  LiveEditParser.SUBSTRING,
  LiveEditParser.TRIM,
  LiveEditParser.TIMESTAMPADD,
  LiveEditParser.TIMESTAMPDIFF,
  LiveEditParser.CLASSIFIER,
]);

export type Candidates = {
  tokens: Map<RuleIndex, RuleList>;
  functions: {
    isViable: boolean;
  };
  identifiers:
    | {
        isViable: false;
      }
    | {
        isViable: true;
        /** Top-level viable identifier rule: compoundIdentifier, simpleIdentifier, or identifier */
        ruleIndex: IdentifierRuleIndex;
        /**
         * List of rules, starting with the start rule and ending with the parent rule of the top-level identifier rule
         * that is viable */
        ruleList: RuleList;
      };
};

let _LOGGER: ReturnType<Logger>;
const getLogger = () => {
  if (typeof _LOGGER === "undefined") {
    _LOGGER = getLoggingContext().createLogger("collectCandidates");
  }
  return _LOGGER;
};

/**
 * This method uses the (consumed) token stream and the parser's Augmented Transition Network (ATN) to find viable
 * candidate tokens, function parser rules, and identifier parser rules at a given token index. Additionally, it uses
 * the parse tree in order to semantically exclude candidates by emulating the runtime-logic baked into the real backend
 * parser (derived from Parser.jj) that is not reflected in the grammar/generated ANTLR parser.
 *
 * Note: The candidates returned are independent of what current token exists at the provided index, if any.
 */
export function collectCandidates(
  parser: LiveEditParser,
  tokenIndex: number,
  priorTerminal: TerminalNode | undefined,
  queryParseTree: ParserRuleContext
): Candidates {
  const candidates = collectCandidatesC3(
    false,
    parser,
    tokenIndex,
    queryParseTree
  );
  if (hasValidExpressionSuggestion(candidates, priorTerminal)) {
    // Expand the expression to valid (non-function name) keywords
    const expandedCandidates = collectCandidatesC3(
      true,
      parser,
      tokenIndex,
      queryParseTree
    );
    for (const [tokenType, tokenList] of expandedCandidates.tokens) {
      candidates.tokens.set(tokenType, tokenList);
    }
    for (const [ruleNum, candidateRule] of expandedCandidates.rules) {
      candidates.rules.set(ruleNum, candidateRule);
    }
  }
  return {
    tokens: candidates.tokens,
    functions: functionsResult(candidates.rules),
    identifiers: identifiersResult(candidates.rules),
  };
}

function collectCandidatesC3(
  collectExpressionTokens: boolean,
  parser: LiveEditParser,
  tokenIndex: number,
  queryParseTree: ParserRuleContext
): CandidatesCollection {
  const core = new CodeCompletionCore(parser);
  const rulesToCollect: number[] = [
    FUNCTION_NAME_RULE,
    IDENTIFIER_RULE,
    ...DISCARD_TOKEN_RULES,
    ...(collectExpressionTokens ? [] : [EXPRESSION_RULE]),
  ];
  const tokensToIgnore: number[] = [
    ...EXCLUDED_FUNCTION_TOKENS,
    LiveEditParser.EOF,
  ];
  core.preferredRules = new Set(rulesToCollect);
  core.ignoredTokens = new Set(tokensToIgnore);

  return core.collectCandidates(tokenIndex, queryParseTree);
}

function hasValidExpressionSuggestion(
  candidates: CandidatesCollection,
  priorTerminal: TerminalNode | undefined
): boolean {
  if (!candidates.rules.has(EXPRESSION_RULE)) {
    return false;
  }
  const expressionRuleList: number[] =
    candidates.rules.get(LiveEditParser.RULE_expression)?.ruleList || [];
  return expressionSuggestionIsValid(expressionRuleList, priorTerminal);
}

// This emulates some of the functionality of javacc parser's use of ExprContext to throw exceptions to prevent
// expressions from appearing where only queries are expected (it uses ExprContext for other purposes too that are not
// supported here). Unfortunately this logic cannot be easily baked into the grammar to control suggestions because
// actions and semantic predicates are not well supported by antlr4 or antlr4-c3 in determining "next" possible tokens
// (they're mostly ignored)
function expressionSuggestionIsValid(
  expressionRuleList: number[],
  priorTerminal: TerminalNode | undefined
): boolean {
  if (!priorTerminal) {
    // Expressions cannot start a query (E.g. "FLOOR(1)"")
    return false;
  }
  if (
    priorTerminal.parent instanceof MapConstructorContext &&
    priorTerminal.parent.LPAREN() == priorTerminal
  ) {
    return false;
  } else if (
    priorTerminal.parent instanceof ArrayConstructorContext &&
    priorTerminal.parent.LPAREN() == priorTerminal
  ) {
    return false;
  } else if (
    priorTerminal.parent instanceof MultisetConstructorContext &&
    priorTerminal.parent.LPAREN() == priorTerminal
  ) {
    return false;
  } else if (
    priorTerminal.parent instanceof BinaryQueryOperatorContext &&
    priorTerminal.parent.parent instanceof QueryOrExprContext
  ) {
    return false;
  } else if (
    priorTerminal.parent instanceof SqlCreateOrReplaceContext &&
    priorTerminal.parent.AS() == priorTerminal
  ) {
    return false;
  } else if (
    priorTerminal.parent instanceof SqlCreateTableContext &&
    priorTerminal.parent.AS().slice(-1)[0] == priorTerminal
  ) {
    return false;
  }

  if (
    expressionRuleList[expressionRuleList.length - 1] ==
      LiveEditParser.RULE_leafQueryOrExpr &&
    expressionRuleList[expressionRuleList.length - 2] ==
      LiveEditParser.RULE_queryOrExpr &&
    expressionRuleList[expressionRuleList.length - 3] ==
      LiveEditParser.RULE_orderedQueryOrExpr
  ) {
    if (
      expressionRuleList[expressionRuleList.length - 4] ==
      LiveEditParser.RULE_sqlStmt
    ) {
      return false;
    } else if (
      expressionRuleList[expressionRuleList.length - 4] ==
      LiveEditParser.RULE_sqlInsertTable
    ) {
      return false;
    } else if (
      expressionRuleList[expressionRuleList.length - 4] ==
      LiveEditParser.RULE_sqlQueryOrDml
    ) {
      return false;
    }
  }

  // If not explicitly blocklisted already, assume it's a valid suggestion of expression - we are not exhaustive
  // but prefer mistakenly showing expression suggestions when invalid over not showing suggestions when valid
  return true;
}

const functionsResult = (
  rules: CandidatesCollection["rules"]
): Candidates["functions"] => ({ isViable: rules.has(FUNCTION_NAME_RULE) });

const identifiersResult = (
  rules: CandidatesCollection["rules"]
): Candidates["identifiers"] => {
  const identifierCandidate = rules.get(IDENTIFIER_RULE);
  if (!identifierCandidate) {
    return { isViable: false };
  }
  const identifierIdx: number | undefined =
    indexOf(
      identifierCandidate.ruleList,
      LiveEditParser.RULE_compoundIdentifier
    ) ??
    indexOf(
      identifierCandidate.ruleList,
      LiveEditParser.RULE_simpleIdentifier
    ) ??
    indexOf(identifierCandidate.ruleList, LiveEditParser.RULE_identifier);
  if (identifierIdx == undefined) {
    getLogger().warn(
      "Unexpected identifier rule list: " + identifierCandidate.ruleList
    );
    return { isViable: false };
  }
  const ruleList = identifierCandidate.ruleList.slice(0, identifierIdx);

  return {
    isViable: true,
    ruleIndex: identifierCandidate.ruleList[
      identifierIdx
    ] as IdentifierRuleIndex,
    ruleList,
  };
};
