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

import {
  LiveEditParser as Parser,
  SqlCreateOrReplaceContext,
} from "../../../../../target/generated-sources/antlr/LiveEditParser";
import {
  type CompositeRuleAnalyzers,
  child,
  includeIf,
  isTokenOfType,
  validateCompositeRules,
} from "./analyzersCommon";
import type { IdentifierCandidate } from "../../types/IdentifierCandidate";
import { virtualDatasetTokens } from "./tableAnalyzer";

const genericContainerRuleAnalyzers: CompositeRuleAnalyzers = {
  [Parser.RULE_sqlShowTables]: includeIf([
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      isTokenOfType(priorToken, [Parser.FROM, Parser.IN])
    ),
  ]),
  [Parser.RULE_sqlShowViews]: includeIf([
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      isTokenOfType(priorToken, [Parser.FROM, Parser.IN])
    ),
  ]),
  [Parser.RULE_sqlCreateOrReplace]: includeIf([
    // If we are creating new view, suggest entities it can be created inside
    child(
      Parser.RULE_compoundIdentifier,
      (priorToken: Token | undefined, ruleContext: RuleContext) =>
        isTokenOfType(priorToken, virtualDatasetTokens) &&
        ruleContext instanceof SqlCreateOrReplaceContext &&
        !ruleContext.REPLACE()
    ),
  ]),
  [Parser.RULE_sqlCreateTable]: includeIf([
    // If we are creating new table, suggest entities it can be created inside
    child(
      Parser.RULE_compoundIdentifier,
      (priorToken: Token | undefined, ruleContext: RuleContext) =>
        isTokenOfType(priorToken, virtualDatasetTokens) &&
        ruleContext instanceof SqlCreateOrReplaceContext &&
        !ruleContext.REPLACE()
    ),
  ]),
};

/**
 * Generic container is any container that can hold catalog entities:
 * Folders, sources, spaces (but not tables, columns, udfs)
 */
export function isGenericContainer(
  priorTerminals: TerminalNode[],
  identifierCandidate: IdentifierCandidate
): boolean {
  if (priorTerminals.length == 0) {
    return false;
  }
  const priorTerminal = priorTerminals[priorTerminals.length - 1];
  return validateCompositeRules(
    identifierCandidate,
    genericContainerRuleAnalyzers,
    priorTerminal
  );
}
