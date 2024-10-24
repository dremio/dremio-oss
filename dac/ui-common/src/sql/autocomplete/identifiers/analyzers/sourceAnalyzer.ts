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

import type { Token } from "antlr4ts";
import type { TerminalNode } from "antlr4ts/tree/TerminalNode";

import { LiveEditParser as Parser } from "../../../../../target/generated-sources/antlr/LiveEditParser";
import {
  type CompositeRuleAnalyzers,
  child,
  includeIf,
  isTokenOfType,
  validateCompositeRules,
} from "./analyzersCommon";
import type { IdentifierCandidate } from "../../types/IdentifierCandidate";

const sourceTokens = [Parser.SOURCE, Parser.CATALOG];
const sqlVersionSourceTokens = [Parser.FROM, Parser.IN];

const followsSqlVersionSourceToken = (priorToken: Token | undefined) =>
  isTokenOfType(priorToken, sqlVersionSourceTokens);

const followsSourceToken = (priorToken: Token | undefined) =>
  isTokenOfType(priorToken, sourceTokens);

const sourceRuleAnalyzers: CompositeRuleAnalyzers = {
  [Parser.RULE_sqlUseVersion]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlShowBranches]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlShowTags]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlShowLogs]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlCreateBranch]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlCreateTag]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlDropBranch]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlDropTag]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlMergeBranch]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlAssignBranch]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlAssignTag]: includeIf([
    child(Parser.RULE_simpleIdentifier, (priorToken: Token | undefined) =>
      followsSqlVersionSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlGrantPrivilege]: includeIf([
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      followsSourceToken(priorToken),
    ),
  ]),
  [Parser.RULE_sqlRevoke]: includeIf([
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      followsSourceToken(priorToken),
    ),
  ]),
};

export function isSource(
  priorTerminals: TerminalNode[],
  identifierCandidate: IdentifierCandidate,
): boolean {
  if (priorTerminals.length == 0) {
    return false;
  }
  const priorTerminal = priorTerminals[priorTerminals.length - 1];
  const fallbackRule = (identifierRuleIndex: number) =>
    identifierRuleIndex == Parser.RULE_simpleIdentifier &&
    isTokenOfType(priorTerminal.symbol, sourceTokens);
  return validateCompositeRules(
    identifierCandidate,
    sourceRuleAnalyzers,
    priorTerminal,
    fallbackRule,
  );
}
