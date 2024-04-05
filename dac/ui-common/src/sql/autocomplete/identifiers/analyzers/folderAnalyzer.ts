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

const folderTokens = [Parser.FOLDER, Parser.SCHEMA];

const folderRuleAnalyzers: CompositeRuleAnalyzers = {
  // [Parser.RULE_sqlCreateFolder]: excludeAll(), // re-generate grammar
  [Parser.RULE_sqlUseSchema]: includeIf([
    child(Parser.RULE_compoundIdentifier),
  ]),
};

export function isFolder(
  priorTerminals: TerminalNode[],
  identifierCandidate: IdentifierCandidate,
): boolean {
  if (priorTerminals.length == 0) {
    return false;
  }
  const priorTerminal = priorTerminals[priorTerminals.length - 1];
  const fallbackRule = (identifierRuleIndex: number) =>
    identifierRuleIndex == Parser.RULE_compoundIdentifier &&
    isTokenOfType(priorTerminal.symbol, folderTokens);
  return validateCompositeRules(
    identifierCandidate,
    folderRuleAnalyzers,
    priorTerminal,
    fallbackRule,
  );
}
