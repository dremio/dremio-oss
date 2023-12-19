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
import { isTokenOfType } from "./analyzersCommon";
import type { IdentifierCandidate } from "../../types/IdentifierCandidate";

export function isSpace(
  priorTerminals: TerminalNode[],
  identifierCandidate: IdentifierCandidate
): boolean {
  if (priorTerminals.length == 0) {
    return false;
  }
  return isTokenOfType(
    priorTerminals[priorTerminals.length - 1].symbol,
    Parser.SPACE
  )
    ? identifierCandidate.ruleIndex == Parser.RULE_simpleIdentifier
    : false;
}
