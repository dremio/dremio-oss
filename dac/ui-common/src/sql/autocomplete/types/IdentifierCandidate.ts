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

import { LiveEditParser } from "../../../../target/generated-sources/antlr/LiveEditParser";
import { assertNever } from "../../../utilities/typeUtils";
import type { IdentifierRuleIndex, RuleList } from "./RuleIndex";

export type IdentifierRuleType =
  | "compoundIdentifier"
  | "simpleIdentifier"
  | "identifier";

export class IdentifierCandidate {
  public readonly ruleIndex: IdentifierRuleIndex;
  public readonly ruleList: RuleList;

  constructor(ruleIndex: IdentifierRuleIndex, ruleList: RuleList) {
    this.ruleIndex = ruleIndex;
    this.ruleList = ruleList;
  }

  getIdentifierRuleType(): IdentifierRuleType {
    switch (this.ruleIndex) {
      case LiveEditParser.RULE_compoundIdentifier:
        return "compoundIdentifier";
      case LiveEditParser.RULE_simpleIdentifier:
        return "simpleIdentifier";
      case LiveEditParser.RULE_identifier:
        return "identifier";
      default:
        assertNever(this.ruleIndex);
    }
  }

  toString() {
    return [...this.ruleList, this.ruleIndex]
      .map((ruleIndex) => LiveEditParser.ruleNames[ruleIndex])
      .join(" -> ");
  }
}
