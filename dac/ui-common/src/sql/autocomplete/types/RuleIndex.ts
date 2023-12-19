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

export type IdentifierRuleIndex =
  | typeof LiveEditParser.RULE_compoundIdentifier
  | typeof LiveEditParser.RULE_simpleIdentifier
  | typeof LiveEditParser.RULE_identifier;

export type RuleIndex = number;

export type RuleList = RuleIndex[];
