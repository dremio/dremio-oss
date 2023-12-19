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

import { RuleContext } from "antlr4ts";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";

export function findAncestorOfType<T extends RuleContext>(
  context: TerminalNode | RuleContext,
  predicate: (ruleContext: RuleContext) => ruleContext is T
): T | undefined {
  let ruleContext: RuleContext | undefined = context._parent?.ruleContext;
  while (ruleContext) {
    if (predicate(ruleContext)) {
      return ruleContext;
    }
    ruleContext = ruleContext.parent;
  }
  return undefined;
}

export function findAncestor(
  context: TerminalNode | RuleContext,
  predicate: (ruleContext: RuleContext) => boolean
): RuleContext | undefined {
  let ruleContext: RuleContext | undefined = context._parent?.ruleContext;
  while (ruleContext) {
    if (predicate(ruleContext)) {
      return ruleContext;
    }
    ruleContext = ruleContext.parent;
  }
  return undefined;
}
