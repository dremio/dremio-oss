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
  type DirectRuleAnalyzers,
  child,
  isTokenOfType,
  validateDirectRules,
} from "./analyzersCommon";
import type { IdentifierCandidate } from "../../types/IdentifierCandidate";

export const virtualDatasetTokens = [Parser.VDS, Parser.VIEW];
export const physicalDatasetTokens = [Parser.PDS, Parser.TABLE];
export const tableTokens = [
  ...virtualDatasetTokens,
  ...physicalDatasetTokens,
  Parser.DATASET,
];

export const tableRuleAnalyzers: DirectRuleAnalyzers = {
  [Parser.RULE_sqlDescribe]: [
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      isTokenOfType(priorToken, [Parser.DESCRIBE, Parser.TABLE])
    ),
  ],
  [Parser.RULE_sqlDescribeTable]: [
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      isTokenOfType(priorToken, [Parser.DESCRIBE, Parser.DESC])
    ),
  ],
  [Parser.RULE_sqlDropView]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlGrant]: [
    child(Parser.RULE_sqlGrantPrivilege),
    child(Parser.RULE_sqlGrantOwnership),
  ],
  [Parser.RULE_sqlGrantPrivilege]: [
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      isTokenOfType(priorToken, tableTokens)
    ),
  ],
  [Parser.RULE_sqlRevoke]: [
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      isTokenOfType(priorToken, tableTokens)
    ),
  ],
  [Parser.RULE_sqlGrantOwnership]: [
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      isTokenOfType(priorToken, tableTokens)
    ),
  ],
  [Parser.RULE_sqlTruncateTable]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlInsertTable]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlUpdateTable]: [
    child(Parser.RULE_compoundIdentifier),
    child(Parser.RULE_fromClause),
  ],
  [Parser.RULE_sqlMergeIntoTable]: [
    child(Parser.RULE_compoundIdentifier),
    child(Parser.RULE_tableRef),
  ],
  [Parser.RULE_sqlDropTable]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlRollbackTable]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlVacuum]: [child(Parser.RULE_sqlVacuumTable)],
  [Parser.RULE_sqlVacuumTable]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlAnalyzeTableStatistics]: [
    child(Parser.RULE_compoundIdentifier),
  ],
  [Parser.RULE_sqlRefreshDataset]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlOptimize]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlAccel]: [
    child(Parser.RULE_compoundIdentifier, (priorToken: Token | undefined) =>
      isTokenOfType(priorToken, tableTokens)
    ),
  ],
  [Parser.RULE_sqlDeleteFromTable]: [
    child(Parser.RULE_compoundIdentifier),
    child(Parser.RULE_fromClause),
  ],
  [Parser.RULE_tableRef3]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlCreateOrReplace]: [
    child(
      Parser.RULE_compoundIdentifier,
      (priorToken: Token | undefined, ruleContext: RuleContext) =>
        isTokenOfType(priorToken, virtualDatasetTokens) &&
        ruleContext instanceof SqlCreateOrReplaceContext &&
        !!ruleContext.REPLACE()
    ),
  ],
  [Parser.RULE_explicitTable]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlSelect]: [child(Parser.RULE_fromClause)],
  [Parser.RULE_fromClause]: [child(Parser.RULE_join)],
  [Parser.RULE_join]: [
    child(Parser.RULE_tableRef1),
    child(Parser.RULE_joinTable),
  ],
  [Parser.RULE_tableRef]: [child(Parser.RULE_tableRef3)],
  [Parser.RULE_tableRef1]: [child(Parser.RULE_tableRef3)],
  [Parser.RULE_joinTable]: [
    child(Parser.RULE_tableRef1),
    child(Parser.RULE_tableRef2),
  ],
  [Parser.RULE_tableRef2]: [child(Parser.RULE_tableRef3)],
  [Parser.RULE_tableRef3]: [
    child(Parser.RULE_tableRefWithHintsOpt),
    child(Parser.RULE_parenthesizedExpression),
  ],
  [Parser.RULE_tableRefWithHintsOpt]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_parenthesizedExpression]: [
    child(Parser.RULE_exprOrJoinOrOrderedQuery),
  ],
  [Parser.RULE_exprOrJoinOrOrderedQuery]: [
    child(Parser.RULE_query), // todo keep? for tables in scope resolution subqueries
    child(Parser.RULE_tableRef1),
    child(Parser.RULE_joinTable),
  ],
  [Parser.RULE_orderedQueryOrExpr]: [child(Parser.RULE_queryOrExpr)],
  [Parser.RULE_queryOrExpr]: [child(Parser.RULE_leafQueryOrExpr)],
  [Parser.RULE_query]: [child(Parser.RULE_leafQuery)],
  [Parser.RULE_leafQueryOrExpr]: [child(Parser.RULE_leafQuery)],
  [Parser.RULE_leafQuery]: [
    child(Parser.RULE_sqlSelect),
    child(Parser.RULE_explicitTable),
  ],
  [Parser.RULE_sqlQueryOrDml]: [child(Parser.RULE_orderedQueryOrExpr)],
  [Parser.RULE_sqlStmt]: "*",
  [Parser.RULE_sqlShowCreate]: [child(Parser.RULE_compoundIdentifier)],
  [Parser.RULE_sqlShowTableProperties]: [child(Parser.RULE_compoundIdentifier)],
};

export function isTable(
  priorTerminals: TerminalNode[],
  identifierCandidate: IdentifierCandidate
): boolean {
  if (priorTerminals.length == 0) {
    return false;
  }
  const priorTerminal = priorTerminals[priorTerminals.length - 1];
  return validateDirectRules(
    identifierCandidate,
    tableRuleAnalyzers,
    priorTerminal
  );
}
