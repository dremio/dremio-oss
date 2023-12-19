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
import type { Token } from "antlr4ts/Token";

import { LiveEditParser as Parser } from "../../../../../target/generated-sources/antlr/LiveEditParser";
import {
  type CompositeRuleAnalyzers,
  child,
  excludeAll,
  excludeIf,
  includeAll,
  includeIf,
  isTokenOfType,
  validateCompositeRules,
} from "./analyzersCommon";
import type { IdentifierCandidate } from "../../types/IdentifierCandidate";

const columnRuleAnalyzers: CompositeRuleAnalyzers = {
  [Parser.RULE_orderByLimitOpt]: includeAll(), // ORDER BY columns
  [Parser.RULE_sqlDescribeTable]: includeIf([
    child(
      Parser.RULE_compoundIdentifier,
      (
        priorToken: Token | undefined // Column name to describe
      ) => !isTokenOfType(priorToken, [Parser.DESCRIBE, Parser.DESC])
    ),
  ]),
  [Parser.RULE_sqlDescribe]: includeIf([
    child(
      Parser.RULE_simpleIdentifier,
      (
        priorToken: Token | undefined // Column name to describe
      ) => !isTokenOfType(priorToken, [Parser.DESCRIBE, Parser.TABLE])
    ),
  ]),
  [Parser.RULE_sqlCreateOrReplace]: includeIf([child(Parser.RULE_policy)]), // Masking columns (referencing subquery only)
  [Parser.RULE_sqlCreateTable]: includeIf([
    // Columns reference subquery if no defined table element list
    child(Parser.RULE_policy), // Masking columns
    child(Parser.RULE_parsePartitionTransformList), // PARTITION BY columns
    child(Parser.RULE_parseRequiredFieldList), // DISTRIBUTE/LOCALSORT BY columns
  ]),

  [Parser.RULE_pivot]: excludeIf([child(Parser.RULE_pivotValue)]), // Pivot values
  [Parser.RULE_rowConstructor]: excludeAll(), // VALUES constructor
  [Parser.RULE_unpivot]: includeIf([child(Parser.RULE_unpivotValue)]), // Unpivot columns
  [Parser.RULE_unpivotValue]: includeIf([
    child(Parser.RULE_simpleIdentifierOrList), // Unpivot columns
  ]),
  [Parser.RULE_dataType]: excludeAll(), // data type names
  [Parser.RULE_policy]: includeIf([child(Parser.RULE_parseColumns)]), // Masking columns
  [Parser.RULE_sqlInsertTable]: includeIf([
    child(Parser.RULE_tableElementList), // Fields to insert into
  ]),
  [Parser.RULE_whereOpt]: includeAll(), // Within DELETE FROM, UPDATE, etc
  [Parser.RULE_sqlUpdateTable]: includeIf([
    child(
      Parser.RULE_simpleIdentifier,
      (
        priorToken: Token | undefined // Column to update
      ) => isTokenOfType(priorToken, [Parser.SET, Parser.COMMA])
    ),
    child(Parser.RULE_expression), // SET .. = <expression>
    child(Parser.RULE_whereOpt), // WHERE <conditions>
  ]),
  [Parser.RULE_sqlMergeIntoTable]: includeIf([
    child(Parser.RULE_expression), // ON .. = <expression>
    child(Parser.RULE_dremioWhenMatchedClause), // Matching rows to update
    child(Parser.RULE_dremioWhenNotMatchedClause), // Non-matching rows to insert
  ]),
  [Parser.RULE_sqlAnalyzeTableStatistics]: includeIf([
    child(Parser.RULE_parseOptionalFieldList), // Analyze columns
  ]),
  [Parser.RULE_sqlRefreshDataset]: includeIf([
    child(Parser.RULE_parseRequiredPartitionList), // Refresh partition columns
  ]),
  [Parser.RULE_sqlAccel]: includeIf([
    child(
      Parser.RULE_simpleIdentifier,
      (
        priorToken: Token | undefined // Column to change/alter/modify or drop
      ) =>
        isTokenOfType(priorToken, [
          Parser.CHANGE,
          Parser.ALTER,
          Parser.MODIFY,
          Parser.COLUMN,
          Parser.DROP,
        ])
    ),
    child(Parser.RULE_policy), // Row access policy to add/drop or column masking policy to set/unset
    child(Parser.RULE_parseRequiredFieldList), // Primary key column to add
    child(Parser.RULE_parsePartitionTransform), // Partition field to add/drop
    child(Parser.RULE_sqlCreateAggReflection), // Aggregate reflection to create (declares columns)
    child(Parser.RULE_sqlCreateRawReflection), // Raw reflection to create (declares columns)
    child(Parser.RULE_parseRequiredPartitionList), // Partition columns to refresh metadata of
  ]),
  [Parser.RULE_parseRequiredPartitionList]: includeAll(), // Partition columns
  [Parser.RULE_sqlSelect]: includeAll(), // Select command elements
  [Parser.RULE_selectItem]: excludeIf([child(Parser.RULE_simpleIdentifier)]), // Select item alias
  [Parser.RULE_tableRef3]: includeIf([
    child(Parser.RULE_matchRecognizeOpt), // MATCH_RECOGNIZE function
    child(Parser.RULE_pivot), // Pivot operator (defines columns to pivot)
    child(Parser.RULE_unpivot), // Unpivot operator (defines columns to unpivot)
  ]),
  [Parser.RULE_joinTable]: includeIf([
    child(Parser.RULE_expression), // ON expression
    child(Parser.RULE_parenthesizedSimpleIdentifierList), // USING list
  ]),
  [Parser.RULE_windowOpt]: excludeIf([child(Parser.RULE_simpleIdentifier)]), // Window name
  [Parser.RULE_windowSpecification]: excludeIf([
    child(Parser.RULE_simpleIdentifier), // Unused identifier
    child(Parser.RULE_windowRange), // Only numeric expression (and unsupported anyways)
  ]),
  [Parser.RULE_matchRecognizeOpt]: excludeIf([
    child(Parser.RULE_simpleIdentifier), // Pattern name
    child(Parser.RULE_subsetDefinitionCommaList), // Subset variables and correlation variables
  ]),
  [Parser.RULE_patternDefinition]: excludeIf([
    child(Parser.RULE_simpleIdentifier), // Pattern name
  ]),
  [Parser.RULE_patternExpression]: excludeAll(), // Pattern symbol (not column name)
  [Parser.RULE_measureColumn]: excludeIf([child(Parser.RULE_simpleIdentifier)]), // Alias
  [Parser.RULE_pivotAgg]: excludeIf([child(Parser.RULE_simpleIdentifier)]), // Alias
  [Parser.RULE_withItem]: excludeAll(), // Alias
  [Parser.RULE_functionName]: excludeAll(), // Function name
  [Parser.RULE_jdbcFunctionCall]: excludeIf([child(Parser.RULE_identifier)]), // JDBC function name
};

export function isColumn(
  priorTerminals: TerminalNode[],
  identifierCandidate: IdentifierCandidate
): boolean {
  if (priorTerminals.length == 0) {
    return false;
  }
  const priorTerminal = priorTerminals[priorTerminals.length - 1];
  return validateCompositeRules(
    identifierCandidate,
    columnRuleAnalyzers,
    priorTerminal
  );
}
