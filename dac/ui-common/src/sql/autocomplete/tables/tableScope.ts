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
/* eslint-disable */
import type { CommonTokenStream } from "antlr4ts/CommonTokenStream";
import type { Interval } from "antlr4ts/misc/Interval";
import type { ParseTree } from "antlr4ts/tree/ParseTree";
import type { RuleNode } from "antlr4ts/tree/RuleNode";
import { RuleContext, Token } from "antlr4ts";

import {
  CompoundIdentifierContext,
  ExplicitTableContext,
  ParenthesizedExpressionContext,
  LiveEditParser as Parser,
  QueryContext,
  QueryOrExprContext,
  SelectExpressionContext,
  SqlSelectContext,
  TableRef3Context,
} from "../../../../target/generated-sources/antlr/LiveEditParser";
import { LiveEditParserVisitor } from "../../../../target/generated-sources/antlr/LiveEditParserVisitor";
import type { DirectRuleAnalyzer } from "../identifiers/analyzers/analyzersCommon";
import { tableRuleAnalyzers } from "../identifiers/analyzers/tableAnalyzer";
import type { IColumnFetcher } from "../apiClient/columnFetcher";
import {
  type CompoundIdentifierPart,
  getCompoundIdentifierParts,
  getUnquotedSimpleIdentifiers,
  IdentifierInfo,
} from "../identifiers/identifierUtils";
import { getPriorToken, getText } from "../parsing/tokenUtils";
import { CursorInfo } from "../parsing/cursorInfo";
import { ContainerType } from "../types/ContainerType";
import type {
  Column,
  DerivedTable,
  ExpressionColumn,
  Table,
} from "../types/Table";
import { getColumnName, getTableName } from "./tableUtils";

/** Index of rules that are within the table's scope */
type TableScope = number[];

/** Map of rules that parse a table to other rules that are within the table's scope */
type ScopeRestraints = {
  [scopedTableRuleIndex: number]: TableScope;
};

/**
 * Map of rules that parse a query containing a table reference to a map of restraints limiting the scope of referenced
 * tables. If a subrule is not within the ScopeRestraints, then its scope extends to the full rule.
 */
type TableScopeRestraints = {
  [tableRuleIndex: number]: ScopeRestraints;
};

const stopRules: number[] = [
  Parser.RULE_join,
  Parser.RULE_parenthesizedExpression,
  Parser.RULE_leafQuery,
  Parser.RULE_leafQueryOrExpr,
  Parser.RULE_sqlStmt,
  Parser.RULE_sqlQueryOrDml,
];

const tableScopeRestraints: TableScopeRestraints = {
  [Parser.RULE_sqlUpdateTable]: {
    // Only the where clause can use the from clause tables
    [Parser.RULE_fromClause]: [Parser.RULE_whereOpt],
  },
};

type ColumnIdentifier = { tableName?: string } & (
  | { type: "text"; column: string }
  | { type: "star" }
);

export type Projection = {
  name: string;
  column: Column;
};

export type QueryPlan = {
  tables?: Table[];
  projections?: Projection[];
};

export class QueryVisitor implements LiveEditParserVisitor<Promise<QueryPlan>> {
  private readonly cursorInfo: CursorInfo;
  private readonly identifierInfo: IdentifierInfo;
  private readonly commonTokenStream: CommonTokenStream;
  private readonly columnFetcher: IColumnFetcher;
  private readonly queryContext: string[];

  /** List of rule indexes from the from context boundary rule to the input rule context (inclusive) */
  private fromContextBoundaryRuleList: number[] = [];
  private withTablesInScope: DerivedTable[] = [];

  public constructor(
    cursorInfo: CursorInfo,
    identifierInfo: IdentifierInfo,
    commonTokenStream: CommonTokenStream,
    columnFetcher: IColumnFetcher,
    queryContext: string[],
  ) {
    this.cursorInfo = cursorInfo;
    this.identifierInfo = identifierInfo;
    this.commonTokenStream = commonTokenStream;
    this.columnFetcher = columnFetcher;
    this.queryContext = queryContext;
  }

  async getTablesInScope(): Promise<QueryPlan> {
    const startRuleContext = this.identifierInfo.terminal // undefined if cursor can start new identifier
      ? this.identifierInfo.terminal._parent!.ruleContext
      : this.cursorInfo.priorTerminals.slice(-1)[0]._parent!.ruleContext;

    // Traverse up to the highest rule that contains tables whose scope extends to this rule context
    let fromContextBoundary: RuleContext | undefined = startRuleContext;
    while (fromContextBoundary) {
      const ruleIndex = fromContextBoundary.ruleIndex;
      this.fromContextBoundaryRuleList.unshift(ruleIndex);
      if (stopRules.includes(ruleIndex)) {
        break;
      }
      fromContextBoundary = fromContextBoundary.parent;
    }
    if (!fromContextBoundary) {
      return Promise.resolve({});
    }

    // Pre-compute with tables that affect the query plan
    this.withTablesInScope.push(
      ...(await this.collectWithTables(fromContextBoundary)),
    );

    // Now visit descendents to find all tables in scope
    return fromContextBoundary.accept(this);
  }

  async visitChildren(node: RuleNode): Promise<QueryPlan> {
    const queryPlan: QueryPlan = {
      tables: [],
      projections: [],
    };
    const ruleIndex = node.ruleContext.ruleIndex;
    if (ruleIndex == Parser.RULE_compoundIdentifier) {
      // Any compound identifier we reach must be a table name, otherwise we would not have visited this child
      const path = getTablePath(node.ruleContext as CompoundIdentifierContext);
      if (!path) {
        return Promise.resolve({});
      }
      const fetchedColumns = await this.columnFetcher.getColumns(
        path,
        this.queryContext,
      );
      return {
        tables: [
          {
            derived: false,
            path,
            columns: fetchedColumns.map((column) => ({
              ...column,
              origin: "table",
            })),
            type: ContainerType.DIRECT, // Hack: getColumns API should return table container type
          },
        ],
      };
    }

    for (let i = 0; i < node.childCount; i++) {
      const child = node.getChild(i);
      const childRuleContext: ParseTree["payload"] = child.payload;
      if (!(childRuleContext instanceof RuleContext)) {
        // Reached a token
        continue;
      }

      let priorToken: Token | undefined = getPriorToken(
        this.commonTokenStream,
        child.sourceInterval.a,
      );

      const analyzer: DirectRuleAnalyzer = tableRuleAnalyzers[ruleIndex];
      const shouldVisitChild =
        (analyzer == "*" ||
          analyzer.some(
            (childRule) =>
              childRule.ruleIndex == childRuleContext.ruleIndex &&
              childRule.applies(priorToken, node.ruleContext),
          )) &&
        (!tableScopeRestraints[ruleIndex] ||
          !tableScopeRestraints[ruleIndex][childRuleContext.ruleIndex] ||
          tableScopeRestraints[ruleIndex][childRuleContext.ruleIndex].some(
            (restraintRuleIndex) =>
              hasRuleTransition(
                this.fromContextBoundaryRuleList,
                ruleIndex,
                restraintRuleIndex,
              ),
          ));

      if (shouldVisitChild) {
        const childQueryPlan = await child.accept(this);
        if (childRuleContext instanceof ParenthesizedExpressionContext) {
          const derivedTable: DerivedTable = createDerivedTable(childQueryPlan);
          queryPlan.tables!.push(derivedTable);
        } else {
          queryPlan.tables!.push(...(childQueryPlan.tables || []));
          queryPlan.projections!.push(...(childQueryPlan.projections || []));
        }
      }
    }

    return Promise.resolve(queryPlan);
  }

  async collectWithTables(
    fromContextBoundary: RuleContext,
  ): Promise<DerivedTable[]> {
    const withTables: DerivedTable[] = [];
    let ruleContext: RuleContext | undefined = fromContextBoundary;
    while (ruleContext) {
      if (
        ruleContext instanceof QueryOrExprContext ||
        ruleContext instanceof QueryContext
      ) {
        const withList = ruleContext.withList();
        for (const withItem of withList?.withItem() || []) {
          const alias = getText(withItem.simpleIdentifier()).text;
          const simpleIdentifierList = withItem
            .parenthesizedSimpleIdentifierList()
            ?.simpleIdentifierCommaList();
          const columnAliases =
            simpleIdentifierList &&
            getUnquotedSimpleIdentifiers(simpleIdentifierList);
          const childQueryPlan = await withItem
            .parenthesizedExpression()
            .accept(this);
          const derivedTable: DerivedTable = {
            ...createDerivedTable(childQueryPlan),
            alias,
            columnAliases,
          };
          // Only add it if it doesn't shadow some lower-scope with table
          if (
            !withTables.some(
              (withTable: DerivedTable) =>
                withTable.alias == derivedTable.alias,
            )
          ) {
            withTables.push(derivedTable);
          }
        }
      }
      ruleContext = ruleContext._parent;
    }
    return withTables;
  }

  async visitSqlSelect(ctx: SqlSelectContext): Promise<QueryPlan> {
    const projections: Projection[] = [];
    const queryPlan = await this.visitChildren(ctx);
    const selectList = ctx.selectList();
    if (!selectList) {
      return queryPlan;
    }

    const tablesInScope = queryPlan.tables || [];
    const usedColumnNames: Set<string> = new Set([]);
    for (const selectItem of selectList.selectItem()) {
      const selectExpression = selectItem.selectExpression();
      const tokenRange: Interval = selectExpression.sourceInterval;
      const compoundIdentifier = getCompoundIdentifier(selectExpression);
      const alias = selectItem.simpleIdentifier()?.text;
      if (
        tokenRange.length >= 1 &&
        !!compoundIdentifier &&
        tokenRange.equals(compoundIdentifier.sourceInterval) &&
        compoundIdentifier.LBRACKET().length == 0
      ) {
        // Table column
        const colIdentifier = parseSelectItemIdentifier(compoundIdentifier);
        if (!colIdentifier) {
          continue; // Invalid reference
        }
        if (colIdentifier.type == "star") {
          const tablesToExpand: Table[] = [];
          const tableName: string | undefined = colIdentifier.tableName;
          const tableInScope: Table | undefined = tablesInScope.find(
            (table) => !tableName || referencesTable(tableName, table),
          );
          if (tableInScope) {
            tablesToExpand.push(tableInScope);
          }

          for (const tableToExpand of tablesToExpand) {
            for (const column of tableToExpand.columns) {
              const projection = createColumnProjection(
                column,
                true,
                usedColumnNames,
              );
              addProjection(projection, projections, usedColumnNames);
            }
          }
        } else {
          const columnName = alias ? alias : colIdentifier.column;
          const column = getColumn(
            tablesInScope,
            columnName,
            colIdentifier.tableName,
          );
          if (column) {
            const projection = createColumnProjection(
              column,
              false,
              usedColumnNames,
            );
            addProjection(projection, projections, usedColumnNames);
          }
        }
      } else if (selectExpression.STAR()) {
        for (const tableToExpand of tablesInScope) {
          for (const column of tableToExpand.columns) {
            const projection = createColumnProjection(
              column,
              true,
              usedColumnNames,
            );
            addProjection(projection, projections, usedColumnNames);
          }
        }
      } else {
        // Expression column
        const projection = createExpressionProjection(alias, usedColumnNames);
        addProjection(projection, projections, usedColumnNames);
      }
    }

    queryPlan.projections = projections;
    return queryPlan;
  }

  async visitTableRef3(ctx: TableRef3Context): Promise<QueryPlan> {
    const queryPlan = await this.visitChildren(ctx);
    // It should have a single child (either cmpd id or derived table), otherwise something's wrong
    if (queryPlan.tables?.length === 1) {
      let table = queryPlan.tables[0];
      const matchingWithTable =
        (!table.derived &&
          table.path.length == 1 &&
          this.withTablesInScope.find((withTable: DerivedTable) =>
            referencesTable(getTableName(table) || "", withTable),
          )) ||
        undefined;
      const simpleIdentifier = ctx.simpleIdentifier();
      const alias = simpleIdentifier && getText(simpleIdentifier).text;
      const simpleIdentifierList = ctx
        .parenthesizedSimpleIdentifierList()
        ?.simpleIdentifierCommaList();
      const columnAliases =
        simpleIdentifierList &&
        getUnquotedSimpleIdentifiers(simpleIdentifierList);
      if (matchingWithTable) {
        const replacementTable = {
          ...matchingWithTable,
        };
        if (alias) {
          replacementTable.alias = alias;
        }
        if (columnAliases) {
          replacementTable.columnAliases = columnAliases;
        }
        queryPlan.tables = [replacementTable];
      } else {
        table.alias = alias;
        table.columnAliases = columnAliases;
      }
    }
    return queryPlan;
  }

  // All columns implicitly projected
  async visitExplicitTable(ctx: ExplicitTableContext): Promise<QueryPlan> {
    const queryPlan = await this.visitChildren(ctx);
    const projections: Projection[] = [];
    const table = queryPlan.tables?.[0];
    if (table) {
      for (const column of table.columns) {
        const projection: Projection = { name: column.name, column };
        addProjection(projection, projections, new Set([]));
      }
    }
    queryPlan.projections = projections;
    return queryPlan;
  }

  visit(): Promise<QueryPlan> {
    throw new Error("Method not implemented.");
  }

  visitTerminal(): Promise<QueryPlan> {
    throw new Error("Method not implemented.");
  }

  visitErrorNode(): Promise<QueryPlan> {
    throw new Error("Method not implemented.");
  }
}

function createDerivedTable(childQueryPlan: QueryPlan): DerivedTable {
  return {
    derived: true,
    columns:
      childQueryPlan.projections?.map((projection) => ({
        ...projection.column,
        name: projection.name || projection.column.name,
      })) || [],
  };
}

function hasRuleTransition(
  fromContextBoundaryRuleList: number[],
  fromRuleIndex: number,
  toRuleIndex: number,
): boolean {
  return fromContextBoundaryRuleList.some(
    (ruleIndex, i) =>
      ruleIndex > 0 &&
      fromContextBoundaryRuleList[i] == toRuleIndex &&
      fromContextBoundaryRuleList[i - 1] == fromRuleIndex,
  );
}

function createColumnProjection(
  column: Column,
  inStar: boolean,
  usedColumnNames: Set<string>,
): Projection | undefined {
  const name = inferProjectionName(column.name, inStar, usedColumnNames);
  return name ? { name, column } : undefined;
}

function createExpressionProjection(
  expressionAlias: string | undefined,
  usedColumnNames: Set<string>,
): Projection | undefined {
  const name = inferProjectionName(expressionAlias, false, usedColumnNames);
  const column: ExpressionColumn = {
    name: expressionAlias || "",
    origin: "expression",
  };
  return name ? { name, column } : undefined;
}

function inferProjectionName(
  columnName: string | undefined,
  inStar: boolean,
  usedColumnNames: Set<string>,
): string | undefined {
  const name = columnName || "EXPR$" + usedColumnNames.size;
  if (!usedColumnNames.has(name)) {
    return name; // It's unique - it will be directly used
  }
  if (!inStar) {
    return undefined; // Column will get silently not projected if not star expension and name is not unique
  }
  let suffixNum = 0;
  while (true) {
    // Expanding a star: if projection column name already used, Calcite appends an incrementing index to make it unique
    const uniqueifiedName = `${name}${suffixNum}`;
    if (!usedColumnNames.has(uniqueifiedName)) {
      return uniqueifiedName;
    }
    suffixNum++;
  }
}

function addProjection(
  projection: Projection | undefined,
  /** out param */ projections: Projection[],
  /** out param */ usedColumnNames: Set<string>,
) {
  if (projection) {
    projections.push(projection);
    usedColumnNames.add(projection.name);
  }
}

function getColumn(
  tables: Table[],
  columnName: string,
  tableName: string | undefined,
): Column | undefined {
  const candidateTables = tableName
    ? tables.filter((table) => referencesTable(tableName, table))
    : tables;
  for (const table of candidateTables) {
    for (const column of table.columns) {
      const tableColumnName = getColumnName(column, table)!;
      if (caseInsensitiveEquals(columnName, tableColumnName)) {
        return column;
      }
    }
  }
  return undefined;
}

function getTablePath(
  ruleContext: CompoundIdentifierContext,
): string[] | undefined {
  if (
    ruleContext.LBRACKET().length > 0 ||
    ruleContext.STAR().length > 0 ||
    ruleContext.simpleIdentifier().length == 0
  ) {
    // Not valid table name
    return undefined;
  }
  return ruleContext
    .simpleIdentifier()
    .map((simpleIdentifier) => getText(simpleIdentifier).text);
}

function parseSelectItemIdentifier(
  compoundIdentifier: CompoundIdentifierContext,
): ColumnIdentifier | undefined {
  const identifierParts: CompoundIdentifierPart[] =
    getCompoundIdentifierParts(compoundIdentifier);
  if (identifierParts.length == 0 || identifierParts.length > 2) {
    return undefined;
  }
  const lastPart =
    identifierParts.length == 2 ? identifierParts[1] : identifierParts[0];
  const firstPart =
    identifierParts.length == 2 ? identifierParts[0] : undefined;
  if (
    // star in non-final segment disallowed
    firstPart?.type == "star"
  ) {
    return undefined; // It's invalid, their query will fail
  }
  const tableName = firstPart?.text;
  if (lastPart.type == "text") {
    return { tableName, type: "text", column: lastPart.text };
  } else {
    return { tableName, type: "star" };
  }
}

function getCompoundIdentifier(
  selectExpression: SelectExpressionContext,
): CompoundIdentifierContext | undefined {
  return selectExpression
    .expression()
    ?.expression2()
    ?.expression2b()[0]
    ?.expression3()
    ?.atomicRowExpression()
    ?.compoundIdentifier();
}

function caseInsensitiveEquals(str1: string, str2: string): boolean {
  return !str1.localeCompare(str2, "en", { sensitivity: "base" });
}

function referencesTable(tableName: string, table: Table): boolean {
  return caseInsensitiveEquals(getTableName(table) || "", tableName);
}
