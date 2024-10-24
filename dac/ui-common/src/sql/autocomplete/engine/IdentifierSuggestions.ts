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

import { CommonTokenStream } from "antlr4ts";

import {
  type AnalyzedIdentifier,
  IdentifierType,
} from "../identifiers/identifierAnalyzer";
import type { CatalogContainer } from "../apiClient/autocompleteApi";
import type { CursorInfo } from "../parsing/cursorInfo";
import {
  type IdentifierInfo,
  getIdentifierInfo,
  needsQuoting,
} from "../identifiers/identifierUtils";
import type { IColumnFetcher } from "../apiClient/columnFetcher";
import { assertNever } from "../../../utilities/typeUtils";
import { QueryVisitor } from "../tables/tableScope";
import { SimpleDataType } from "../../../sonar/catalog/SimpleDataType.type";
import type { IContainerFetcher } from "../apiClient/containerFetcher";
import { ContainerType as Type } from "../types/ContainerType";
import { getColumnName, getTableName } from "../tables/tableUtils";
import type { Column, Table } from "../types/Table";
import { timedAsync } from "../../../utilities/timed";

export type ContainerSuggestion = {
  name: string;
  type: Type;
  insertText?: string;
};
export type ColumnSuggestion = {
  name: string;
  table?: string; // undefined if unnamed derived table
  type: SimpleDataType;
  insertText?: string;
};

export type GetIdentifierSuggestionsResult =
  | {
      type: "containers";
      containers: ContainerSuggestion[];
    }
  | {
      type: "columns";
      columns: ColumnSuggestion[];
      tables: ContainerSuggestion[];
    };

export class IdentifierSuggestions {
  private readonly analyzedIdentifier: AnalyzedIdentifier;
  private readonly cursorInfo: CursorInfo;
  private readonly tokenStream: CommonTokenStream;
  private readonly containerFetcher: IContainerFetcher;
  private readonly columnFetcher: IColumnFetcher;
  private readonly queryContext: string[];
  private readonly identifierInfo: IdentifierInfo;

  constructor(
    analyzedIdentifier: AnalyzedIdentifier,
    cursorInfo: CursorInfo,
    tokenStream: CommonTokenStream,
    containerFetcher: IContainerFetcher,
    columnFetcher: IColumnFetcher,
    queryContext: string[],
  ) {
    this.analyzedIdentifier = analyzedIdentifier;
    this.cursorInfo = cursorInfo;
    this.tokenStream = tokenStream;
    this.containerFetcher = containerFetcher;
    this.columnFetcher = columnFetcher;
    this.queryContext = queryContext;
    this.identifierInfo = getIdentifierInfo(this.cursorInfo);
  }

  @timedAsync("IdentifierSuggestions.get")
  async get(): Promise<GetIdentifierSuggestionsResult | undefined> {
    if (!this.identifierInfo.completable) {
      return undefined;
    }

    switch (this.analyzedIdentifier.type) {
      case IdentifierType.GENERIC_CONTAINER:
        return this.containersResult(
          this.getContainerSuggestions([
            Type.SOURCE,
            Type.SPACE,
            Type.HOME,
            Type.FOLDER,
          ]),
        );
      case IdentifierType.FOLDER:
        return this.containersResult(
          this.getContainerSuggestions([
            Type.SOURCE,
            Type.SPACE,
            Type.HOME,
            Type.FOLDER,
          ]),
        );
      case IdentifierType.SOURCE:
        return this.containersResult(
          this.getContainerSuggestions([Type.SOURCE]),
        );
      case IdentifierType.SPACE:
        return this.containersResult(
          this.getContainerSuggestions([Type.SPACE, Type.HOME]),
        );
      case IdentifierType.TABLE:
        return this.containersResult(
          this.getContainerSuggestions([
            Type.SOURCE,
            Type.SPACE,
            Type.HOME,
            Type.FOLDER,
            Type.VIRTUAL,
            Type.PROMOTED,
            Type.DIRECT,
          ]),
        );
      case IdentifierType.UDF:
        return this.containersResult(
          this.getContainerSuggestions([
            Type.SPACE,
            Type.HOME,
            Type.FOLDER,
            Type.FUNCTION,
          ]),
        );
      case IdentifierType.COLUMN:
        return this.columnsResult(
          this.getColumnSuggestions(this.analyzedIdentifier.compoundAllowed),
        );
      default:
        return assertNever(this.analyzedIdentifier);
    }
  }

  private getContainerSuggestions(
    allowedTypes: Type[],
  ): Promise<CatalogContainer[]> {
    if (!this.identifierInfo.completable) {
      return Promise.resolve([]);
    }
    return this.containerFetcher.getContainers(
      this.identifierInfo.parentPath,
      this.identifierInfo.prefix,
      this.queryContext,
      new Set(allowedTypes),
    );
  }

  private async getColumnSuggestions(compoundAllowed: boolean): Promise<{
    columns: [Column[], Table][];
    tables: Table[];
  }> {
    let columns: [Column[], Table][] = [];
    let tables: Table[] = [];
    if (
      !this.identifierInfo.completable ||
      this.identifierInfo.parentPath.length > 1
    ) {
      // No possible columns if the identifier could not be processed or cannot reference a column name (max two parts)
      return Promise.resolve({ columns, tables });
    }
    const queryVisitor = new QueryVisitor(
      this.cursorInfo,
      this.identifierInfo,
      this.tokenStream,
      this.columnFetcher,
      this.queryContext,
    );
    const queryPlan = await queryVisitor.getTablesInScope();
    if (!queryPlan.tables) {
      return { columns, tables };
    }
    const relationName: string | undefined = this.identifierInfo.parentPath[0];
    const possibleTables = !relationName
      ? queryPlan.tables
      : queryPlan.tables.filter(
          (table: Table) =>
            relationName.toLocaleLowerCase() ===
            getTableName(table)?.toLocaleLowerCase(),
        );
    columns = possibleTables
      .map((table: Table): [Column[], Table] => [
        table.columns.filter((column) =>
          getColumnName(column, table)
            ?.toLocaleLowerCase()
            .startsWith(this.identifierInfo.prefix.toLocaleLowerCase()),
        ),
        table,
      ])
      .filter(([columns]) => columns.length > 0);
    if (compoundAllowed && !relationName) {
      // Since we're not already qualified with a table name, we should suggest table names/aliases if possible here
      // compoundAllowed == false in some cases e.g. SET MASKING POLICY funcName (^) <-- non-qualified column names only
      tables = queryPlan.tables.filter((table: Table) =>
        getTableName(table)
          ?.toLocaleLowerCase()
          .startsWith(this.identifierInfo.prefix.toLocaleLowerCase()),
      );
    }
    return { columns, tables };
  }

  private async columnsResult(
    filteredColumnsWithTable: Promise<{
      columns: [Column[], Table][];
      tables: Table[];
    }>,
  ): Promise<GetIdentifierSuggestionsResult> {
    const columnsWithTable = await filteredColumnsWithTable;
    return {
      type: "columns",
      columns: columnsWithTable.columns.flatMap(([columns, table]) =>
        columns.map((column) => {
          const columnName = getColumnName(column, table) || "";
          const tableName = getTableName(table);
          const base = {
            name: columnName,
            table: tableName,
            type:
              column.origin === "table" ? column.type : SimpleDataType.OTHER,
          };
          const uniqueifyColumnName = (name: string) => {
            const isUniqueColumnName = !columnsWithTable.columns.some(
              ([searchColumns, searchTable]) =>
                searchTable !== table &&
                searchColumns.some(
                  (searchColumn) =>
                    getColumnName(searchColumn, searchTable) === name,
                ),
            );
            return !isUniqueColumnName && tableName
              ? `${tableName}.${name}` // qualify the column to avoid ambiguity
              : name;
          };
          const insertText = this.getInsertText(base, uniqueifyColumnName);
          return {
            ...base,
            ...(insertText ? { insertText } : {}),
          };
        }),
      ),
      tables: columnsWithTable.tables
        .filter((table: Table) => !table.derived || !!table.alias)
        .map((table: Table) => {
          const base = {
            name: getTableName(table) || "",
            type: !table.derived ? table.type : Type.DIRECT, // TODO what icon to show for derived tables?
          };
          const insertText = this.getInsertText(base);
          return {
            ...base,
            ...(insertText ? { insertText } : {}),
          };
        }),
    };
  }

  private async containersResult(
    containers: Promise<CatalogContainer[]>,
  ): Promise<GetIdentifierSuggestionsResult> {
    return {
      type: "containers",
      containers: (await containers).map((container) => {
        const insertText = this.getInsertText(container);
        return {
          ...container,
          ...(insertText ? { insertText } : {}),
        };
      }),
    };
  }

  private getInsertText = (
    entity: { name: string },
    ...additionalTransforms: ((name: string) => string)[]
  ): string | undefined => {
    const prefixQuoted = this.identifierInfo.prefixQuoted;
    const transforms: ((name: string) => string)[] = [];
    transforms.push((name) => name.replace(/"/g, '""'));
    if (!prefixQuoted && needsQuoting(entity.name)) {
      transforms.push((name) => `"${name}"`);
    }
    transforms.push(...additionalTransforms);
    let insertText = entity.name;
    for (const transform of transforms) {
      insertText = transform(insertText);
    }
    return insertText !== entity.name ? insertText : undefined;
  };
}
