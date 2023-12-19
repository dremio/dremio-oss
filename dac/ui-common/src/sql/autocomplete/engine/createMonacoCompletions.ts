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

import { SimpleDataType } from "../../../sonar/catalog/SimpleDataType.type";
import { assertNever } from "../../../utilities/typeUtils";
import type { SQLFunction } from "../types/SQLFunction";
import { ContainerType } from "../types/ContainerType";
import { KeywordSuggestion } from "../types/KeywordSuggestion";
import type {
  GetIdentifierSuggestionsResult,
  ContainerSuggestion,
  ColumnSuggestion,
} from "./IdentifierSuggestions";
import type { SuggestionInfo } from "./TokenSuggestions";

const MAX_SUGGESTIONS_EXP = 4; // 10^4 max suggestions

export function createMonacoCompletions(
  tokenSuggestions: SuggestionInfo,
  identifierSuggestions: GetIdentifierSuggestionsResult | undefined
): monaco.languages.CompletionItem[] {
  const completionItems: monaco.languages.CompletionItem[] = [];
  let itemNum = { num: 0 };

  if (identifierSuggestions) {
    completionItems.push(
      ...createCatalogCompletions(identifierSuggestions, itemNum)
    );
  }

  for (const func of tokenSuggestions.functions) {
    completionItems.push(createFunctionCompletion(func, itemNum));
  }

  const sortedKeywords = tokenSuggestions.keywords.sort((a, b) =>
    a.toString().localeCompare(b.toString())
  );
  for (const keyword of sortedKeywords) {
    completionItems.push(createKeywordCompletion(keyword, itemNum));
  }

  return completionItems;
}

function createCatalogCompletions(
  suggestions: GetIdentifierSuggestionsResult | undefined,
  itemNum: { num: number }
): monaco.languages.CompletionItem[] {
  if (!suggestions) {
    return [];
  }
  const addContainerCompletion = (container: ContainerSuggestion) => {
    completions.push(
      createContainerCompletion(
        container.name,
        container.type,
        itemNum,
        container.insertText
      )
    );
  };
  const addColumnCompletion = (column: ColumnSuggestion) => {
    const tableName = column.table ?? "Unnamed derived table";
    completions.push(
      createColumnCompletion(
        column.name,
        column.type,
        tableName,
        itemNum,
        column.insertText
      )
    );
  };
  const completions: monaco.languages.CompletionItem[] = [];
  switch (suggestions.type) {
    case "containers": {
      const sortedContainers = suggestions.containers.sort((a, b) =>
        a.name.localeCompare(b.name)
      );
      for (const container of sortedContainers) {
        addContainerCompletion(container);
      }
      break;
    }
    case "columns": {
      const sortedEntities = [
        ...suggestions.columns.map((column: ColumnSuggestion) => ({
          type: "column" as const,
          entity: column,
        })),
        ...suggestions.tables.map((table: ContainerSuggestion) => ({
          type: "table" as const,
          entity: table,
        })),
      ].sort((a, b) => a.entity.name.localeCompare(b.entity.name));
      for (const entity of sortedEntities) {
        switch (entity.type) {
          case "column":
            addColumnCompletion(entity.entity);
            break;
          case "table":
            addContainerCompletion(entity.entity);
            break;
          default:
            assertNever(entity);
        }
      }
      break;
    }
    default:
      assertNever(suggestions);
  }
  return completions;
}

function createFunctionCompletion(
  func: SQLFunction,
  itemNum: { num: number }
): monaco.languages.CompletionItem {
  const label = `${func.name}${func.label}`;
  const insertText: monaco.languages.SnippetString = {
    value: `${func.name}${func.snippet}`,
  };
  const completionItem = createMonacoCompletionItem(
    label,
    getCompletionItemKind("Function"),
    itemNum.num,
    func.description?.trim(),
    insertText
  );
  itemNum.num += 1;
  return completionItem;
}

function createContainerCompletion(
  containerName: string,
  type: ContainerType,
  itemNum: { num: number },
  insertText?: string
): monaco.languages.CompletionItem {
  const kind = getContainerCompletionItemKind(type);
  const completionItem = createMonacoCompletionItem(
    containerName,
    kind,
    itemNum.num,
    undefined,
    insertText
  );
  itemNum.num += 1;
  return completionItem;
}

function createColumnCompletion(
  columnName: string,
  dataType: SimpleDataType,
  tableName: string,
  itemNum: { num: number },
  insertText?: string
): monaco.languages.CompletionItem {
  const kind = getColumnCompletionItemKind(dataType);
  const detail = `column (${SimpleDataType[dataType]}) in ${tableName}`;
  const completionItem = createMonacoCompletionItem(
    columnName,
    kind,
    itemNum.num,
    detail,
    insertText
  );
  itemNum.num += 1;
  return completionItem;
}

function createKeywordCompletion(
  keyword: KeywordSuggestion,
  itemNum: { num: number }
): monaco.languages.CompletionItem {
  const completionItem = createMonacoCompletionItem(
    keyword.toString(),
    getCompletionItemKind("Keyword"),
    itemNum.num
  );
  itemNum.num += 1;
  return completionItem;
}

function createMonacoCompletionItem(
  label: string,
  kind: monaco.languages.CompletionItemKind,
  sortNum: number,
  detail?: string,
  insertText?: string | monaco.languages.SnippetString
): monaco.languages.CompletionItem {
  // Fixed-length string so "030" < "200"
  const sortText = sortNum.toString().padStart(MAX_SUGGESTIONS_EXP, "0");
  return {
    label,
    kind,
    sortText,
    detail,
    insertText,
  };
}

function getCompletionItemKind(
  key: keyof typeof monaco.languages.CompletionItemKind
): monaco.languages.CompletionItemKind {
  // Type-safe hack to work around not being able to import CompletionItemKind enum from monaco.d.ts
  // This will not compile if there are missing enum keys or mismatched values
  const completionItemKindMap: typeof monaco.languages.CompletionItemKind = {
    Text: 0,
    Method: 1,
    Function: 2,
    Constructor: 3,
    Field: 4,
    Variable: 5,
    Class: 6,
    Interface: 7,
    Module: 8,
    Property: 9,
    Unit: 10,
    Value: 11,
    Enum: 12,
    Keyword: 13,
    Snippet: 14,
    Color: 15,
    File: 16,
    Reference: 17,
    Folder: 18,
  };
  return completionItemKindMap[key];
}

function getContainerCompletionItemKind(
  type: ContainerType
): monaco.languages.CompletionItemKind {
  switch (type) {
    case ContainerType.FOLDER:
      return getCompletionItemKind("Folder");
    case ContainerType.HOME:
      return getCompletionItemKind("Color");
    case ContainerType.VIRTUAL:
      return getCompletionItemKind("Constructor");
    case ContainerType.PROMOTED:
    case ContainerType.DIRECT:
      return getCompletionItemKind("Snippet");
    case ContainerType.SOURCE:
      return getCompletionItemKind("Property");
    case ContainerType.SPACE:
      return getCompletionItemKind("Module");
    case ContainerType.FUNCTION:
      return getCompletionItemKind("Function");
    default:
      return assertNever(type);
  }
}

function getColumnCompletionItemKind(
  dataType: SimpleDataType
): monaco.languages.CompletionItemKind {
  // We hijack the monaco completionitemkind meaning based on our own data types
  // These only matter for the purpose of the custom icons we show, see SQLEditor.less
  switch (dataType) {
    case SimpleDataType.TEXT:
      return getCompletionItemKind("Text");
    case SimpleDataType.BINARY:
      return getCompletionItemKind("Method");
    case SimpleDataType.BOOLEAN:
      return getCompletionItemKind("Enum");
    case SimpleDataType.MAP:
      return getCompletionItemKind("Field");
    case SimpleDataType.GEO:
      return getCompletionItemKind("Keyword");
    case SimpleDataType.FLOAT:
    case SimpleDataType.DECIMAL:
      return getCompletionItemKind("Reference");
    case SimpleDataType.INTEGER:
      return getCompletionItemKind("Unit");
    case SimpleDataType.MIXED:
    case SimpleDataType.LIST:
      return getCompletionItemKind("Variable");
    case SimpleDataType.OTHER:
      return getCompletionItemKind("Value");
    case SimpleDataType.DATE:
    case SimpleDataType.TIME:
    case SimpleDataType.DATETIME:
      return getCompletionItemKind("Class");
    case SimpleDataType.STRUCT:
      return getCompletionItemKind("Interface");
    default:
      return assertNever(dataType);
  }
}
