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

import * as monaco from "monaco-editor";
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

// `range` is not optional but will default to the current position if omitted
export type CompletionItem = Omit<monaco.languages.CompletionItem, "range">;

export function createMonacoCompletions(
  tokenSuggestions: SuggestionInfo,
  identifierSuggestions: GetIdentifierSuggestionsResult | undefined,
): CompletionItem[] {
  const completionItems: CompletionItem[] = [];
  let itemNum = { num: 0 };

  if (identifierSuggestions) {
    completionItems.push(
      ...createCatalogCompletions(identifierSuggestions, itemNum),
    );
  }

  for (const func of tokenSuggestions.functions) {
    completionItems.push(createFunctionCompletion(func, itemNum));
  }

  const sortedKeywords = tokenSuggestions.keywords.sort((a, b) =>
    a.toString().localeCompare(b.toString()),
  );
  for (const keyword of sortedKeywords) {
    completionItems.push(createKeywordCompletion(keyword, itemNum));
  }

  return completionItems;
}

function createCatalogCompletions(
  suggestions: GetIdentifierSuggestionsResult | undefined,
  itemNum: { num: number },
): CompletionItem[] {
  if (!suggestions) {
    return [];
  }
  const addContainerCompletion = (container: ContainerSuggestion) => {
    completions.push(
      createContainerCompletion(
        container.name,
        container.type,
        itemNum,
        container.insertText,
      ),
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
        column.insertText,
      ),
    );
  };
  const completions: CompletionItem[] = [];
  switch (suggestions.type) {
    case "containers": {
      const sortedContainers = suggestions.containers.sort((a, b) =>
        a.name.localeCompare(b.name),
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
  itemNum: { num: number },
): CompletionItem {
  const label = `${func.name}${func.label}`;
  const insertText = `${func.name}${func.snippet}`;
  const completionItem = createMonacoCompletionItem(
    label,
    getCompletionItemKind("Function"),
    itemNum.num,
    func.description?.trim(),
    insertText,
    getInsertTextRules("InsertAsSnippet"),
  );
  itemNum.num += 1;
  return completionItem;
}

function createContainerCompletion(
  containerName: string,
  type: ContainerType,
  itemNum: { num: number },
  insertText?: string,
): CompletionItem {
  const kind = getContainerCompletionItemKind(type);
  const completionItem = createMonacoCompletionItem(
    containerName,
    kind,
    itemNum.num,
    undefined,
    insertText,
  );
  itemNum.num += 1;
  return completionItem;
}

function createColumnCompletion(
  columnName: string,
  dataType: SimpleDataType,
  tableName: string,
  itemNum: { num: number },
  insertText?: string,
): CompletionItem {
  const kind = getColumnCompletionItemKind(dataType);
  const detail = `column (${SimpleDataType[dataType]}) in ${tableName}`;
  const completionItem = createMonacoCompletionItem(
    columnName,
    kind,
    itemNum.num,
    detail,
    insertText,
  );
  itemNum.num += 1;
  return completionItem;
}

function createKeywordCompletion(
  keyword: KeywordSuggestion,
  itemNum: { num: number },
): CompletionItem {
  const completionItem = createMonacoCompletionItem(
    keyword.toString(),
    getCompletionItemKind("Keyword"),
    itemNum.num,
  );
  itemNum.num += 1;
  return completionItem;
}

function createMonacoCompletionItem(
  label: string,
  kind: monaco.languages.CompletionItemKind,
  sortNum: number,
  detail?: string,
  insertText?: string,
  insertTextRules?: monaco.languages.CompletionItemInsertTextRule,
): CompletionItem {
  // Fixed-length string so "030" < "200"
  const sortText = sortNum.toString().padStart(MAX_SUGGESTIONS_EXP, "0");
  return {
    label,
    kind,
    sortText,
    detail,
    insertText: insertText ?? label,
    insertTextRules,
  };
}

function getInsertTextRules(
  key: keyof typeof monaco.languages.CompletionItemInsertTextRule,
): monaco.languages.CompletionItemInsertTextRule {
  // Type-safe hack to avoid importing CompletionItemInsertTextRule enum from editor.api.d.ts
  enum CompletionItemInsertTextRule {
    None = 0,
    /**
     * Adjust whitespace/indentation of multiline insert texts to
     * match the current line indentation.
     */
    KeepWhitespace = 1,
    /**
     * `insertText` is a snippet.
     */
    InsertAsSnippet = 4,
  }

  return CompletionItemInsertTextRule[key];
}

function getCompletionItemKind(
  key: keyof typeof monaco.languages.CompletionItemKind,
): monaco.languages.CompletionItemKind {
  if (key === "Text") {
    throw new Error(
      "'Text' is reserved for Monaco's default suggestions and should not be used",
    );
  }

  // Type-safe hack to work around not being able to import CompletionItemKind enum from editor.api.d.ts
  // This will not compile if there are missing enum keys or mismatched values
  enum CompletionItemKind {
    Method = 0,
    Function = 1,
    Constructor = 2,
    Field = 3,
    Variable = 4,
    Class = 5,
    Struct = 6,
    Interface = 7,
    Module = 8,
    Property = 9,
    Event = 10, // unused
    Operator = 11, // unused
    Unit = 12,
    Value = 13,
    Constant = 14, // unused
    Enum = 15,
    EnumMember = 16, //unused
    Keyword = 17,
    Text = 18,
    Color = 19,
    File = 20, // unused
    Reference = 21,
    Customcolor = 22, // unused
    Folder = 23,
    TypeParameter = 24, // unused
    User = 25, // unused
    Issue = 26, // unused
    Snippet = 27,
  }

  return CompletionItemKind[key];
}

function getContainerCompletionItemKind(
  type: ContainerType,
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
  dataType: SimpleDataType,
): monaco.languages.CompletionItemKind {
  // We hijack the monaco completionitemkind meaning based on our own data types
  // These only matter for the purpose of the custom icons we show, see SQLEditor.module.less
  switch (dataType) {
    case SimpleDataType.TEXT:
      return getCompletionItemKind("Struct");
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
