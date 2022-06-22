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
import debounce from "lodash/debounce";
import ApiUtils from "utils/apiUtils/apiUtils";

const getItems = (monaco, sqlContextGetter) => {
  if (typeof sqlContextGetter !== "function") {
    throw new Error(
      "sqlContextGetter must be specified and must be a function"
    );
  }

  const CompletionItemKind = monaco.languages.CompletionItemKind;
  const typeMap = {
    Repositary: CompletionItemKind.Class,
    DATE: CompletionItemKind.Class,
    DATETIME: CompletionItemKind.Class,
    DATETIME_INTERVAL: CompletionItemKind.Class,
    TIME: CompletionItemKind.Class,
    TIME_WITH_LOCAL_TIME_ZONE: CompletionItemKind.Class,
    TIMESTAMP: CompletionItemKind.Class,
    TIMESTAMP_WITH_LOCAL_TIME_ZONE: CompletionItemKind.Class,
    INTERVAL_YEAR: CompletionItemKind.Class,
    INTERVAL_YEAR_MONTH: CompletionItemKind.Class,
    INTERVAL_MONTH: CompletionItemKind.Class,
    INTERVAL_DAY: CompletionItemKind.Class,
    INTERVAL_DAY_HOUR: CompletionItemKind.Class,
    INTERVAL_DAY_MINUTE: CompletionItemKind.Class,
    INTERVAL_DAY_SECOND: CompletionItemKind.Class,
    INTERVAL_DAY_TIME: CompletionItemKind.Class,
    INTERVAL_HOUR: CompletionItemKind.Class,
    INTERVAL_HOUR_MINUTE: CompletionItemKind.Class,
    INTERVAL_HOUR_SECOND: CompletionItemKind.Class,
    INTERVAL_MINUTE: CompletionItemKind.Class,
    INTERVAL_MINUTE_SECOND: CompletionItemKind.Class,
    INTERVAL_SECOND: CompletionItemKind.Class,
    Schema: CompletionItemKind.File,
    Column: CompletionItemKind.Field,
    COLUMN_LIST: CompletionItemKind.Field,
    Catalog: CompletionItemKind.Folder,
    File: CompletionItemKind.File,
    Folder: CompletionItemKind.Folder,
    Function: CompletionItemKind.Function,
    GEOMETRY: CompletionItemKind.Issue,
    GEO: CompletionItemKind.Issue,
    Keyword: CompletionItemKind.Keyword,
    EXACT_NUMERIC: CompletionItemKind.Keyword,
    MULTISET: CompletionItemKind.Keyword,
    MAP: CompletionItemKind.Keyword,
    CURSOR: CompletionItemKind.Keyword,
    BINARY: CompletionItemKind.Method,
    VARBINARY: CompletionItemKind.Method,
    DISTINCT: CompletionItemKind.Method,
    BOOLEAN: CompletionItemKind.Operator,
    NULL: CompletionItemKind.Interface,
    STRUCTURED: CompletionItemKind.Interface,
    ROW: CompletionItemKind.Interface,
    STRING: CompletionItemKind.Text,
    CHARACTER: CompletionItemKind.Text,
    CHAR: CompletionItemKind.Text,
    VARCHAR: CompletionItemKind.Text,
    SYMBOL: CompletionItemKind.Text,
    OTHER: CompletionItemKind.Text,
    INTEGER: CompletionItemKind.Unit,
    APPROXIMATE_NUMERIC: CompletionItemKind.Unit,
    NUMERIC: CompletionItemKind.Unit,
    TINYINT: CompletionItemKind.Unit,
    SMALLINT: CompletionItemKind.Unit,
    BIGINT: CompletionItemKind.Unit,
    DECIMAL: CompletionItemKind.References,
    FLOAT: CompletionItemKind.References,
    REAL: CompletionItemKind.Unit,
    DOUBLE: CompletionItemKind.Unit,
    DYNAMIC_STAR: CompletionItemKind.References,
    ARRAY: CompletionItemKind.Variable,
    Table: CompletionItemKind.Variable,
    ANY: CompletionItemKind.Variable,
    CatalogEntry: CompletionItemKind.Interface,
    Space: CompletionItemKind.Module,
    Home: CompletionItemKind.Color,
    "Virtual Dataset": CompletionItemKind.Constructor,
    "Physical Dataset": CompletionItemKind.Snippet,
  };

  return (document, position) => {
    const delimiter = "\n";
    const content = document.getLinesContent();

    let pos = position.column - 1; // -1 to convert to zero-base index
    for (let i = 0; i < position.lineNumber - 1; i++) {
      pos += content[i].length + delimiter.length;
    }

    const request = {
      query: content.join(delimiter),
      cursor: pos,
      context: sqlContextGetter(),
    };

    return ApiUtils.fetch(
      "sql/autocomplete",
      {
        method: "POST",
        body: JSON.stringify(request),
      },
      2
    )
      .then((response) => {
        const contentType = response.headers.get("content-type");

        if (contentType && contentType.indexOf("application/json") !== -1) {
          return response.json().then(({ completionItems }) => {
            return completionItems.map(
              ({ label, detail, kind, data, insertText }, index) => {
                let sortText;
                let itemDetails;
                const ALPHABET_COUNT = 26;

                // Set order of items to be displayed in suggestion list
                if (index < ALPHABET_COUNT) {
                  sortText = String.fromCharCode("a".charCodeAt() + index);
                } else if (index < ALPHABET_COUNT * 2) {
                  sortText =
                    "z" +
                    String.fromCharCode(
                      "a".charCodeAt() + (index - ALPHABET_COUNT)
                    );
                } else {
                  sortText =
                    "zz" +
                    String.fromCharCode(
                      "a".charCodeAt() + (index - ALPHABET_COUNT * 2)
                    );
                }

                // Changing the lable and description for certain types
                switch (kind) {
                  case "Function":
                    label = insertText;
                    itemDetails = detail;
                    break;
                  case "Column":
                    if (data.tableAlias && data.column && data.column.type) {
                      itemDetails = `column (${data.column.type}) in ${data.tableAlias}`;
                    }
                    break;
                  default:
                    break;
                }

                // Change kind if object has type or column.type
                if (data && data.type) {
                  kind = data.type;
                } else if (data && data.column && data.column.type) {
                  kind = data.column.type;
                }

                return {
                  label,
                  kind: typeMap[kind],
                  sortText,
                  insertText,
                  detail: itemDetails,
                };
              }
            );
          });
        }
        return [];
      })
      .catch((response) => {
        if (response.status === 500) {
          return response.json().then((error) => {
            console.error(error);
            return null;
          });
        } else if (response.messeage) {
          console.error(response.message);
        }
      });
  };
};

export const SQLAutoCompleteProvider = (monaco, sqlContextGetter) => ({
  provideCompletionItems: debounce(getItems(monaco, sqlContextGetter), 200, {
    leading: true,
    maxWait: 200,
  }),
});
