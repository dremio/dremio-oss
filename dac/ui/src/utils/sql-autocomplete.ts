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

export const getAutoCompleteTypeMap = (CompletionItemKind: any) => ({
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
});

export const getAutoCompleteSortTextValue = (index: number) => {
  const ALPHABET_COUNT = 26;

  // Set order of items to be displayed in suggestion list
  if (index < ALPHABET_COUNT) {
    return String.fromCharCode("a".charCodeAt(0) + index);
  } else if (index < ALPHABET_COUNT * 2) {
    return (
      "z" + String.fromCharCode("a".charCodeAt(0) + (index - ALPHABET_COUNT))
    );
  } else {
    return (
      "zz" +
      String.fromCharCode("a".charCodeAt(0) + (index - ALPHABET_COUNT * 2))
    );
  }
};

export const getAutoCompleteKind = (typeMap: any, kind: string, data: any) => {
  // Change kind if object has type or column.type
  if (data && data.type) {
    return typeMap[data.type];
  } else if (data && data.column && data.column.type) {
    return typeMap[data.column.type];
  } else return typeMap[kind];
};

export const constructTransformValues = (
  content: any,
  position: any,
  activeWord: string
) => {
  // find if entity inside a solo or paired double quote
  const curLine = content?.[position.lineNumber - 1] ?? "";
  const leftOfCursor = curLine.substring(0, position.column - 1);
  const leftDoubleQuoteCount = leftOfCursor.split('"').length - 1;
  const rightOfCursor = curLine.substring(position.column - 1);
  const rightDoubleQuoteCount = rightOfCursor.split('"').length - 1;

  const insidePairedDoubleQuote =
    leftDoubleQuoteCount % 2 !== 0 && rightDoubleQuoteCount % 2 !== 0;
  const insideSoloDoubleQuote =
    leftDoubleQuoteCount % 2 !== 0 && rightDoubleQuoteCount % 2 === 0;

  // find if entity is a subpart, and replace only sub section
  const entityLeftOfCursor = leftOfCursor.substring(
    leftOfCursor.lastIndexOf('"') + 1
  );
  const replaceWithSubEntity =
    entityLeftOfCursor !== activeWord &&
    (insidePairedDoubleQuote || insideSoloDoubleQuote);

  return {
    insidePairedDoubleQuote,
    insideSoloDoubleQuote,
    replaceWithSubEntity,
    entityLeftOfCursor,
  };
};
