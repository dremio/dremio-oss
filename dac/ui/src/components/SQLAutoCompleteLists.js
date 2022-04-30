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
export const SQLAutoCompleteItems = (monaco) => {
  return [
    {
      label: 'ABS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ABS',
      detail: 'ABS is meant to be used along with ALL'
    },
    {
      label: 'ALL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ALL',
      detail: 'Keyword'
    },
    {
      label: 'ALLOCATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ALLOCATE',
      detail: 'Keyword'
    },
    {
      label: 'ALLOW',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ALLOW',
      detail: 'Keyword'
    },
    {
      label: 'ALTER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ALTER',
      detail: 'Keyword'
    },
    {
      label: 'AND',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'AND',
      detail: 'Keyword'
    },
    {
      label: 'ANY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ANY',
      detail: 'Keyword'
    },
    {
      label: 'ARE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ARE',
      detail: 'Keyword'
    },
    {
      label: 'ARRAY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ARRAY',
      detail: 'Keyword'
    },
    {
      label: 'ARRAY_MAX_CARDINALITY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ARRAY_MAX_CARDINALITY',
      detail: 'Keyword'
    },
    {
      label: 'AS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'AS',
      detail: 'Keyword'
    },
    {
      label: 'ASENSITIVE’, ‘ASYMMETRIC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ASENSITIVE’, ‘ASYMMETRIC',
      detail: 'Keyword'
    },
    {
      label: 'AT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'AT',
      detail: 'Keyword'
    },
    {
      label: 'ATOMIC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ATOMIC',
      detail: 'Keyword'
    },
    {
      label: 'AUTHORIZATION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'AUTHORIZATION',
      detail: 'Keyword'
    },
    {
      label: 'AVG',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'AVG',
      detail: 'Keyword'
    },
    {
      label: 'BEGIN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BEGIN',
      detail: 'Keyword'
    },
    {
      label: 'BEGIN_FRAME',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BEGIN_FRAME',
      detail: 'Keyword'
    },
    {
      label: 'BEGIN_PARTITION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BEGIN_PARTITION',
      detail: 'Keyword'
    },
    {
      label: 'BETWEEN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BETWEEN',
      detail: 'Keyword'
    },
    {
      label: 'BIGINT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BIGINT',
      detail: 'Keyword'
    },
    {
      label: 'BINARY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BINARY',
      detail: 'Keyword'
    },
    {
      label: 'BIT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BIT',
      detail: 'Keyword'
    },
    {
      label: 'BLOB',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BLOB',
      detail: 'Keyword'
    },
    {
      label: 'BOOLEAN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BOOLEAN',
      detail: 'Keyword'
    },
    {
      label: 'BOTH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BOTH',
      detail: 'Keyword'
    },
    {
      label: 'BY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'BY',
      detail: 'Keyword'
    },
    {
      label: 'CALL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CALL',
      detail: 'Keyword'
    },
    {
      label: 'CALLED',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CALLED',
      detail: 'Keyword'
    },
    {
      label: 'CARDINALITY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CARDINALITY',
      detail: 'Keyword'
    },
    {
      label: 'CASCADED',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CASCADED',
      detail: 'Keyword'
    },
    {
      label: 'CASE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CASE',
      detail: 'Keyword'
    },
    {
      label: 'CAST',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CAST',
      detail: 'Keyword'
    },
    {
      label: 'CEIL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CEIL',
      detail: 'Keyword'
    },
    {
      label: 'CEILING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CEILING',
      detail: 'Keyword'
    },
    {
      label: 'CHAR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CHAR',
      detail: 'Keyword'
    },
    {
      label: 'CHAR_LENGTH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CHAR_LENGTH',
      detail: 'Keyword'
    },
    {
      label: 'CHARACTER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CHARACTER',
      detail: 'Keyword'
    },
    {
      label: 'CHARACTER_LENGTH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CHARACTER_LENGTH',
      detail: 'Keyword'
    },
    {
      label: 'CHECK',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CHECK',
      detail: 'Keyword'
    },
    {
      label: 'CLASSIFIER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CLASSIFIER',
      detail: 'Keyword'
    },
    {
      label: 'CLOB',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CLOB',
      detail: 'Keyword'
    },
    {
      label: 'CLOSE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CLOSE',
      detail: 'Keyword'
    },
    {
      label: 'COALESCE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'COALESCE',
      detail: 'Keyword'
    },
    {
      label: 'COLLATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'COLLATE',
      detail: 'Keyword'
    },
    {
      label: 'COLLECT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'COLLECT',
      detail: 'Keyword'
    },
    {
      label: 'COLUMN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'COLUMN',
      detail: 'Keyword'
    },
    {
      label: 'COMMIT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'COMMIT',
      detail: 'Keyword'
    },
    {
      label: 'CONDITION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CONDITION',
      detail: 'Keyword'
    },
    {
      label: 'CONNECT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CONNECT',
      detail: 'Keyword'
    },
    {
      label: 'CONSTRAINT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CONSTRAINT',
      detail: 'Keyword'
    },
    {
      label: 'CONTAINS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CONTAINS',
      detail: 'Keyword'
    },
    {
      label: 'CONVERT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CONVERT',
      detail: 'Keyword'
    },
    {
      label: 'CORR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CORR',
      detail: 'Keyword'
    },
    {
      label: 'CORRESPONDING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CORRESPONDING',
      detail: 'Keyword'
    },
    {
      label: 'COUNT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'COUNT',
      detail: 'Keyword'
    },
    {
      label: 'COVAR_POP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'COVAR_POP',
      detail: 'Keyword'
    },
    {
      label: 'COVAR_SAMP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'COVAR_SAMP',
      detail: 'Keyword'
    },
    {
      label: 'CREATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CREATE',
      detail: 'Keyword'
    },
    {
      label: 'CROSS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CROSS',
      detail: 'Keyword'
    },
    {
      label: 'CUBE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CUBE',
      detail: 'Keyword'
    },
    {
      label: 'CUME_DIST',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CUME_DIST',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_CATALOG',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_CATALOG',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_DATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_DATE',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_DEFAULT_TRANSFORM_GROUP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_DEFAULT_TRANSFORM_GROUP',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_PATH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_PATH',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_ROLE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_ROLE',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_ROW',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_ROW',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_SCHEMA',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_SCHEMA',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_TIME',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_TIME',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_TIMESTAMP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_TIMESTAMP',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_TRANSFORM_GROUP_FOR_TYPE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_TRANSFORM_GROUP_FOR_TYPE',
      detail: 'Keyword'
    },
    {
      label: 'CURRENT_USER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURRENT_USER',
      detail: 'Keyword'
    },
    {
      label: 'CURSOR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CURSOR',
      detail: 'Keyword'
    },
    {
      label: 'CYCLE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'CYCLE',
      detail: 'Keyword'
    },
    {
      label: 'DATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DATE',
      detail: 'Keyword'
    },
    {
      label: 'DAY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DAY',
      detail: 'Keyword'
    },
    {
      label: 'DEALLOCATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DEALLOCATE',
      detail: 'Keyword'
    },
    {
      label: 'DEC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DEC',
      detail: 'Keyword'
    },
    {
      label: 'DECIMAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DECIMAL',
      detail: 'Keyword'
    },
    {
      label: 'DECLARE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DECLARE',
      detail: 'Keyword'
    },
    {
      label: 'DEFAULT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DEFAULT',
      detail: 'Keyword'
    },
    {
      label: 'DEFINE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DEFINE',
      detail: 'Keyword'
    },
    {
      label: 'DELETE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DELETE',
      detail: 'Keyword'
    },
    {
      label: 'DENSE_RANK',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DENSE_RANK',
      detail: 'Keyword'
    },
    {
      label: 'DEREF',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DEREF',
      detail: 'Keyword'
    },
    {
      label: 'DESCRIBE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DESCRIBE',
      detail: 'Keyword'
    },
    {
      label: 'DETERMINISTIC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DETERMINISTIC',
      detail: 'Keyword'
    },
    {
      label: 'DISALLOW',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DISALLOW',
      detail: 'Keyword'
    },
    {
      label: 'DISCONNECT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DISCONNECT',
      detail: 'Keyword'
    },
    {
      label: 'DISTINCT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DISTINCT',
      detail: 'Keyword'
    },
    {
      label: 'DOUBLE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DOUBLE',
      detail: 'Keyword'
    },
    {
      label: 'DROP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DROP',
      detail: 'Keyword'
    },
    {
      label: 'DYNAMIC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'DYNAMIC',
      detail: 'Keyword'
    },
    {
      label: 'EACH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EACH',
      detail: 'Keyword'
    },
    {
      label: 'ELEMENT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ELEMENT',
      detail: 'Keyword'
    },
    {
      label: 'ELSE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ELSE',
      detail: 'Keyword'
    },
    {
      label: 'EMPTY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EMPTY',
      detail: 'Keyword'
    },
    {
      label: 'END',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'END',
      detail: 'Keyword'
    },
    {
      label: 'END-EXEC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'END-EXEC',
      detail: 'Keyword'
    },
    {
      label: 'END_FRAME',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'END_FRAME',
      detail: 'Keyword'
    },
    {
      label: 'END_PARTITION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'END_PARTITION',
      detail: 'Keyword'
    },
    {
      label: 'EQUALS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EQUALS',
      detail: 'Keyword'
    },
    {
      label: 'ESCAPE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ESCAPE',
      detail: 'Keyword'
    },
    {
      label: 'EVERY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EVERY',
      detail: 'Keyword'
    },
    {
      label: 'EXCEPT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EXCEPT',
      detail: 'Keyword'
    },
    {
      label: 'EXEC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EXEC',
      detail: 'Keyword'
    },
    {
      label: 'EXECUTE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EXECUTE',
      detail: 'Keyword'
    },
    {
      label: 'EXISTS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EXISTS',
      detail: 'Keyword'
    },
    {
      label: 'EXP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EXP',
      detail: 'Keyword'
    },
    {
      label: 'EXPLAIN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EXPLAIN',
      detail: 'Keyword'
    },
    {
      label: 'EXTEND',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EXTEND',
      detail: 'Keyword'
    },
    {
      label: 'EXTERNAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EXTERNAL',
      detail: 'Keyword'
    },
    {
      label: 'EXTRACT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'EXTRACT',
      detail: 'Keyword'
    },
    {
      label: 'FALSE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FALSE',
      detail: 'Keyword'
    },
    {
      label: 'FETCH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FETCH',
      detail: 'Keyword'
    },
    {
      label: 'FILTER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FILTER',
      detail: 'Keyword'
    },
    {
      label: 'FIRST_VALUE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FIRST_VALUE',
      detail: 'Keyword'
    },
    {
      label: 'FLOAT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FLOAT',
      detail: 'Keyword'
    },
    {
      label: 'FLOOR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FLOOR',
      detail: 'Keyword'
    },
    {
      label: 'FOR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FOR',
      detail: 'Keyword'
    },
    {
      label: 'FOREIGN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FOREIGN',
      detail: 'Keyword'
    },
    {
      label: 'FRAME_ROW',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FRAME_ROW',
      detail: 'Keyword'
    },
    {
      label: 'FREE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FREE',
      detail: 'Keyword'
    },
    {
      label: 'FROM',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FROM',
      detail: 'Keyword'
    },
    {
      label: 'FULL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FULL',
      detail: 'Keyword'
    },
    {
      label: 'FUNCTION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FUNCTION',
      detail: 'Keyword'
    },
    {
      label: 'FUSION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'FUSION',
      detail: 'Keyword'
    },
    {
      label: 'GET',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'GET',
      detail: 'Keyword'
    },
    {
      label: 'GLOBAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'GLOBAL',
      detail: 'Keyword'
    },
    {
      label: 'GRANT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'GRANT',
      detail: 'Keyword'
    },
    {
      label: 'GROUP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'GROUP',
      detail: 'Keyword'
    },
    {
      label: 'GROUPING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'GROUPING',
      detail: 'Keyword'
    },
    {
      label: 'GROUPS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'GROUPS',
      detail: 'Keyword'
    },
    {
      label: 'HAVING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'HAVING',
      detail: 'Keyword'
    },
    {
      label: 'HOLD',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'HOLD',
      detail: 'Keyword'
    },
    {
      label: 'HOUR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'HOUR',
      detail: 'Keyword'
    },
    {
      label: 'IDENTITY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'IDENTITY',
      detail: 'Keyword'
    },
    {
      label: 'IMPORT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'IMPORT',
      detail: 'Keyword'
    },
    {
      label: 'IN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'IN',
      detail: 'Keyword'
    },
    {
      label: 'INDICATOR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INDICATOR',
      detail: 'Keyword'
    },
    {
      label: 'INITIAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INITIAL',
      detail: 'Keyword'
    },
    {
      label: 'INNER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INNER',
      detail: 'Keyword'
    },
    {
      label: 'INOUT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INOUT',
      detail: 'Keyword'
    },
    {
      label: 'INSENSITIVE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INSENSITIVE',
      detail: 'Keyword'
    },
    {
      label: 'INSERT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INSERT',
      detail: 'Keyword'
    },
    {
      label: 'INT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INT',
      detail: 'Keyword'
    },
    {
      label: 'INTEGER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INTEGER',
      detail: 'Keyword'
    },
    {
      label: 'INTERSECT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INTERSECT',
      detail: 'Keyword'
    },
    {
      label: 'INTERSECTION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INTERSECTION',
      detail: 'Keyword'
    },
    {
      label: 'INTERVAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INTERVAL',
      detail: 'Keyword'
    },
    {
      label: 'INTO',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'INTO',
      detail: 'Keyword'
    },
    {
      label: 'IS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'IS',
      detail: 'Keyword'
    },
    {
      label: 'JOIN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'JOIN',
      detail: 'Keyword'
    },
    {
      label: 'LAG',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LAG',
      detail: 'Keyword'
    },
    {
      label: 'LANGUAGE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LANGUAGE',
      detail: 'Keyword'
    },
    {
      label: 'LARGE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LARGE',
      detail: 'Keyword'
    },
    {
      label: 'LAST_VALUE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LAST_VALUE',
      detail: 'Keyword'
    },
    {
      label: 'LATERAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LATERAL',
      detail: 'Keyword'
    },
    {
      label: 'LEAD',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LEAD',
      detail: 'Keyword'
    },
    {
      label: 'LEADING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LEADING',
      detail: 'Keyword'
    },
    {
      label: 'LEFT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LEFT',
      detail: 'Keyword'
    },
    {
      label: 'LIKE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LIKE',
      detail: 'Keyword'
    },
    {
      label: 'LIKE_REGEX',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LIKE_REGEX',
      detail: 'Keyword'
    },
    {
      label: 'LIMIT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LIMIT',
      detail: 'Keyword'
    },
    {
      label: 'LN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LN',
      detail: 'Keyword'
    },
    {
      label: 'LOCAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LOCAL',
      detail: 'Keyword'
    },
    {
      label: 'LOCALTIME',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LOCALTIME',
      detail: 'Keyword'
    },
    {
      label: 'LOCALTIMESTAMP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LOCALTIMESTAMP',
      detail: 'Keyword'
    },
    {
      label: 'LOWER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'LOWER',
      detail: 'Keyword'
    },
    {
      label: 'MATCH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MATCH',
      detail: 'Keyword'
    },
    {
      label: 'MATCHES',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MATCHES',
      detail: 'Keyword'
    },
    {
      label: 'MATCH_NUMBER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MATCH_NUMBER',
      detail: 'Keyword'
    },
    {
      label: 'MATCH_RECOGNIZE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MATCH_RECOGNIZE',
      detail: 'Keyword'
    },
    {
      label: 'MAX',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MAX',
      detail: 'Keyword'
    },
    {
      label: 'MEASURES',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MEASURES',
      detail: 'Keyword'
    },
    {
      label: 'MEMBER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MEMBER',
      detail: 'Keyword'
    },
    {
      label: 'MERGE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MERGE',
      detail: 'Keyword'
    },
    {
      label: 'METHOD',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'METHOD',
      detail: 'Keyword'
    },
    {
      label: 'MIN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MIN',
      detail: 'Keyword'
    },
    {
      label: 'MINUTE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MINUTE',
      detail: 'Keyword'
    },
    {
      label: 'MOD',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MOD',
      detail: 'Keyword'
    },
    {
      label: 'MODIFIES',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MODIFIES',
      detail: 'Keyword'
    },
    {
      label: 'MODULE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MODULE',
      detail: 'Keyword'
    },
    {
      label: 'MONTH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MONTH',
      detail: 'Keyword'
    },
    {
      label: 'MORE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MORE',
      detail: 'Keyword'
    },
    {
      label: 'MULTISET',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MULTISET',
      detail: 'Keyword'
    },
    {
      label: 'NATIONAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NATIONAL',
      detail: 'Keyword'
    },
    {
      label: 'NATURAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NATURAL',
      detail: 'Keyword'
    },
    {
      label: 'NCHAR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NCHAR',
      detail: 'Keyword'
    },
    {
      label: 'NCLOB',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NCLOB',
      detail: 'Keyword'
    },
    {
      label: 'NEW',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NEW',
      detail: 'Keyword'
    },
    {
      label: 'NEXT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NEXT',
      detail: 'Keyword'
    },
    {
      label: 'NO',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NO',
      detail: 'Keyword'
    },
    {
      label: 'NONE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NONE',
      detail: 'Keyword'
    },
    {
      label: 'NORMALIZE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NORMALIZE',
      detail: 'Keyword'
    },
    {
      label: 'NOT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NOT',
      detail: 'Keyword'
    },
    {
      label: 'NTH_VALUE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NTH_VALUE',
      detail: 'Keyword'
    },
    {
      label: 'NTILE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NTILE',
      detail: 'Keyword'
    },
    {
      label: 'NULL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NULL',
      detail: 'Keyword'
    },
    {
      label: 'NULLIF',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NULLIF',
      detail: 'Keyword'
    },
    {
      label: 'NUMERIC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'NUMERIC',
      detail: 'Keyword'
    },
    {
      label: 'OCCURRENCES_REGEX',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OCCURRENCES_REGEX',
      detail: 'Keyword'
    },
    {
      label: 'OCTET_LENGTH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OCTET_LENGTH',
      detail: 'Keyword'
    },
    {
      label: 'OF',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OF',
      detail: 'Keyword'
    },
    {
      label: 'OFFSET',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OFFSET',
      detail: 'Keyword'
    },
    {
      label: 'OLD',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OLD',
      detail: 'Keyword'
    },
    {
      label: 'OMIT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OMIT',
      detail: 'Keyword'
    },
    {
      label: 'ON',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ON',
      detail: 'Keyword'
    },
    {
      label: 'ONE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ONE',
      detail: 'Keyword'
    },
    {
      label: 'ONLY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ONLY',
      detail: 'Keyword'
    },
    {
      label: 'OPEN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OPEN',
      detail: 'Keyword'
    },
    {
      label: 'OR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OR',
      detail: 'Keyword'
    },
    {
      label: 'ORDER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ORDER',
      detail: 'Keyword'
    },
    {
      label: 'OUT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OUT',
      detail: 'Keyword'
    },
    {
      label: 'OUTER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OUTER',
      detail: 'Keyword'
    },
    {
      label: 'OVER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OVER',
      detail: 'Keyword'
    },
    {
      label: 'OVERLAPS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OVERLAPS',
      detail: 'Keyword'
    },
    {
      label: 'OVERLAY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'OVERLAY',
      detail: 'Keyword'
    },
    {
      label: 'PARAMETER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PARAMETER',
      detail: 'Keyword'
    },
    {
      label: 'PARTITION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PARTITION',
      detail: 'Keyword'
    },
    {
      label: 'PATTERN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PATTERN',
      detail: 'Keyword'
    },
    {
      label: 'PER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PER',
      detail: 'Keyword'
    },
    {
      label: 'PERCENT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PERCENT',
      detail: 'Keyword'
    },
    {
      label: 'PERCENTILE_CONT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PERCENTILE_CONT',
      detail: 'Keyword'
    },
    {
      label: 'PERCENTILE_DISC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PERCENTILE_DISC',
      detail: 'Keyword'
    },
    {
      label: 'PERCENT_RANK',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PERCENT_RANK',
      detail: 'Keyword'
    },
    {
      label: 'PERIOD',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PERIOD',
      detail: 'Keyword'
    },
    {
      label: 'PERMUTE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PERMUTE',
      detail: 'Keyword'
    },
    {
      label: 'PORTION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PORTION',
      detail: 'Keyword'
    },
    {
      label: 'POSITION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'POSITION',
      detail: 'Keyword'
    },
    {
      label: 'POSITION_REGEX',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'POSITION_REGEX',
      detail: 'Keyword'
    },
    {
      label: 'POWER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'POWER',
      detail: 'Keyword'
    },
    {
      label: 'PRECEDES',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PRECEDES',
      detail: 'Keyword'
    },
    {
      label: 'PRECISION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PRECISION',
      detail: 'Keyword'
    },
    {
      label: 'PREPARE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PREPARE',
      detail: 'Keyword'
    },
    {
      label: 'PREV',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PREV',
      detail: 'Keyword'
    },
    {
      label: 'PRIMARY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PRIMARY',
      detail: 'Keyword'
    },
    {
      label: 'PROCEDURE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'PROCEDURE',
      detail: 'Keyword'
    },
    {
      label: 'RANGE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RANGE',
      detail: 'Keyword'
    },
    {
      label: 'RANK',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RANK',
      detail: 'Keyword'
    },
    {
      label: 'READS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'READS',
      detail: 'Keyword'
    },
    {
      label: 'REAL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REAL',
      detail: 'Keyword'
    },
    {
      label: 'RECURSIVE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RECURSIVE',
      detail: 'Keyword'
    },
    {
      label: 'REF',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REF',
      detail: 'Keyword'
    },
    {
      label: 'REFERENCES',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REFERENCES',
      detail: 'Keyword'
    },
    {
      label: 'REFERENCING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REFERENCING',
      detail: 'Keyword'
    },
    {
      label: 'REGR_AVGX',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REGR_AVGX',
      detail: 'Keyword'
    },
    {
      label: 'REGR_AVGY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REGR_AVGY',
      detail: 'Keyword'
    },
    {
      label: 'REGR_COUNT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REGR_COUNT',
      detail: 'Keyword'
    },
    {
      label: 'REGR_INTERCEPT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REGR_INTERCEPT',
      detail: 'Keyword'
    },
    {
      label: 'REGR_R2',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REGR_R2',
      detail: 'Keyword'
    },
    {
      label: 'REGR_SLOPE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REGR_SLOPE',
      detail: 'Keyword'
    },
    {
      label: 'REGR_SXX',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REGR_SXX',
      detail: 'Keyword'
    },
    {
      label: 'REGR_SXY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REGR_SXY',
      detail: 'Keyword'
    },
    {
      label: 'REGR_SYY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REGR_SYY',
      detail: 'Keyword'
    },
    {
      label: 'RELEASE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RELEASE',
      detail: 'Keyword'
    },
    {
      label: 'RESET',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RESET',
      detail: 'Keyword'
    },
    {
      label: 'RESULT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RESULT',
      detail: 'Keyword'
    },
    {
      label: 'RETURN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RETURN',
      detail: 'Keyword'
    },
    {
      label: 'RETURNS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RETURNS',
      detail: 'Keyword'
    },
    {
      label: 'REVOKE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'REVOKE',
      detail: 'Keyword'
    },
    {
      label: 'RIGHT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RIGHT',
      detail: 'Keyword'
    },
    {
      label: 'ROLLBACK',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ROLLBACK',
      detail: 'Keyword'
    },
    {
      label: 'ROLLUP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ROLLUP',
      detail: 'Keyword'
    },
    {
      label: 'ROW',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ROW',
      detail: 'Keyword'
    },
    {
      label: 'ROW_NUMBER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ROW_NUMBER',
      detail: 'Keyword'
    },
    {
      label: 'ROWS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'ROWS',
      detail: 'Keyword'
    },
    {
      label: 'RUNNING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'RUNNING',
      detail: 'Keyword'
    },
    {
      label: 'SAVEPOINT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SAVEPOINT',
      detail: 'Keyword'
    },
    {
      label: 'SCOPE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SCOPE',
      detail: 'Keyword'
    },
    {
      label: 'SCROLL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SCROLL',
      detail: 'Keyword'
    },
    {
      label: 'SEARCH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SEARCH',
      detail: 'Keyword'
    },
    {
      label: 'SECOND',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SECOND',
      detail: 'Keyword'
    },
    {
      label: 'SEEK',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SEEK',
      detail: 'Keyword'
    },
    {
      label: 'SELECT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SELECT',
      detail: 'Keyword'
    },
    {
      label: 'SENSITIVE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SENSITIVE',
      detail: 'Keyword'
    },
    {
      label: 'SESSION_USER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SESSION_USER',
      detail: 'Keyword'
    },
    {
      label: 'SET',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SET',
      detail: 'Keyword'
    },
    {
      label: 'MINUS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'MINUS',
      detail: 'Keyword'
    },
    {
      label: 'SHOW',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SHOW',
      detail: 'Keyword'
    },
    {
      label: 'SIMILAR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SIMILAR',
      detail: 'Keyword'
    },
    {
      label: 'SKIP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SKIP',
      detail: 'Keyword'
    },
    {
      label: 'SMALLINT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SMALLINT',
      detail: 'Keyword'
    },
    {
      label: 'SOME',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SOME',
      detail: 'Keyword'
    },
    {
      label: 'SPECIFIC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SPECIFIC',
      detail: 'Keyword'
    },
    {
      label: 'SPECIFICTYPE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SPECIFICTYPE',
      detail: 'Keyword'
    },
    {
      label: 'SQL',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SQL',
      detail: 'Keyword'
    },
    {
      label: 'SQLEXCEPTION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SQLEXCEPTION',
      detail: 'Keyword'
    },
    {
      label: 'SQLSTATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SQLSTATE',
      detail: 'Keyword'
    },
    {
      label: 'SQLWARNING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SQLWARNING',
      detail: 'Keyword'
    },
    {
      label: 'SQRT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SQRT',
      detail: 'Keyword'
    },
    {
      label: 'START',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'START',
      detail: 'Keyword'
    },
    {
      label: 'STATIC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'STATIC',
      detail: 'Keyword'
    },
    {
      label: 'STDDEV_POP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'STDDEV_POP',
      detail: 'Keyword'
    },
    {
      label: 'STDDEV_SAMP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'STDDEV_SAMP',
      detail: 'Keyword'
    },
    {
      label: 'STREAM',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'STREAM',
      detail: 'Keyword'
    },
    {
      label: 'SUBMULTISET',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SUBMULTISET',
      detail: 'Keyword'
    },
    {
      label: 'SUBSET',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SUBSET',
      detail: 'Keyword'
    },
    {
      label: 'SUBSTRING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SUBSTRING',
      detail: 'Keyword'
    },
    {
      label: 'SUBSTRING_REGEX',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SUBSTRING_REGEX',
      detail: 'Keyword'
    },
    {
      label: 'SUCCEEDS',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SUCCEEDS',
      detail: 'Keyword'
    },
    {
      label: 'SUM',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SUM',
      detail: 'Keyword'
    },
    {
      label: 'SYMMETRIC',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SYMMETRIC',
      detail: 'Keyword'
    },
    {
      label: 'SYSTEM',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SYSTEM',
      detail: 'Keyword'
    },
    {
      label: 'SYSTEM_TIME',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SYSTEM_TIME',
      detail: 'Keyword'
    },
    {
      label: 'SYSTEM_USER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'SYSTEM_USER',
      detail: 'Keyword'
    },
    {
      label: 'TABLE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TABLE',
      detail: 'Keyword'
    },
    {
      label: 'TABLESAMPLE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TABLESAMPLE',
      detail: 'Keyword'
    },
    {
      label: 'THEN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'THEN',
      detail: 'Keyword'
    },
    {
      label: 'TIME',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TIME',
      detail: 'Keyword'
    },
    {
      label: 'TIMESTAMP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TIMESTAMP',
      detail: 'Keyword'
    },
    {
      label: 'TIMEZONE_HOUR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TIMEZONE_HOUR',
      detail: 'Keyword'
    },
    {
      label: 'TIMEZONE_MINUTE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TIMEZONE_MINUTE',
      detail: 'Keyword'
    },
    {
      label: 'TINYINT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TINYINT',
      detail: 'Keyword'
    },
    {
      label: 'TO',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TO',
      detail: 'Keyword'
    },
    {
      label: 'TRAILING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TRAILING',
      detail: 'Keyword'
    },
    {
      label: 'TRANSLATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TRANSLATE',
      detail: 'Keyword'
    },
    {
      label: 'TRANSLATE_REGEX',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TRANSLATE_REGEX',
      detail: 'Keyword'
    },
    {
      label: 'TRANSLATION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TRANSLATION',
      detail: 'Keyword'
    },
    {
      label: 'TREAT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TREAT',
      detail: 'Keyword'
    },
    {
      label: 'TRIGGER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TRIGGER',
      detail: 'Keyword'
    },
    {
      label: 'TRIM',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TRIM',
      detail: 'Keyword'
    },
    {
      label: 'TRIM_ARRAY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TRIM_ARRAY',
      detail: 'Keyword'
    },
    {
      label: 'TRUE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TRUE',
      detail: 'Keyword'
    },
    {
      label: 'TRUNCATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'TRUNCATE',
      detail: 'Keyword'
    },
    {
      label: 'UESCAPE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'UESCAPE',
      detail: 'Keyword'
    },
    {
      label: 'UNION',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'UNION',
      detail: 'Keyword'
    },
    {
      label: 'UNIQUE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'UNIQUE',
      detail: 'Keyword'
    },
    {
      label: 'UNKNOWN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'UNKNOWN',
      detail: 'Keyword'
    },
    {
      label: 'UNNEST',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'UNNEST',
      detail: 'Keyword'
    },
    {
      label: 'UPDATE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'UPDATE',
      detail: 'Keyword'
    },
    {
      label: 'UPPER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'UPPER',
      detail: 'Keyword'
    },
    {
      label: 'UPSERT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'UPSERT',
      detail: 'Keyword'
    },
    {
      label: 'USER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'USER',
      detail: 'Keyword'
    },
    {
      label: 'USING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'USING',
      detail: 'Keyword'
    },
    {
      label: 'VALUE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'VALUE',
      detail: 'Keyword'
    },
    {
      label: 'VALUES',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'VALUES',
      detail: 'Keyword'
    },
    {
      label: 'VALUE_OF',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'VALUE_OF',
      detail: 'Keyword'
    },
    {
      label: 'VAR_POP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'VAR_POP',
      detail: 'Keyword'
    },
    {
      label: 'VAR_SAMP',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'VAR_SAMP',
      detail: 'Keyword'
    },
    {
      label: 'VARBINARY',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'VARBINARY',
      detail: 'Keyword'
    },
    {
      label: 'VARCHAR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'VARCHAR',
      detail: 'Keyword'
    },
    {
      label: 'VARYING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'VARYING',
      detail: 'Keyword'
    },
    {
      label: 'VERSIONING',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'VERSIONING',
      detail: 'Keyword'
    },
    {
      label: 'WHEN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'WHEN',
      detail: 'Keyword'
    },
    {
      label: 'WHENEVER',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'WHENEVER',
      detail: 'Keyword'
    },
    {
      label: 'WHERE',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'WHERE',
      detail: 'Keyword'
    },
    {
      label: 'WIDTH_BUCKET',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'WIDTH_BUCKET',
      detail: 'Keyword'
    },
    {
      label: 'WINDOW',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'WINDOW',
      detail: 'Keyword'
    },
    {
      label: 'WITH',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'WITH',
      detail: 'Keyword'
    },
    {
      label: 'WITHIN',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'WITHIN',
      detail: 'Keyword'
    },
    {
      label: 'WITHOUT',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'WITHOUT',
      detail: 'Keyword'
    },
    {
      label: 'YEAR',
      kind: monaco.languages.CompletionItemKind.Keyword,
      insertText: 'YEAR',
      detail: 'Keyword'
    }
  ];
};
