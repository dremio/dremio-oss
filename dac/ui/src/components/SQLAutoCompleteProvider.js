/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import debounce from 'lodash/debounce';
import ApiUtils from 'utils/apiUtils/apiUtils';

const errorHandler = (error) => {
  //add loggin here if it is needed
};

const getItems = (monaco, sqlContextGetter) => {
  if (typeof sqlContextGetter !== 'function') {
    throw new Error('sqlContextGetter must be specified and must be a function');
  }

  const CompletionItemKind = monaco.languages.CompletionItemKind;
  const typeMap = {
    COLUMN: CompletionItemKind.Field,
    TABLE: CompletionItemKind.Variable,
    VIEW: CompletionItemKind.Reference,
    SCHEMA: CompletionItemKind.File,
    CATALOG: CompletionItemKind.Folder,
    REPOSITORY: CompletionItemKind.Class,
    FUNCTION: CompletionItemKind.Function,
    KEYWORD: CompletionItemKind.Keyword
  };

  return (document, position, cancellationToken, context) => {
    const delimiter = '\n';
    const content = document.getLinesContent();

    let pos = position.column - 1; // -1 to convert to zero-base index
    for (let i = 0; i < position.lineNumber - 1; i++) {
      pos += content[i].length + delimiter.length;
    }

    const request = {
      sql: content.join(delimiter),
      context: sqlContextGetter() || [], // string[]
      cursorPosition: pos
    };

    return ApiUtils.fetch('sql/analyze/suggest',
      {
        method: 'POST',
        body: JSON.stringify(request)
      }, 2).then((data) => {
        const contentType = data.headers.get('content-type');
        if (contentType && contentType.indexOf('application/json') !== -1) {
          return data.json().then(({ suggestions }) => {
            return suggestions.map(({
              name,
              type
            }) => ({
              label: name,
              kind: typeMap[type],
              detail: type
            }));
          });
        }
        return [];
      }, errorHandler);
  };
};

export const SQLAutoCompleteProvider = (monaco, sqlContextGetter) => ({
  provideCompletionItems: debounce(getItems(monaco, sqlContextGetter), 100, {
    leading: true,
    maxWait: 500
  })
});
