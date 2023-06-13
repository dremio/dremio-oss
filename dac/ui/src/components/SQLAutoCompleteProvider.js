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
import {
  getAutoCompleteTypeMap,
  getAutoCompleteSortTextValue,
  getAutoCompleteKind,
  constructTransformValues,
} from "@app/utils/sql-autocomplete";

const getItems = (monaco, sqlContextGetter) => {
  if (typeof sqlContextGetter !== "function") {
    throw new Error(
      "sqlContextGetter must be specified and must be a function"
    );
  }

  const CompletionItemKind = monaco.languages.CompletionItemKind;
  const typeMap = getAutoCompleteTypeMap(CompletionItemKind);

  return async (document, position) => {
    const delimiter = "\n";
    const content = document.getLinesContent();

    let pos = position.column - 1; // -1 to convert to zero-base index
    for (let i = 0; i < position.lineNumber - 1; i++) {
      pos += content[i].length + delimiter.length;
    }

    // If the previous char is a whitespace, don't trigger autocomplete
    if (
      position.column >= 2 &&
      /\s/.test(content[position.lineNumber - 1][position.column - 2])
    ) {
      return [];
    }

    const requestBody = {
      query: content.join(delimiter),
      cursor: pos,
      context: sqlContextGetter(),
    };

    try {
      let response = await ApiUtils.fetch(
        "sql/autocomplete",
        {
          method: "POST",
          body: JSON.stringify(requestBody),
        },
        2
      );
      response = await response.json();

      const activeWord = document.getWordAtPosition(position)?.word ?? "";
      const transformValues = constructTransformValues(
        content,
        position,
        activeWord
      );

      return (response?.completionItems ?? []).map(
        ({ label, detail, kind, data, insertText }, index) => {
          let insertTextOrSnippet;
          if (kind == "Function") {
            // Snippet String
            // https://github.com/microsoft/monaco-editor/blob/v0.10.1/monaco.d.ts#L4119
            insertTextOrSnippet = { value: insertText };
          } else {
            insertTextOrSnippet = insertText;
          }

          return {
            label: label,
            kind: getAutoCompleteKind(typeMap, kind, data),
            sortText: getAutoCompleteSortTextValue(index),
            detail: detail,
            insertText: insertTextOrSnippet,
          };
        }
      );
    } catch (e) {
      console.error(e);
    }
  };
};

export const SQLAutoCompleteProvider = (monaco, sqlContextGetter) => ({
  provideCompletionItems: debounce(getItems(monaco, sqlContextGetter), 200, {
    leading: true,
    maxWait: 200,
  }),
});
