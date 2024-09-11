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

import Immutable from "immutable";
import { runAutocompleteOnWorker } from "../../../../../../sql/worker/client/SQLParsingWorkerClient";
import type {
  Document,
  RunAutocompleteData,
} from "../../../../../../sql/worker/server/SQLParsingWorker.worker";
import type { ModifiedFunction } from "../../../../../functions/Functions.type";
import { monacoLogger } from "../../../utilities/monacoLogger";
import type { CompletionProviderExtension } from "../types/extensions.type";

export const completionProvider =
  (
    functions?: ModifiedFunction[],
    queryContext?: Immutable.List<string>,
  ): CompletionProviderExtension =>
  (extensions) => ({
    ...extensions,
    completionItemProvider: {
      functions,
      triggerCharacters: [".", '"', "("],
      // @ts-expect-error type mismatch
      provideCompletionItems: async (model, position, context, token) => {
        const document: Document = {
          linesContent: model.getLinesContent(),
          wordAtPosition: model.getWordAtPosition(position),
        };

        const data: RunAutocompleteData = {
          document,
          position,
          queryContext: queryContext?.toJS() ?? [],
        };

        const completionItems = await runAutocompleteOnWorker(
          data,
          () => token.isCancellationRequested,
        );

        if (completionItems == null) {
          monacoLogger.debug(
            `Autocomplete request cancelled for model version: ${model.getVersionId()}, word: ${JSON.stringify(
              document.wordAtPosition,
            )}`,
          );

          return { suggestions: [] };
        }

        return {
          suggestions: completionItems,
        };
      },
    },
  });
