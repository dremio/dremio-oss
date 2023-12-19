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

import SQLParsingWorker, {
  type CheckCancellationRequest,
  type CheckCancellationResponse,
  type Document,
  type RunAutocompleteData,
  type RunAutocompleteRequest,
  type RunAutocompleteResponse,
  type RunErrorDetectionData,
  type RunErrorDetectionRequest,
  type RunErrorDetectionResponse,
  type RunInitializeData,
  type RunInitializeRequest,
  type SQLFunction,
  type SQLParsingWorkerRequest,
  type SQLParsingWorkerResponse,
  type SQLError,
} from "../server/SQLParsingWorker.worker";

type SqlContextGetter = () => string[];

// SQL parsing for live edit and autocomplete runs in a web worker background process to ensure that lengthy parse times
// don't interfere with re-rendering of the DOM/sql runner showing the characters typed
const sqlParsingWorker = new SQLParsingWorker();

function initializeWorker(data: RunInitializeData) {
  const initializeRequest: RunInitializeRequest = {
    type: "initialize",
    data,
  };
  sqlParsingWorker.postMessage(initializeRequest);
}

function runAutocompleteOnWorker(
  data: RunAutocompleteData,
  isCancellationRequested: () => boolean
): Promise<RunAutocompleteResponse | null> {
  const request: RunAutocompleteRequest = {
    type: "autocomplete",
    data,
  };
  return runRequestOnWorker<RunAutocompleteResponse>(
    request,
    isCancellationRequested
  );
}

function runErrorDetectionOnWorker(
  data: RunErrorDetectionData,
  isCancellationRequested: () => boolean
): Promise<RunErrorDetectionResponse | null> {
  const request: RunErrorDetectionRequest = {
    type: "errorDetection",
    data,
  };
  return runRequestOnWorker<RunErrorDetectionResponse>(
    request,
    isCancellationRequested
  );
}

function runRequestOnWorker<P extends SQLParsingWorkerResponse>(
  request: SQLParsingWorkerRequest,
  isCancellationRequested: () => boolean
): Promise<P | null> {
  return new Promise((resolve) => {
    // We use a message channel to ensure that we only listen to responses for the request we sent (and not the response
    // from some request sent earlier that was still pending at the time we started)
    const channel = new MessageChannel();
    const complete = (result: P | null) => {
      resolve(result);
      channel.port1.close();
      channel.port2.close();
    };
    channel.port2.onmessage = (
      e: MessageEvent<P | CheckCancellationRequest>
    ) => {
      if ("checkShouldCancel" in e.data) {
        // Tell the worker to cancel this request if Monaco says so (this is true if another character has since been
        // typed, or the user hit the escape key to make sure the suggestion widget closes or stays closed).
        // The intention is to make sure that requests for intermediate characters typed are skipped if there is a newer
        // request that the worker should handle instead. E.g. type S -> worker starts handling "S" -> type E -> type L
        // -> type E -> type C -> worker finishes handling "S" -> worker should skip handling ELE requests and next
        // handle "C" rather than fulfill them one by one
        const shouldCancel = isCancellationRequested();
        const response: CheckCancellationResponse = { shouldCancel };
        channel.port2.postMessage(response);
        if (shouldCancel) {
          complete(null);
        }
      } else {
        complete(e.data); // run response data
      }
    };
    sqlParsingWorker.postMessage(request, [channel.port1]);
  });
}

export class SQLEditorExtension {
  private sqlContextGetter: SqlContextGetter;
  private currentModelVersion: number;

  constructor(
    sqlContextGetter: SqlContextGetter,
    authToken: string,
    getSuggestionsURL: string,
    sqlFunctions: SQLFunction[]
  ) {
    this.sqlContextGetter = sqlContextGetter;
    this.currentModelVersion = -1;
    initializeWorker({
      sqlFunctions,
      authToken,
      getSuggestionsURL,
    });
  }

  completionItemProvider = {
    provideCompletionItems: async (
      model: monaco.editor.IReadOnlyModel,
      position: monaco.Position,
      cancellationToken: monaco.CancellationToken
    ): Promise<monaco.languages.CompletionItem[]> => {
      const document: Document = {
        linesContent: model.getLinesContent(),
        wordAtPosition: model.getWordAtPosition(position),
      };
      const data: RunAutocompleteData = {
        document,
        position,
        queryContext: this.sqlContextGetter(),
      };
      const completionItems: monaco.languages.CompletionItem[] | null =
        await runAutocompleteOnWorker(
          data,
          () => cancellationToken.isCancellationRequested
        );
      if (completionItems == null) {
        console.debug(
          `Autocomplete request cancelled for model version: ${model.getVersionId()}, word: ${JSON.stringify(
            document.wordAtPosition
          )}`
        );
        return [];
      }
      return completionItems;
    },
  };

  errorDetectionProvider = {
    getLiveErrors: async (
      model: monaco.editor.IReadOnlyModel,
      modelVersion: number
    ): Promise<SQLError[]> => {
      this.currentModelVersion = modelVersion;

      const data: RunErrorDetectionData = {
        linesContent: model.getLinesContent(),
      };
      // If this returns false, that means there is a newer pending error detection request; we should cancel this one
      // to prioritize the newer one instead
      const isCancellationRequested = () =>
        modelVersion < this.currentModelVersion;
      const syntaxErrors: SQLError[] | null = await runErrorDetectionOnWorker(
        data,
        isCancellationRequested
      );
      // Do not use model after this point because it may have been mutated already by react-monaco-editor with newer
      // editor changes
      if (syntaxErrors == null) {
        console.debug(
          `Error detection request cancelled for model version: ${modelVersion}`
        );
        return [];
      }
      return syntaxErrors;
    },
  };
}
