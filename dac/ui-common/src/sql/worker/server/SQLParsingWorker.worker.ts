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

/* eslint-disable */
import { additionalSetup } from "../../../utilities/webWorker";
import { assertNever } from "../../../utilities/typeUtils";
import type { SQLFunction } from "../../autocomplete/types/SQLFunction";
import { AutocompleteApiClient } from "../../autocomplete/apiClient/autocompleteApi";
import { getSuggestionsAPICreator } from "../../autocomplete/endpoints/GetSuggestionsAPI";
import { AutocompleteEngine } from "../../autocomplete/engine/AutocompleteEngine";
import { CursorQueryPosition } from "../../autocomplete/types/CursorQueryPosition";
import { LiveEditParsingEngine } from "../../parser/engine/LiveEditParsingEngine";
import { ErrorDetectionEngine } from "../../errorDetection/engine/ErrorDetectionEngine";
import { SQLError } from "../../errorDetection/types/SQLError";

export { SQLFunction, SQLError };

export type Document = {
  /**
   * The text for all lines.
   */
  linesContent: string[];

  /**
   * The word at the given editor position.
   */
  wordAtPosition: monaco.editor.IWordAtPosition | null;
};

export type RunInitializeRequest = {
  type: "initialize";
  data: RunInitializeData;
};

export type RunInitializeData = {
  sqlFunctions: SQLFunction[];
  authToken: string;
  getSuggestionsURL: string;
};

export type RunAutocompleteRequest = {
  type: "autocomplete";
  data: RunAutocompleteData;
};

export type RunAutocompleteData = {
  document: Document;
  position: monaco.Position;
  queryContext: string[];
};

export type RunErrorDetectionRequest = {
  type: "errorDetection";
  data: RunErrorDetectionData;
};

export type RunErrorDetectionData = {
  linesContent: string[];
};

export type SQLParsingWorkerRequest =
  | RunInitializeRequest
  | RunAutocompleteRequest
  | RunErrorDetectionRequest;

export type SQLParsingWorkerResponse =
  | RunAutocompleteResponse
  | RunErrorDetectionResponse;

export type RunAutocompleteResponse = monaco.languages.CompletionItem[];

export type RunErrorDetectionResponse = SQLError[];

export type CheckCancellationRequest = {
  checkShouldCancel: true;
};

export type CheckCancellationResponse = {
  shouldCancel: boolean;
};

let liveEditParsingEngine: LiveEditParsingEngine;
let autocompleteEngine: AutocompleteEngine;
let errorDetectionEngine: ErrorDetectionEngine;

function initialize(data: RunInitializeData): void {
  const autocompleteApi = new AutocompleteApiClient(
    getSuggestionsAPICreator(data.authToken, data.getSuggestionsURL)
  );
  if (!liveEditParsingEngine) {
    liveEditParsingEngine = new LiveEditParsingEngine();
    errorDetectionEngine = new ErrorDetectionEngine(liveEditParsingEngine);
  }
  autocompleteEngine = new AutocompleteEngine(
    liveEditParsingEngine,
    autocompleteApi,
    data.sqlFunctions
  );
}

function runAutocomplete(
  data: RunAutocompleteData
): Promise<RunAutocompleteResponse> {
  const queryPosition: CursorQueryPosition = {
    line: data.position.lineNumber,
    column: data.position.column - 1,
  };
  return autocompleteEngine.generateCompletionItems(
    data.document.linesContent,
    queryPosition,
    data.queryContext
  );
}

function runErrorDetection(
  data: RunErrorDetectionData
): RunErrorDetectionResponse {
  return errorDetectionEngine.detectSqlErrors(data.linesContent);
}

async function checkShouldCancel(port: MessagePort): Promise<boolean> {
  const request: CheckCancellationRequest = { checkShouldCancel: true };
  return new Promise((resolve) => {
    port.onmessage = (e: MessageEvent<CheckCancellationResponse>) => {
      resolve(e.data.shouldCancel);
    };
    port.postMessage(request);
  });
}

(async function () {
  additionalSetup();

  const ctx: Worker = self as any;

  ctx.onmessage = async (e: MessageEvent<SQLParsingWorkerRequest>) => {
    const port = e.ports[0];
    switch (e.data.type) {
      case "initialize":
        initialize(e.data.data);
        break;
      case "autocomplete":
      case "errorDetection":
        const shouldCancel = await checkShouldCancel(port);
        if (shouldCancel) {
          return;
        }
        let result: RunAutocompleteResponse | RunErrorDetectionResponse;
        if (e.data.type == "autocomplete") {
          result = await runAutocomplete(e.data.data);
        } else {
          result = runErrorDetection(e.data.data);
        }
        port.postMessage(result);
        break;
      default:
        return assertNever(e.data);
    }
  };
})();

// Documented solution to tell Typescript that this file should be treated as a *.worker module that automatically
// exports a no-arg constructor class does not work: https://github.com/webpack-contrib/worker-loader/issues/315
// Typescript ignores the .d.ts declaration if it can resolve the imported file with tsc
// Solution is to export a fake class that matches that default export declaration - this isn't actually used at runtime
// because worker-loader emits its own code to create the worker.
class WebpackWorker extends Worker {
  constructor() {
    super(null as any, null as any);
    throw new Error();
  }
}
export default WebpackWorker;
