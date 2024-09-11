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
  type RunAutocompleteData,
  type RunAutocompleteRequest,
  type RunAutocompleteResponse,
  type RunErrorDetectionData,
  type RunErrorDetectionRequest,
  type RunErrorDetectionResponse,
  type RunInitializeData,
  type RunInitializeRequest,
  type SQLParsingWorkerRequest,
  type SQLParsingWorkerResponse,
} from "../server/SQLParsingWorker.worker";

// SQL parsing for live edit and autocomplete runs in a web worker background process to ensure that lengthy parse times
// don't interfere with re-rendering of the DOM/sql runner showing the characters typed
const sqlParsingWorker = new SQLParsingWorker();

export function initializeWorker(data: RunInitializeData) {
  const initializeRequest: RunInitializeRequest = {
    type: "initialize",
    data,
  };
  sqlParsingWorker.postMessage(initializeRequest);
}

export function runAutocompleteOnWorker(
  data: RunAutocompleteData,
  isCancellationRequested: () => boolean,
): Promise<RunAutocompleteResponse | null> {
  const request: RunAutocompleteRequest = {
    type: "autocomplete",
    data,
  };
  return runRequestOnWorker<RunAutocompleteResponse>(
    request,
    isCancellationRequested,
  );
}

export function runErrorDetectionOnWorker(
  data: RunErrorDetectionData,
  isCancellationRequested: () => boolean,
): Promise<RunErrorDetectionResponse | null> {
  const request: RunErrorDetectionRequest = {
    type: "errorDetection",
    data,
  };
  return runRequestOnWorker<RunErrorDetectionResponse>(
    request,
    isCancellationRequested,
  );
}

function runRequestOnWorker<P extends SQLParsingWorkerResponse>(
  request: SQLParsingWorkerRequest,
  isCancellationRequested: () => boolean,
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
      e: MessageEvent<P | CheckCancellationRequest>,
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
