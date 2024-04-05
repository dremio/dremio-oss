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

import { UnauthorizedError } from "../errors/UnauthorizedError";
import { HttpError } from "../errors/HttpError";
import { narrowHttpError } from "../errors/narrowHttpError";
import { getSessionContext } from "../contexts/SessionContext";
import { BadGatewayError } from "../errors/BadGatewayError";
import { ServiceUnavailableError } from "../errors/ServiceUnavailableError";
import { GatewayTimeoutError } from "../errors/GatewayTimeoutError";
import { waitForServerReachable } from "../utilities/waitForServerReachable";

const MAX_RETRIES = 4;

const getRetryDuration = (error: HttpError, triesRemaining: number): number => {
  if (error.res.headers.get("retry-after")) {
    // ignore for now
  }
  return 2 ** (MAX_RETRIES - triesRemaining) * 1000;
};

const isRetryableError = (error: HttpError) => {
  return (
    error instanceof BadGatewayError ||
    error instanceof ServiceUnavailableError ||
    error instanceof GatewayTimeoutError
  );
};

export const appFetch = (input: RequestInfo | URL, init?: RequestInit) => {
  const startFetch = (
    triesRemaining: number,
  ): ReturnType<typeof window.fetch> => {
    return fetch(input, init)
      .then(async (res) => {
        if (!res.ok) {
          const error = await narrowHttpError(new HttpError(res));
          if (isRetryableError(error) && triesRemaining) {
            await new Promise((resolve) =>
              setTimeout(resolve, getRetryDuration(error, triesRemaining)),
            );
            return startFetch(triesRemaining - 1);
          }
          throw error;
        }
        return res;
      })
      .catch((err) => {
        if (err instanceof TypeError || err.message === "Failed to fetch") {
          return waitForServerReachable().then(() =>
            startFetch(triesRemaining),
          );
        }

        if (err instanceof UnauthorizedError) {
          getSessionContext().handleInvalidSession();
          return new Promise(() => {});
        }

        throw err;
      });
  };

  return startFetch(MAX_RETRIES);
};

/**
 * @deprecated This version of `appFetch` allows old error handling code that expects to decode
 * the response itself to continue working.
 */
export const appFetchWithoutErrorHandling = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => {
  const startFetch = (
    triesRemaining: number,
  ): ReturnType<typeof window.fetch> => {
    return fetch(input, init)
      .then(async (res) => {
        if (!res.ok) {
          throw res;
        }
        return res;
      })
      .catch((err) => {
        if (err instanceof TypeError || err.message === "Failed to fetch") {
          return waitForServerReachable().then(() =>
            startFetch(triesRemaining),
          );
        }

        throw err;
      });
  };

  return startFetch(MAX_RETRIES);
};
