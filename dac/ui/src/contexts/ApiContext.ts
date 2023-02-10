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

import { UnauthorizedError } from "dremio-ui-common/errors/UnauthorizedError";
import { setApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { HttpError } from "dremio-ui-common/errors/HttpError";
import { narrowHttpError } from "dremio-ui-common/errors/narrowHttpError";
import { getLoggingContext } from "dremio-ui-common/contexts/LoggingContext.js";
import { getSessionContext } from "./SessionContext";
import { BadGatewayError } from "dremio-ui-common/errors/BadGatewayError";
import { ServiceUnavailableError } from "dremio-ui-common/errors/ServiceUnavailableError";
import { GatewayTimeoutError } from "dremio-ui-common/errors/GatewayTimeoutError";

const MAX_RETRIES = 4;

const getRetryDuration = (error: HttpError, triesRemaining: number): number => {
  if (error.res.headers.get("retry-after")) {
    // ignore for now
  }
  return 2 ** (MAX_RETRIES - triesRemaining) * 1000;
};

const logger = getLoggingContext().createLogger("ApiContext");
const sessionContext = getSessionContext();

const isRetryableError = (error: HttpError) => {
  return (
    error instanceof BadGatewayError ||
    error instanceof ServiceUnavailableError ||
    error instanceof GatewayTimeoutError
  );
};

setApiContext({
  fetch: (input, init = {}) => {
    const sessionIdentifier = sessionContext.getSessionIdentifier();
    if (!sessionIdentifier) {
      logger.debug("Canceling request because session is invalid", input);
      sessionContext.handleInvalidSession();
      return new Promise(() => {});
    }

    const startFetch = (
      triesRemaining: number
    ): ReturnType<typeof window.fetch> => {
      return fetch(input, {
        ...init,
        headers: {
          Authorization: `Bearer ${sessionIdentifier}`,
          ...init.headers,
        },
      })
        .then(async (res) => {
          if (!res.ok) {
            const error = await narrowHttpError(new HttpError(res));
            if (isRetryableError(error) && triesRemaining) {
              await new Promise((resolve) =>
                setTimeout(resolve, getRetryDuration(error, triesRemaining))
              );
              return startFetch(triesRemaining - 1);
            }
            throw error;
          }
          return res;
        })
        .catch((err) => {
          if (err instanceof TypeError) {
            logger.error(`Failed to establish connection to ${input}`);
          }

          if (err instanceof UnauthorizedError) {
            // sessionContext.handleInvalidSession();
            return new Promise(() => {});
          }

          throw err;
        });
    };

    return startFetch(MAX_RETRIES);
  },
});
