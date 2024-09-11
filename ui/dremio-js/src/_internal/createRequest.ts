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

import type { Config, RequestFn } from "./types/Config.js";
import { HttpError } from "../common/HttpError.js";
import {
  networkError,
  systemError,
  tokenInvalidError,
} from "./SharedErrors.js";

const getTokenFromConfig = (token: string | (() => string)): string => {
  if (typeof token === "string") {
    return token;
  }

  return token();
};

const getHeadersFromConfig = (config: Config): RequestInit["headers"] => {
  return {
    ...(config.token && {
      Authorization: `Bearer ${getTokenFromConfig(config.token)}`,
    }),
  };
};

/**
 * @internal
 * @hidden
 */
export const createRequest = (config: Config): RequestFn => {
  const fetch = (config.fetch as typeof globalThis.fetch) || globalThis.fetch;
  return (path, init) => {
    const start = performance.now();
    return fetch(new URL(path, config.origin), {
      ...init,
      headers: { ...getHeadersFromConfig(config), ...init?.headers },
    })
      .then(async (res) => {
        config.logger?.debug(
          `${init?.method?.toUpperCase() || "GET"} ${path} (${res.status}) [${Math.round(performance.now() - start)}ms]`,
        );
        if (!res.ok) {
          switch (res.status) {
            case 401:
              throw new (Error as any)(tokenInvalidError.title, {
                cause: tokenInvalidError,
              });

            case 500:
            case 502:
            case 503:
            case 504:
              throw new (Error as any)(systemError.title, {
                cause: systemError,
              });

            default:
              throw await HttpError.fromResponse(res);
          }
        }
        return res;
      })
      .catch((err) => {
        if (err instanceof TypeError) {
          throw new (Error as any)(networkError.title, {
            cause: { ...networkError, additionalDetails: err },
          });
        } else {
          throw err;
        }
      });
  };
};
