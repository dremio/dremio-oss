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

import { type UnexpectedError } from "../_internal/SharedErrors.js";

const extractResponseBody = async (
  res: Response,
): Promise<string | unknown> => {
  if (res.headers.get("content-type")?.includes("application/json")) {
    const result = await res.json();
    if (
      typeof result === "object" &&
      Object.prototype.hasOwnProperty.call(result, "errorMessage")
    ) {
      return {
        detail: result.errorMessage,
        title: "An unexpected error occurred while processing this request.",
        type: "https://api.dremio.dev/problems/unexpected-error",
      } satisfies UnexpectedError;
    }
    return result;
  }

  return res.text();
};

/**
 * @hidden
 * @internal
 */
export class HttpError {
  constructor(
    public readonly status: number,
    public readonly body: unknown,
  ) {}

  static async fromResponse(res: Response) {
    return new HttpError(res.status, await extractResponseBody(res));
  }
}
