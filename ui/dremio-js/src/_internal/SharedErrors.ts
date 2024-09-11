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

import type { Problem } from "../interfaces/Problem.js";

export const tokenInvalidError = {
  title:
    "The provided authentication token was rejected because it was invalid or may have expired.",
  type: "https://api.dremio.dev/problems/auth/token-invalid",
} as const satisfies Problem;

export const systemError = {
  title:
    "An unexpected error occurred while processing your request. Please contact Dremio support if this problem persists.",
  type: "https://api.dremio.dev/problems/system-error",
} as const satisfies Problem;

export const networkError = {
  title:
    "A network error occurred while processing your request. Please ensure that you have a working connection to Dremio.",
  type: "https://api.dremio.dev/problems/network-error",
} as const satisfies Problem;

export type UnexpectedError = {
  type: "https://api.dremio.dev/problems/unexpected-error";
  title: "An unexpected error occurred while processing this request.";
  detail: string;
};

export type SharedErrors =
  | typeof tokenInvalidError
  | typeof systemError
  | typeof networkError
  | UnexpectedError;
