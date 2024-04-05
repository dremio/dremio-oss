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

import { Problem } from "../classes/Problem.js";

export const tokenInvalidError = {
  code: "auth:token_invalid",
  title:
    "The provided authentication token was rejected because it was invalid or may have expired.",
  links: {
    type: "https://docs.dremio.com/current/reference/api/#authentication",
  },
} as const satisfies Problem;

export const systemError = {
  code: "system_error",
  title:
    "An unexpected error occurred while processing your request. Please contact Dremio support if this problem persists.",
} as const satisfies Problem;

export const networkError = {
  code: "network_error",
  title:
    "A network error occurred while processing your request. Please ensure that you have a working connection to Dremio.",
} as const satisfies Problem;

export type SharedErrors = typeof tokenInvalidError;
