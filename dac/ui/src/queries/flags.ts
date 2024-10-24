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

import { dremio } from "#oss/dremio";
import { queryOptions } from "@tanstack/react-query";

export type SupportKeyState = {
  id: string;
} & (
  | {
      type: "BOOLEAN";
      value: boolean;
    }
  | { type: "INTEGER"; value: number }
  | { type: "TEXT"; value: string }
);

const getTypeString = (v: string | number | boolean) => {
  switch (typeof v) {
    case "number":
      return "INTEGER";
    case "string":
      return "TEXT";
    case "boolean":
    default:
      return "BOOLEAN";
  }
};

export const getDefaultSupportKey = (
  defaultValue: string | number | boolean,
) => ({
  id: supportKey,
  type: getTypeString(defaultValue),
  value: defaultValue,
});

export const supportKey =
  (_pid: string | undefined) =>
  (supportKey: string, defaultValue?: string | number | boolean) =>
    queryOptions({
      queryKey: ["support-key", supportKey],
      queryFn: ({ signal }): Promise<SupportKeyState> =>
        dremio
          ._sonarV2Request(`settings/${supportKey}`, { signal })
          .then((res) => res.json())
          .catch((e) => {
            if (typeof defaultValue !== "undefined") {
              return getDefaultSupportKey(defaultValue);
            } else {
              throw e;
            }
          }),
      retry: false,
      staleTime: Infinity,
    });
