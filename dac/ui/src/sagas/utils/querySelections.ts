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

import { QueryRange } from "@app/utils/statements/statement";
import sentryUtil from "@app/utils/sentryUtil";

export const QUERY_SELECTIONS = "querySelections";

export const getQuerySelectionsFromStorage = (): Partial<{
  [key: string]: QueryRange[];
}> => {
  const querySelections = localStorage.getItem(QUERY_SELECTIONS) ?? "{}";

  try {
    return JSON.parse(querySelections);
  } catch (e) {
    sentryUtil.logException(e);
    return {};
  }
};

export const setQuerySelectionsInStorage = (
  scriptId: string,
  queryRanges: QueryRange[],
) => {
  const allQuerySelections = getQuerySelectionsFromStorage();

  localStorage.setItem(
    QUERY_SELECTIONS,
    JSON.stringify({
      ...allQuerySelections,
      [scriptId]: queryRanges,
    }),
  );
};

export const updateQuerySelectionsInStorage = (
  scriptId: string,
  queryRange: QueryRange,
) => {
  const allQuerySelections = getQuerySelectionsFromStorage();
  const curScriptSelections = allQuerySelections[scriptId] ?? [];

  localStorage.setItem(
    QUERY_SELECTIONS,
    JSON.stringify({
      ...allQuerySelections,
      [scriptId]: [...curScriptSelections, queryRange],
    }),
  );
};

export const deleteQuerySelectionsFromStorage = (scriptId: string) => {
  const allQuerySelections = getQuerySelectionsFromStorage();
  delete allQuerySelections[scriptId];

  localStorage.setItem(QUERY_SELECTIONS, JSON.stringify(allQuerySelections));
};
