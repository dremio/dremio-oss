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
import { LogResponse } from "@app/services/nessie/client";

type CommitBrowserState = {
  search?: string;
  data?: LogResponse;
  numRows: number;
};

export const initialState = {
  search: "",
  data: undefined,
  numRows: 1,
};

type ActionTypes =
  | { type: "SET_SEARCH"; value?: string }
  | { type: "SET_DATA"; value: LogResponse };

export function CommitBrowserReducer(
  state: CommitBrowserState,
  action: ActionTypes
) {
  switch (action.type) {
    case "SET_SEARCH": {
      return { ...state, search: action.value, numRows: 1, data: undefined };
    }
    case "SET_DATA": {
      const { logEntries, hasMore } = action.value;
      const cur = state.data ? state.data.logEntries : [];
      const newVals = cur.concat(logEntries);
      const numRows = hasMore ? newVals.length + 1 : newVals.length;
      return {
        ...state,
        data: { ...state.data, ...action.value, logEntries: newVals },
        numRows,
      };
    }
    default:
      return state;
  }
}

export function formatQuery(
  search: string | undefined,
  path: string[] | undefined
) {
  let clauses: string[] = [];
  if (search) {
    const q = search.toLowerCase();
    clauses = [
      `commit.author == "${q}"`,
      `commit.hash == "${q}"`,
      `commit.hash.startsWith("${q}")`,
      `commit.author.startsWith("${q}")`,
    ];
  }

  if (path?.length) {
    //TODO Check with nessie team on escaping this namespace
    const namespace = path.map((c) => decodeURIComponent(c)).join(".");
    clauses.push(`operations.exists(op, op.namespace == '${namespace}')`);
  }

  return clauses.join(" || ");
}
