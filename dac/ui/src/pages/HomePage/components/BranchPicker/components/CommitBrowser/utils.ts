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
import { LogResponseV2 as LogResponse } from "#oss/services/nessie/client";

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
  action: ActionTypes,
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
  path: string[] | undefined,
  tableName: string | undefined,
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

  const opClauses = [];
  if (path?.length) {
    const namespace = path
      .map((c) =>
        decodeURIComponent(c)
          //Hyphenated paths are surrounded by double-quotes on SQL runner page, remove the quotes
          .replaceAll('"', ""),
      )
      .join(".");
    opClauses.push(`op.namespace == '${namespace}'`);
  } else {
    //Only add this filter clause if tableName is provided since we're looking for a specific one
    if (tableName) opClauses.push("size(op.namespaceElements) == 0");
  }

  if (tableName) {
    opClauses.push(
      `op.name == '${decodeURIComponent(tableName).replace(/"/g, "")}'`,
    );
  }

  if (opClauses.length) {
    clauses.push(`operations.exists(op, ${opClauses.join(" && ")})`);
  }

  return clauses.join(" || ");
}
