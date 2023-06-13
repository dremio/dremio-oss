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

import { useEffect, useMemo, useReducer, useRef } from "react";
import { SmartResource } from "smart-resource";
import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { useArcticCatalogContext } from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { FetchOption, LogEntryV2 } from "@app/services/nessie/client/index";

type CommitsState = {
  logEntries: LogEntryV2[] | null;
  key?: string;
};

const getInitialState = (key: string) => ({
  logEntries: null,
  key,
});

const commitsReducer = (state: CommitsState, action: any): CommitsState => {
  if (action.type === "nextPage") {
    const { key, logEntries } = action.payload;
    return {
      ...state,
      key,
      logEntries:
        key === state.key
          ? [...(state.logEntries || []), ...logEntries]
          : logEntries,
    };
  } else {
    return state;
  }
};

export const useArcticCatalogCommits = ({
  filter,
  branch,
  hash,
  pageSize = 50,
  pageToken,
}: {
  filter: string;
  branch?: string;
  pageSize?: number;
  pageToken?: string;
  hash?: string | null;
}) => {
  const { apiV2 } = useNessieContext();
  const { reservedNamespace } = useArcticCatalogContext() ?? {};

  const extractedPath = useMemo(
    () => reservedNamespace?.split("/")?.slice(1) ?? [],
    [reservedNamespace]
  );

  const genFilter = (path: string[], search: string) => {
    const clauses: string[] = [];

    if (path.length) {
      const namespace = path
        .map((folder) => decodeURIComponent(folder))
        .join(".");

      clauses.push(`operations.exists(op, op.namespace == '${namespace}')`);
    }

    if (search) {
      const searchClause: string[] = [];
      const searchFilter = search.toLowerCase();

      searchClause.push(
        `commit.author.contains('${searchFilter}')`,
        `commit.hash.contains('${searchFilter}')`
      );

      clauses.push(`(${searchClause.join(" || ")})`);
    }

    return clauses.join(" && ");
  };

  const ArcticCatalogCommitsResource = useRef(
    new SmartResource(({ name, hash, search, path, token }) =>
      apiV2.getCommitLogV2({
        ref: hash ? `${name}@${hash}` : name,
        fetch: path.length ? FetchOption.All : FetchOption.Minimal,
        filter: genFilter(path, search),
        maxRecords: pageSize,
        pageToken: token,
      })
    )
  );

  useEffect(() => {
    if (branch) {
      ArcticCatalogCommitsResource.current.fetch({
        name: branch,
        search: filter,
        path: extractedPath,
        hash,
        token: pageToken,
      });
    }
  }, [branch, hash, filter, extractedPath, pageToken]);

  const key = useRef("");
  key.current = `${branch}-${hash}-${filter}-${reservedNamespace}`;

  const [commitState, setCommitState] = useReducer(
    commitsReducer,
    getInitialState(key.current)
  );
  const [next, error] = useResourceSnapshot(
    ArcticCatalogCommitsResource.current
  );

  useEffect(() => {
    if (!next?.logEntries) return;
    setCommitState({
      type: "nextPage",
      payload: { logEntries: next.logEntries, key: key.current },
    });
  }, [next?.logEntries]);

  return [
    { logEntries: commitState.logEntries, pageToken: next?.token },
    error,
    useResourceStatus(ArcticCatalogCommitsResource.current),
  ] as const;
};
