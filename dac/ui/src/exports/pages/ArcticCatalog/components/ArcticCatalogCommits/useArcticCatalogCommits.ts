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

import { useEffect, useReducer, useRef } from "react";
import { SmartResource } from "smart-resource";
import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { LogEntry } from "@app/services/nessie/client/index";

const newArrayReducer = (state: LogEntry[], action: any): LogEntry[] => {
  if (action.type === "reset") {
    return [];
  } else if (action.type === "nextPage") {
    return [...state, ...action.payload];
  } else {
    return state;
  }
};

export const useArcticCatalogCommits = ({
  filter,
  branch,
  pageSize = 50,
  pageToken,
}: {
  filter: string;
  branch?: string;
  pageSize?: number;
  pageToken?: string;
}) => {
  const { api } = useNessieContext();

  const ArcticCatalogCommitsResource = useRef(
    new SmartResource(({ name, search, token }) =>
      api.getCommitLog({
        ref: name,
        filter: search
          ? `commit.author.contains('${search}') || \
          commit.hash.contains('${search}')`
          : undefined,
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
        token: pageToken,
      });
    }
  }, [branch, filter, pageToken]);

  const [tags, setNextPage] = useReducer(newArrayReducer, []);
  const [next, error] = useResourceSnapshot(
    ArcticCatalogCommitsResource.current
  );

  useEffect(() => {
    setNextPage({ type: "reset" });
  }, [branch, filter]);

  useEffect(() => {
    if (next?.logEntries)
      setNextPage({ type: "nextPage", payload: next.logEntries });
  }, [next?.logEntries]);

  return [
    { logEntries: tags, pageToken: next?.token },
    error,
    useResourceStatus(ArcticCatalogCommitsResource.current),
  ] as const;
};
