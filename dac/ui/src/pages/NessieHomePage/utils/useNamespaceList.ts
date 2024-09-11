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
import { EntryV2, V2BetaApi } from "@app/services/nessie/client";
import { SmartResource } from "smart-resource";
import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";

const generateFilter = (search?: string) => {
  const clauses: string[] = [];
  if (search) {
    const searchClause: string[] = [];
    searchClause.push(`entry.name.contains('${search}')`);
    clauses.push(`(${searchClause.join(" || ")})`);
  }

  return clauses.join(" && ");
};

function formatQuery(path: string[] = [], filter?: string) {
  const generatedFilter = generateFilter(filter);
  let appendFilter = "";

  if (generatedFilter) {
    appendFilter = `&& ${generatedFilter}`;
  }

  if (!path?.length)
    return `size(entry.keyElements) == ${path.length + 1}${appendFilter}`;
  return `entry.encodedKey.startsWith('${
    path.map((c) => decodeURIComponent(c)).join(".") + "."
  }') && size(entry.keyElements) == ${path.length + 1}${appendFilter}`;
}

type NamespaceState = {
  entries: EntryV2[] | null;
  key?: string;
};

const getInitialState = (key: string) => ({
  entries: null,
  key,
});

const namespaceReducer = (
  state: NamespaceState,
  action: any,
): NamespaceState => {
  if (action.type === "reset") {
    return {
      key: "",
      entries: null,
    };
  }
  if (action.type === "nextPage") {
    const { key, entries } = action.payload;
    return {
      ...state,
      key,
      entries:
        key === state.key ? [...(state.entries || []), ...entries] : entries,
    };
  } else {
    return state;
  }
};

const NamespaceDataResource = new SmartResource(
  ({ hash, path, reference, pageToken, filter, api }) =>
    api.getEntriesV2({
      ref: hash ? `${reference}@${hash}` : reference,
      filter: formatQuery(path, filter),
      maxRecords: 50,
      pageToken: pageToken,
    }),
);

function useNamespaceList({
  reference,
  hash,
  path,
  api,
  pageToken,
  filter,
}: {
  reference: string;
  hash?: string | null;
  path?: string[];
  api: V2BetaApi;
  pageToken?: string;
  filter?: string;
}) {
  useEffect(() => {
    NamespaceDataResource.reset();
    setNamespaceState({ type: "reset" });
    return () => {
      NamespaceDataResource.reset();
      setNamespaceState({ type: "reset" });
    };
  }, [path]);

  useEffect(() => {
    if (reference) {
      NamespaceDataResource.fetch({
        reference,
        hash,
        path,
        pageToken,
        filter,
        api,
      });
    }
  }, [reference, hash, path, pageToken, filter, api]);

  const key = useRef("");
  key.current = `${reference}-${hash}-${filter}-${path?.join(".")}`;

  const [namespaceState, setNamespaceState] = useReducer(
    namespaceReducer,
    getInitialState(key.current),
  );

  const [next, error] = useResourceSnapshot(NamespaceDataResource);

  useEffect(() => {
    if (!next?.entries) return;
    setNamespaceState({
      type: "nextPage",
      payload: { entries: next.entries, key: key.current },
    });
  }, [next?.entries]);

  return [
    { entries: namespaceState.entries, pageToken: next?.token },
    error,
    useResourceStatus(NamespaceDataResource),
  ] as const;
}

export default useNamespaceList;
