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
import moize from "moize";
import { getEntries } from "@app/services/nessie/impl/TreeApi";
import { useMemo } from "react";
import { usePromise } from "react-smart-promise";

const QUERY_POSTFIX = "(\\\\.|$)";

const memoGetEntries = moize(getEntries, {
  maxSize: 1,
  isPromise: true,
  isDeepEqual: true,
});

function formatQuery(path: string[] = []) {
  return `entry.namespace.matches('${
    path.map((c) => decodeURIComponent(c)).join("\\\\.") + QUERY_POSTFIX
  }')`;
}

function useNamespaceList({
  reference,
  hash: hashOnRef,
  path,
}: {
  reference: string;
  hash?: string | null;
  path?: string[];
}) {
  return usePromise(
    useMemo(
      () =>
        !reference
          ? null
          : () =>
              memoGetEntries({
                ref: reference,
                ...(hashOnRef && { hashOnRef }),
                namespaceDepth: path ? path.length + 1 : 1,
                filter: formatQuery(path),
              }),
      [reference, hashOnRef, path]
    )
  );
}

export default useNamespaceList;
