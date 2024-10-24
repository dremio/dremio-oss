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

import { useCallback, useEffect, useState } from "react";
import { usePromise } from "react-smart-promise";

import { FetchOption, V2BetaApi } from "#oss/services/nessie/client";
import { Reference } from "#oss/types/nessie";

export type RepoViewContextType = {
  defaultRef: Reference;
  allRefs: Reference[];
  allRefsErr: any;
  allRefsStatus: any;
  setDefaultRef: React.Dispatch<React.SetStateAction<Reference>>;
  setAllRefs: React.Dispatch<React.SetStateAction<Reference[]>>;
};

export function useRepoViewContext(api: V2BetaApi): RepoViewContextType {
  const [allRefsErr, allBranches, allRefsStatus] = usePromise(
    useCallback(
      () => api.getAllReferencesV2({ fetch: FetchOption.All }),
      [api],
    ),
  );

  const [defaultRef, setDefaultRef] = useState({} as Reference);

  const refs = allBranches ? allBranches.references : [];
  const [allRefs, setAllRefs] = useState(refs as Reference[]);

  useEffect(() => {
    const resultRefs = allBranches ? allBranches.references.reverse() : [];
    setAllRefs(resultRefs as Reference[]);
  }, [allBranches]);

  return {
    defaultRef,
    allRefs,
    setDefaultRef,
    setAllRefs,
    allRefsErr,
    allRefsStatus,
  };
}
