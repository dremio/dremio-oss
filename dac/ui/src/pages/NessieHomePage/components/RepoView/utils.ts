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

import { useCallback, useEffect, useState } from 'react';
import { usePromise } from 'react-smart-promise';

import { DefaultApi, FetchOption, Reference } from '@app/services/nessie/client';

export type RepoViewContextType = {
  defaultRef: Reference;
  allRefs: Reference[];
  allRefsErr: any;
  allRefsStatus: any;
  setDefaultRef: React.Dispatch<React.SetStateAction<Reference>>;
  setAllRefs: React.Dispatch<React.SetStateAction<Reference[]>>;
};

export function useRepoViewContext(api: DefaultApi): RepoViewContextType {
  const [allRefsErr, allBranches, allRefsStatus] = usePromise(
    useCallback(() => api.getAllReferences({ fetch: FetchOption.All }), [api])
  );

  const [defaultRef, setDefaultRef] = useState({} as Reference);

  const [allRefs, setAllRefs] = useState(
    allBranches ? allBranches.references : []
  );

  useEffect(() => {
    setAllRefs(allBranches ? allBranches.references.reverse() : []);
  }, [allBranches]);

  return {
    defaultRef,
    allRefs,
    setDefaultRef,
    setAllRefs,
    allRefsErr,
    allRefsStatus
  };
}
