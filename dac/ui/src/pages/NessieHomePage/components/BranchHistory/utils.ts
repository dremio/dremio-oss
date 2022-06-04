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

import {
  DefaultApi,
  FetchOption,
  LogResponse,
  Reference
} from '@app/services/nessie/client';

export type BranchHistoryContextType = {
  currentRef: Reference;
  currentRefStatus: any;
  currentRefErr: any;
  commitLog: LogResponse;
  setCommitLog: React.Dispatch<React.SetStateAction<LogResponse>>;
  defaultRef: Reference;
  setDefaultRef: React.Dispatch<React.SetStateAction<Reference>>;
  commitHistoryErr: any;
  commitHistoryStatus: any;
};

export function useBranchHistoryContext(
  branchName: string,
  api: DefaultApi
): BranchHistoryContextType {
  const [currentRefErr, currentBranch, currentRefStatus] = usePromise(
    useCallback(
      () => api.getReferenceByName({ ref: branchName, fetch: FetchOption.All }),
      [branchName, api]
    )
  );

  const [commitHistoryErr, commitHistory, commitHistoryStatus] = usePromise(
    useCallback(
      () => api.getCommitLog({ ref: branchName, maxRecords: 100 }),
      [branchName, api]
    )
  );

  const [commitLog, setCommitLog] = useState({} as LogResponse);
  const [defaultRef, setDefaultRef] = useState({} as Reference);

  useEffect(() => {
    setCommitLog(commitHistory ? commitHistory : ({} as LogResponse));
  }, [commitHistory, setCommitLog]);

  return {
    currentRef: currentBranch ? currentBranch : ({} as Reference),
    currentRefStatus,
    currentRefErr,
    commitLog,
    setCommitLog,
    defaultRef,
    setDefaultRef,
    commitHistoryErr,
    commitHistoryStatus
  };
}
