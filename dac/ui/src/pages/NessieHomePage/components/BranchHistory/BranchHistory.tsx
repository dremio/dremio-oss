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

import { createContext, useEffect, useMemo } from 'react';
import { FormattedMessage } from 'react-intl';

import { oc } from 'ts-optchain';
import { Reference } from '@app/services/nessie/client';
import { isDefaultReferenceLoading } from '@app/selectors/nessie/nessie';
import PromiseViewState from '@app/components/PromiseViewState/PromiseViewState';
import { isReqLoading } from '@app/utils/smartPromise';
import { useNessieContext } from '../../utils/context';
import BranchHistoryCommits from './components/BranchHistoryCommits/BranchHistoryCommits';
import BranchHistoryHeader from './components/BranchHistoryHeader/BranchHistoryHeader';
import BranchHistoryMetadata from './components/BranchHistoryMetadata/BranchHistoryMetadata';

import { BranchHistoryContextType, useBranchHistoryContext } from './utils';

import './BranchHistory.less';

type BranchHistoryProps = {
  params: any;
  repo: string;
  defaultReference: Reference | null;
  defaultReferenceLoading: boolean;
};

export const BranchHistoryContext = createContext(
  {} as BranchHistoryContextType
);

function BranchHistory({
  params
}: BranchHistoryProps) {
  const branchName: string = useMemo(() => {
    return oc(params).branchName('').split('.')[0];
  }, [params]);

  const { state, api } = useNessieContext();
  const { defaultReference } = state;
  const defaultReferenceLoading = isDefaultReferenceLoading(state);
  const branchHistoryContext = useBranchHistoryContext(branchName, api);
  const { currentRefErr: err, currentRefStatus: status, setDefaultRef } = branchHistoryContext;

  useEffect(() => {
    defaultReference && !defaultReferenceLoading
      ? setDefaultRef({
        type: 'BRANCH',
        name: defaultReference.name,
        hash: defaultReference.hash
      } as Reference)
      : setDefaultRef({} as Reference);
  }, [defaultReference, defaultReferenceLoading, setDefaultRef]);

  return (
    <BranchHistoryContext.Provider value={branchHistoryContext}>
      {isReqLoading(status) ? (
        <div className='branch-loading'>
          <PromiseViewState error={err} status={status} />
        </div>
      ) : Object.keys(branchHistoryContext.currentRef).length &&
        branchHistoryContext.currentRef.type === 'BRANCH' ? (
          <div className='branch-history'>
            <div className='branch-history-header-div'>
              <BranchHistoryHeader />
            </div>
            <div className='branch-history-metadata-div'>
              <BranchHistoryMetadata />
            </div>
            <div className='branch-history-commits-div'>
              <BranchHistoryCommits />
            </div>
          </div>
        ) : (
          <div className='branch-dne'>
            <span className='branch-dne-message'>
              <FormattedMessage
                id='BranchHistory.DoesNotExist'
                values={{ branchName: <strong>{branchName}</strong> }}
              />
            </span>
          </div>
        )}
    </BranchHistoryContext.Provider>
  );
}

export default BranchHistory;
