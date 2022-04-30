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

import { useContext, useMemo } from 'react';
import { FormattedMessage } from 'react-intl';

import { useNessieContext } from '@app/pages/NessieHomePage/utils/context';
import StatefulTableViewer from '@app/components/StatefulTableViewer';
import { getViewStateFromReq } from '@app/utils/smartPromise';

import { BranchHistoryContext } from '../../BranchHistory';
import { columns, createTableData } from './utils';

import './BranchHistoryCommits.less';

function BranchHistoryCommits() {
  const {
    commitLog,
    currentRef,
    setCommitLog,
    commitHistoryStatus: status,
    commitHistoryErr: err
  } = useContext(BranchHistoryContext);
  const { api } = useNessieContext();

  const tableData = useMemo(
    () => createTableData(currentRef.name, commitLog, setCommitLog, api),
    [currentRef.name, commitLog, setCommitLog, api]
  );

  return (
    <div className='branch-history-commits'>
      <div className='branch-history-commits-header'>
        <FormattedMessage id='Common.History' />
      </div>
      <div className='branch-history-commits'>
        <StatefulTableViewer
          virtualized
          disableZebraStripes
          columns={columns}
          tableData={tableData}
          rowHeight={39}
          viewState={getViewStateFromReq(err, status)}
        />
      </div>
    </div>
  );
}

export default BranchHistoryCommits;
