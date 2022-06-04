

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
import FontIcon from '@app/components/Icon/FontIcon';
import CommitBrowser from '@app/pages/HomePage/components/BranchPicker/components/CommitBrowser/CommitBrowser';
import { LogEntry, LogResponse } from '@app/services/nessie/client';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useIntl } from 'react-intl';
import { oc } from 'ts-optchain';
import { useNessieContext } from '../../utils/context';
import CommitDetails from '../CommitDetails/CommitDetails';
import { NamespaceIcon } from '../NamespaceItem/NamespaceItem';
import PageBreadcrumbHeader from '../PageBreadcrumbHeader/PageBreadcrumbHeader';

import './TableDetailsPage.less';

function TableDetailsPage({ params }: { params: any }) {
  const { state: { reference }, api } = useNessieContext();
  const intl = useIntl();
  const [fullPath, namespace, tableName] = useMemo(() => {
    const full = oc(params).id('');
    const split: string[] = full.split('.');
    const tName = split.pop() || '';
    return [full, split.join('.'), tName];
  }, [params]);

  const [commit, setCommit] = useState<LogEntry>();
  const [list, setList] = useState<LogResponse | undefined>();
  const count = useRef(0);

  useEffect(() => {
    const entries = oc(list).logEntries([]);
    if (count.current !== 1 || !entries.length) return;
    setCommit(entries[0]); // Set current commit to first in list on load
  }, [list]);

  const onDataChange = useCallback(
    function(arg) {
      count.current++;
      setList(arg);
    },
    []
  );

  const commitMeta = oc(commit).commitMeta();

  return (
    <div className='tableDetailsPage'>
      <PageBreadcrumbHeader />
      <div className='tableDetailsPage-header'>
        <span className='tableDetailsPage-tableHeader'>
          <NamespaceIcon type='ICEBERG_TABLE' />
          <div className='tableDetailsPage-tableFullName'>
            <div className='tableDetailsPage-tableName text-ellipsis' title={tableName}>
              {tableName}
            </div>
            <div className='tableDetailsPage-tableNamespace text-ellipsis' title={namespace}>
              {namespace}
            </div>
          </div>
        </span>
        <span className='tableDetailsPage-tabs'>
          <span className='tableDetailsPage-historyTab'>
            <FontIcon type='Clock' />
            {intl.formatMessage({ id: 'Common.History' })}
          </span>
        </span>
      </div>
      <div className='tableDetailsPage-content'>
        <span className='tableDetailsPage-commits'>
          {!!reference && (
            <CommitBrowser
              pageSize={25}
              namespace={fullPath}
              hasSearch={false}
              branch={reference}
              onDataChange={onDataChange}
              selectedHash={oc(commit).commitMeta.hash()}
              onClick={setCommit}
              api={api}
            />
          )}
        </span>
        {commitMeta && reference && (
          <CommitDetails branch={reference.name} commitMeta={commitMeta} />
        )}
      </div>
    </div>
  );
}

export default TableDetailsPage;
