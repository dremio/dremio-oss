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
import { useCallback, useMemo, useRef, useState } from 'react';
import { useIntl } from 'react-intl';
import { usePromise } from 'react-smart-promise';
import { AutoSizer, List } from 'react-virtualized';
import { MenuItem } from '@material-ui/core';

import { useNessieContext } from '@app/pages/NessieHomePage/utils/context';
import { Reference } from '@app/services/nessie/client';
import { SearchField } from 'components/Fields';
import RefIcon from '../RefIcon/RefIcon';

import './BranchList.less';

const LIST_ITEM_HEIGHT = 32;

function isHeader(el: any) {
  return el.isHeader === true;
}

type BranchListProps = {
    onClick?: (reference: Reference) => void;
    currentReference: Reference;
    defaultReference: Reference;
}

function BranchList({ onClick, currentReference, defaultReference }: BranchListProps) {
  const intl = useIntl();
  const ref = useRef(null);
  const { api } = useNessieContext();

  const [, data] = usePromise(useCallback(() => api.getAllReferences({ maxRecords: 1000000 }), [api]));
  const branchList = useMemo(
    () => {
      if (!data) return [];
      const branches = data.references;
      return [
        {
          isHeader: true,
          name: intl.formatMessage({ id: 'Nessie.DefaultBranchHeader' })
        },
        defaultReference,
        {
          isHeader: true,
          name: intl.formatMessage({ id: 'Nessie.AllBranchesHeader' })
        },
        ...branches.filter(b => b.name !== defaultReference.name)
      ];
    }, [data, defaultReference, intl]);

  const [search, setSearch] = useState('');
  const filteredList = useMemo(() => {
    return !search ?
      branchList :
      branchList.filter(cur => {
        return isHeader(cur) || cur.name.toLowerCase().includes(search.trim());
      });
  }, [search, branchList]);

  function renderRow({ index, key, style }: any) {
    const cur = filteredList[index] as Reference;
    return (
      <div key={key} style={style}>
        {isHeader(cur) ? (
          <MenuItem disabled className='branchList-header-item'>
            <span className='text-ellipsis'>
              {cur.name}
            </span>
          </MenuItem>
        ) : (
          <MenuItem
            {...onClick && { onClick: () => onClick(cur as Reference) }}
            data-testid={`branch-${cur.name}`}
            className='branchList-item'
            selected={cur.name === currentReference.name}
            title={cur.name}
          >
            <span className='branchList-item-icon'>
              <RefIcon reference={cur} style={{ width: 20, height: 20 }} />
            </span>
            <span className='text-ellipsis'>
              {cur.name}
            </span>
          </MenuItem>
        )}
      </div>
    );
  }

  return (
    <div className='branchList'>
      <div className='branchList-search'>
        <SearchField showIcon onChange={setSearch} placeholder={intl.formatMessage({ id: 'Nessie.BranchSearchPlaceholder' })} />
      </div>
      <div className='branchList-listContainer'>
        <AutoSizer>
          {({ height, width }) => (
            <List
              ref={ref}
              rowRenderer={renderRow}
              rowCount={filteredList.length}
              rowHeight={LIST_ITEM_HEIGHT}
              height={height || 600}
              width={width || 150}
            />
          )}
        </AutoSizer>
      </div>
    </div>
  );
}

export default BranchList;
