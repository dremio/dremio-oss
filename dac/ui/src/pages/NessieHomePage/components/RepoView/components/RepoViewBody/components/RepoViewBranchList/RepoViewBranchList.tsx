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

import { useContext } from 'react';
import { useEffect, useState } from 'react';
import { FormattedMessage } from 'react-intl';

import { AutoSizer, List } from 'react-virtualized';
import { MenuItem } from '@material-ui/core';
import CommitHash from '@app/pages/HomePage/components/BranchPicker/components/CommitBrowser/components/CommitHash/CommitHash';

import PromiseViewState from '@app/components/PromiseViewState/PromiseViewState';
import { Reference } from '@app/services/nessie/client';
import NessieLink from '@app/pages/NessieHomePage/components/NessieLink/NessieLink';
import { RepoViewContext } from '../../../../RepoView';
import { convertISOString, renderIcons } from './utils';

import './RepoViewBranchList.less';

type RepoViewBranchTableProps = {
  rows: Reference[];
  openCreateDialog: (branch: Reference) => void;
  openDeleteDialog?: (branch: Reference) => void;
  isDefault?: boolean;
};

function RepoViewBranchList({
  rows,
  openCreateDialog,
  openDeleteDialog,
  isDefault
}: RepoViewBranchTableProps) {
  const { allRefsStatus: status, allRefsErr: err } = useContext(RepoViewContext);
  const [rowHover, setRowHover] = useState<boolean[]>(
    new Array(rows.length).fill(false)
  );

  useEffect(() => {
    setRowHover(new Array(rows.length).fill(false));
  }, [rows.length]);

  const renderRow = ({ index, key, style }: any): JSX.Element => {
    const cur = rows[index];

    return (
      <div
        key={key}
        style={style}
        onMouseEnter={() => {
          const copyHover = new Array(rowHover.length).fill(false);
          copyHover[key.split('-')[0]] = true;
          setRowHover(copyHover);
        }}
        onMouseLeave={() => setRowHover(new Array(rowHover.length).fill(false))}
      >
        <MenuItem
          data-testid={`brach-${cur.name}`}
          className='branch-list-item'
          title={cur.name}
        >
          <span className='branch-list-item-content'>
            <NessieLink
              to={`/branches/${cur.name}`}
              title={cur.name}
              className='branch-list-item-name'
            >
              {cur.name}
            </NessieLink>
            {!isDefault &&
              cur.metadata &&
              cur.metadata.commitMetaOfHEAD &&
              cur.metadata.commitMetaOfHEAD.author && (
                <>
                  <span className='branch-list-item-history'>
                    <FormattedMessage
                      id='RepoView.LastUpdate'
                      values={{
                        time: convertISOString(cur)
                      }}
                    />
                  </span>
                  <span className='branch-list-item-author'>
                    {cur.metadata.commitMetaOfHEAD.author}
                  </span>
                </>
            )}
          </span>
          {cur.hash && (
            <span className='branch-list-item-hash'>
              <CommitHash branch={cur.name} hash={cur.hash} />
            </span>
          )}
          {renderIcons(
            cur,
            rowHover[key.split('-')[0]],
            openCreateDialog,
            openDeleteDialog,
            isDefault
          )}
        </MenuItem>
      </div>
    );
  };

  return (
    <div className='branch-list'>
      <div className='branch-list-name'>
        {isDefault ? (
          <FormattedMessage id='RepoView.DefaultBranch' />
        ) : (
          <FormattedMessage id='RepoView.AllBranches' />
        )}
      </div>
      <div className='branch-list-container'>
        <PromiseViewState status={status} error={err} />
        <AutoSizer>
          {({ height }) => (
            <List
              rowRenderer={renderRow}
              rowCount={rows.length}
              rowHeight={39}
              height={height}
              width={1}
            />
          )}
        </AutoSizer>
      </div>
    </div>
  );
}

export default RepoViewBranchList;
