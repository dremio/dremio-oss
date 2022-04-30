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

import { useContext, useState } from 'react';
import { FormattedMessage, useIntl } from 'react-intl';

import Art from '@app/components/Art';
import { useNessieContext } from '@app/pages/NessieHomePage/utils/context';
import { Menu, MenuItem } from '@material-ui/core';
import NewBranchDialog from '../../../NewBranchDialog/NewBranchDialog';
import BranchButton from '../../../BranchButton/BranchButton';
import MergeBranchDialog from '../../../MergeBranchDialog/MergeBranchDialog';
import DeleteBranchDialog from '../../../DeleteBranchDialog/DeleteBranchDialog';
import RenameBranchDialog from '../../../RenameBranchDialog/RenameBranchDialog';

import { BranchHistoryContext } from '../../BranchHistory';
import NessieBreadcrumb from '../../../NessieBreadcrumb/NessieBreadcrumb';
import {
  DialogStatesType,
  manageDialogs,
  redirectOnReferenceActions
} from './utils';

import './BranchHistoryHeader.less';

function BranchHistoryHeader() {
  const { baseUrl } = useNessieContext();
  const { currentRef, defaultRef } = useContext(BranchHistoryContext);
  const [anchorEl, setAnchorEl] = useState(null);
  const [successMessage, setSuccessMessage] = useState<JSX.Element | null>(
    null
  );
  const [dialogStates, setDialogStates] = useState({
    create: false,
    merge: false,
    rename: false,
    delete: false
  } as DialogStatesType);

  const intl = useIntl();

  const openBranchOptions = (event: any) => {
    setAnchorEl(event.currentTarget);
  };

  const closeBranchOptions = () => {
    setAnchorEl(null);
  };

  return (
    <div className='branch-history-header'>
      <span className='branch-history-header-name'>
        <NessieBreadcrumb />
      </span>
      <span className='branch-history-header-success'>{successMessage}</span>
      <BranchButton onClick={() => manageDialogs(dialogStates, setDialogStates, 'create', true)} />
      <NewBranchDialog
        open={dialogStates.create}
        forkFrom={currentRef}
        closeDialog={() =>
          manageDialogs(dialogStates, setDialogStates, 'create', false)
        }
        setSuccessMessage={setSuccessMessage}
      />
      {defaultRef.name !== currentRef.name && (
        <>
          <span className='merge-branch-wrap'>
            <BranchButton
              onClick={() => manageDialogs(dialogStates, setDialogStates, 'merge', true)}
              text={<FormattedMessage id='BranchHistory.Header.Merge' />}
              iconType='GitBranch'
            />
            <MergeBranchDialog
              open={dialogStates.merge}
              mergeFrom={currentRef}
              mergeTo={defaultRef}
              closeDialog={() =>
                manageDialogs(dialogStates, setDialogStates, 'merge', false)
              }
              setSuccessMessage={setSuccessMessage}
            />
          </span>

          <span
            className='branch-history-header-ellipsis'
            onClick={openBranchOptions}
            aria-haspopup='true'
          >
            <Art
              src='Ellipsis.svg'
              alt={intl.formatMessage({ id: 'RepoView.BranchMenu' })}
              style={{ width: 24, height: 24 }}
            />
          </span>
          <Menu
            anchorEl={anchorEl}
            open={Boolean(anchorEl)}
            onClose={closeBranchOptions}
          >
            <MenuItem
              onClick={() => {
                manageDialogs(dialogStates, setDialogStates, 'rename', true);
                closeBranchOptions();
              }}
            >
              <FormattedMessage id='BranchHistory.BranchOptions.RenameBranch' />
            </MenuItem>

            <MenuItem
              onClick={() => {
                manageDialogs(dialogStates, setDialogStates, 'delete', true);
                closeBranchOptions();
              }}
            >
              <FormattedMessage id='RepoView.DeleteBranch' />
            </MenuItem>
          </Menu>
          <RenameBranchDialog
            open={dialogStates.rename}
            referenceToRename={currentRef}
            closeDialog={() =>
              manageDialogs(dialogStates, setDialogStates, 'rename', false)
            }
            handleNameChange={name => redirectOnReferenceActions(name, baseUrl)}
          />
          <DeleteBranchDialog
            open={dialogStates.delete}
            referenceToDelete={currentRef}
            closeDialog={() =>
              manageDialogs(dialogStates, setDialogStates, 'delete', false)
            }
            specialHandling={() => redirectOnReferenceActions('', baseUrl)}
          />
        </>
      )}
    </div>
  );
}

export default BranchHistoryHeader;
