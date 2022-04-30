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

import { useState } from 'react';
import { FormattedMessage } from 'react-intl';

import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText
} from '@material-ui/core';

import {
  Merge,
  MergeRefIntoBranchRequest,
  Reference
} from '@app/services/nessie/client';
import { CustomDialogTitle } from '../NewBranchDialog/utils';
import { useNessieContext } from '../../utils/context';

import './MergeBranchDialog.less';

type MergeBranchDialogProps = {
  open: boolean;
  mergeFrom: Reference;
  mergeTo: Reference;
  closeDialog: () => void;
  setSuccessMessage?: React.Dispatch<React.SetStateAction<JSX.Element | null>>;
};

function MergeBranchDialog({
  open,
  mergeFrom,
  mergeTo,
  closeDialog,
  setSuccessMessage
}: MergeBranchDialogProps): JSX.Element {
  const [isSending, setIsSending] = useState(false);
  const [errorText, setErrorText] = useState<JSX.Element | null>(null);
  const { api } = useNessieContext();

  const onMerge = async () => {
    setIsSending(true);

    try {
      await api.mergeRefIntoBranch({
        branchName: mergeTo.name,
        expectedHash: mergeTo.hash,
        merge: {
          fromRefName: mergeFrom.name,
          fromHash: mergeFrom.hash as string
        } as Merge
      } as MergeRefIntoBranchRequest);

      if (setSuccessMessage) {
        setSuccessMessage(
          <FormattedMessage id='BranchHistory.Dialog.MergeBranch.Success' />
        );
      }

      setErrorText(null);
      closeDialog();
      setIsSending(false);
    } catch (error: any) {
      if (error.statusText === 'Conflict') {
        setErrorText(
          <FormattedMessage id='BranchHistory.Dialog.MergeBranch.Error.Conflict' />
        );
      } else if (error.statusText === 'Bad Request') {
        setErrorText(
          <FormattedMessage id='BranchHistory.Dialog.MergeBranch.Error.NoHashes' />
        );
      } else {
        setErrorText(
          <FormattedMessage id='RepoView.Dialog.DeleteBranch.Error' />
        );
      }

      setIsSending(false);
    }
  };

  return (
    <div>
      <Dialog open={open} onClose={closeDialog} className='merge-branch-dialog'>
        <CustomDialogTitle
          onClose={closeDialog}
          className='merge-branch-dialog-header'
        >
          <span className='merge-branch-dialog-header-title'>
            <FormattedMessage
              id='BranchHistory.Dialog.MergeBranch.MergeBranch'
              values={{
                srcBranch: mergeFrom.name,
                dstBranch: mergeTo.name
              }}
            />
          </span>
        </CustomDialogTitle>
        <DialogContent className='merge-branch-dialog-body'>
          <DialogContentText>
            <FormattedMessage id='BranchHistory.Dialog.MergeBranch.ContentText' />
          </DialogContentText>
          <div className='merge-branch-dialog-body-error'>{errorText}</div>
        </DialogContent>
        <DialogActions className='merge-branch-dialog-actions'>
          <Button
            onClick={closeDialog}
            disabled={isSending}
            className='cancel-button'
          >
            <FormattedMessage id='Common.Cancel' />
          </Button>
          <Button
            onClick={onMerge}
            disabled={isSending}
            className='merge-button'
          >
            <FormattedMessage id='BranchHistory.Header.Merge' />
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
}

export default MergeBranchDialog;
