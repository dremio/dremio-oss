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
import { connect } from 'react-redux';

import { setReference as setReferenceAction } from '@app/actions/nessie/nessie';
import { Reference } from '@app/services/nessie/client';
import { CustomDialogTitle } from '../NewBranchDialog/utils';
import { useNessieContext } from '../../utils/context';

import './DeleteBranchDialog.less';

type DeleteBranchDialogProps = {
  open: boolean;
  referenceToDelete: Reference;
  closeDialog: () => void;
  allRefs?: Reference[];
  setAllRefs?: React.Dispatch<React.SetStateAction<Reference[]>>;
  specialHandling?: () => void;
};

type ConnectedProps = {
  setReference: typeof setReferenceAction;
}

function DeleteBranchDialog({
  open,
  referenceToDelete,
  closeDialog,
  allRefs,
  setAllRefs,
  specialHandling,
  setReference
}: DeleteBranchDialogProps & ConnectedProps): JSX.Element {
  const [isSending, setIsSending] = useState(false);
  const [errorText, setErrorText] = useState<JSX.Element | null>(null);
  const {
    api,
    stateKey,
    state: {
      reference,
      defaultReference
    }
  } = useNessieContext();

  const onDelete = async () => {
    setIsSending(true);

    try {
      await api.deleteBranch({
        branchName: referenceToDelete.name,
        expectedHash: referenceToDelete.hash
      });

      if (allRefs && setAllRefs) {
        const indexToRemove = allRefs.findIndex(
          (ref: Reference) => ref.name === referenceToDelete.name
        );

        setAllRefs([
          ...allRefs.slice(0, indexToRemove),
          ...allRefs.slice(indexToRemove + 1)
        ]);
      }

      if (specialHandling) specialHandling();

      setErrorText(null);

      if (reference && referenceToDelete.name === reference.name) {
        setReference({ reference: defaultReference }, stateKey);
      }

      closeDialog();
      setIsSending(false);
    } catch (error: any) {
      setErrorText(
        <FormattedMessage id='RepoView.Dialog.DeleteBranch.Error' />
      );
      setIsSending(false);
    }
  };

  return (
    <div>
      <Dialog
        open={open}
        onClose={closeDialog}
        className='delete-branch-dialog'
      >
        <CustomDialogTitle
          onClose={closeDialog}
          className='delete-branch-dialog-header'
        >
          <span className='delete-branch-dialog-header-title'>
            <FormattedMessage
              id='RepoView.Dialog.DeleteBranch.DeleteBranch'
              values={{ branchName: referenceToDelete.name }}
            />
          </span>
        </CustomDialogTitle>
        <DialogContent className='delete-branch-dialog-body'>
          <DialogContentText>
            <FormattedMessage id='RepoView.Dialog.DeleteBranch.Confirmation' />
          </DialogContentText>
          <div className='delete-branch-dialog-body-error'>{errorText}</div>
        </DialogContent>
        <DialogActions className='delete-branch-dialog-actions'>
          <Button
            onClick={closeDialog}
            disabled={isSending}
            className='cancel-button'
          >
            <FormattedMessage id='Common.Cancel' />
          </Button>
          <Button
            onClick={onDelete}
            disabled={isSending}
            className='delete-button'
          >
            <FormattedMessage id='Common.Delete' />
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
}

const mapDispatchToProps = { setReference: setReferenceAction };
export default connect(null, mapDispatchToProps)(DeleteBranchDialog);
