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

import { useState } from "react";
import { connect } from "react-redux";
import { FormattedMessage } from "react-intl";

import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  TextField,
} from "@material-ui/core";

import { setReference as setReferenceAction } from "@app/actions/nessie/nessie";
import { Reference } from "@app/services/nessie/client";
import { CustomDialogTitle } from "../NewBranchDialog/utils";
import { useNessieContext } from "../../utils/context";

import "./RenameBranchDialog.less";

type RenameBranchDialogProps = {
  open: boolean;
  referenceToRename: Reference;
  closeDialog: () => void;
  handleNameChange?: (newName: string) => void;
};

type ConnectedProps = {
  setReference: typeof setReferenceAction;
};

function RenameBranchDialog({
  open,
  referenceToRename,
  closeDialog,
  handleNameChange,
  setReference,
}: RenameBranchDialogProps & ConnectedProps) {
  const {
    api,
    stateKey,
    state: { reference },
  } = useNessieContext();
  const [newName, setNewName] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [errorText, setErrorText] = useState<JSX.Element | null>(null);

  const updateInput = (event: any) => {
    setNewName(event.target.value);
  };

  const onCancel = () => {
    closeDialog();
    setNewName("");
  };

  const onAdd = async () => {
    setIsSending(true);

    try {
      const newReference = {
        type: "BRANCH",
        hash: referenceToRename.hash,
        name: newName,
      } as Reference;

      await api.createReference({
        sourceRefName: referenceToRename.name,
        reference: newReference,
      });

      await api.deleteBranch({
        branchName: referenceToRename.name,
        expectedHash: referenceToRename.hash,
      });

      if (reference && referenceToRename.name === reference.name) {
        setReference({ reference: newReference }, stateKey);
      }

      if (handleNameChange) handleNameChange(newName);

      setErrorText(null);
      closeDialog();
      setNewName("");
      setIsSending(false);
    } catch (error: any) {
      if (error.status === 400) {
        setErrorText(
          <FormattedMessage id="RepoView.Dialog.CreateBranch.Error.InvalidName" />
        );
      } else if (error.status === 409) {
        setErrorText(
          <FormattedMessage id="RepoView.Dialog.CreateBranch.Error.Conflict" />
        );
      } else {
        setErrorText(
          <FormattedMessage id="RepoView.Dialog.DeleteBranch.Error" />
        );
      }

      setIsSending(false);
    }
  };

  return (
    <div>
      <Dialog
        open={open}
        onClose={closeDialog}
        className="rename-branch-dialog"
      >
        <CustomDialogTitle
          onClose={onCancel}
          className="rename-branch-dialog-header"
        >
          <span className="rename-branch-dialog-header-title">
            <FormattedMessage id="BranchHistory.BranchOptions.RenameBranch" />
          </span>
        </CustomDialogTitle>
        <DialogContent className="rename-branch-dialog-body">
          <DialogContentText>
            <FormattedMessage id="RepoView.Dialog.CreateBranch.BranchName" />
          </DialogContentText>
          <TextField
            onChange={updateInput}
            value={newName}
            onKeyDown={(e: any) => {
              e.key === "Enter" && onAdd();
            }}
            autoFocus
            margin="normal"
            id="name"
            type="text"
            fullWidth
            variant="outlined"
            error={!!errorText}
            helperText={errorText}
            label={errorText && <FormattedMessage id="Common.Error" />}
          ></TextField>
        </DialogContent>
        <DialogActions className="rename-branch-dialog-actions">
          <Button
            onClick={onCancel}
            disabled={isSending}
            className="cancel-button"
          >
            <FormattedMessage id="Common.Cancel" />
          </Button>
          <Button
            onClick={onAdd}
            disabled={isSending}
            className="rename-button"
          >
            <FormattedMessage id="Common.Rename" />
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
}

const mapDispatchToProps = { setReference: setReferenceAction };
export default connect(null, mapDispatchToProps)(RenameBranchDialog);
