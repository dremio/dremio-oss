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
import { Button } from "dremio-ui-lib/dist-esm";
import { setReference as setReferenceAction } from "@app/actions/nessie/nessie";
import {
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  TextField,
} from "@mui/material";

import { Reference } from "@app/types/nessie";
import { useNessieContext } from "../../utils/context";
import { CustomDialogTitle } from "./utils";

import "./NewBranchDialog.less";

type NewBranchDialogProps = {
  open: boolean;
  closeDialog: () => void;
  forkFrom: Reference;
  newRefType?: "BRANCH" | "TAG";
  customHeader?: string;
  customContentText?: string;
  allRefs?: Reference[];
  setAllRefs?: React.Dispatch<React.SetStateAction<Reference[]>>;
  setSuccessMessage?: React.Dispatch<React.SetStateAction<JSX.Element | null>>;
  refetch?: () => void;
};

type ConnectedProps = {
  setReference: typeof setReferenceAction;
};

function NewBranchDialog({
  open,
  closeDialog,
  forkFrom,
  newRefType = "BRANCH",
  customHeader,
  customContentText,
  allRefs,
  setAllRefs,
  setSuccessMessage,
  setReference,
  refetch,
}: NewBranchDialogProps & ConnectedProps): JSX.Element {
  const { api, stateKey } = useNessieContext();
  const [newBranchName, setNewBranchName] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [errorText, setErrorText] = useState<JSX.Element | null>(null);

  const updateInput = (event: any) => {
    setNewBranchName(event.target.value);
  };

  function updateBranchFromRef(ref: Reference) {
    setReference({ reference: ref }, stateKey);
  }

  const onCancel = () => {
    closeDialog();
    setNewBranchName("");
  };

  const onAdd = async () => {
    setIsSending(true);
    try {
      const reference = (await api.createReference({
        sourceRefName: forkFrom ? forkFrom.name : undefined,
        reference: {
          type: newRefType,
          hash: forkFrom ? forkFrom.hash : null,
          name: newBranchName,
        } as Reference,
      })) as Reference;

      if (allRefs && setAllRefs) {
        setAllRefs([
          { ...reference, metadata: forkFrom && forkFrom.metadata },
          ...allRefs,
        ]);
      }

      if (setSuccessMessage) {
        setSuccessMessage(
          <FormattedMessage id="RepoView.Dialog.CreateBranch.Success" />
        );
      }

      updateBranchFromRef(reference);

      setErrorText(null);
      closeDialog();
      refetch?.();
      setNewBranchName("");
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
      <Dialog open={open} onClose={closeDialog} className="new-branch-dialog">
        <CustomDialogTitle
          onClose={onCancel}
          className="new-branch-dialog-header"
        >
          <span className="new-branch-dialog-header-title">
            <FormattedMessage
              id={customHeader ?? "RepoView.Dialog.CreateBranch.CreateBranch"}
            />
          </span>
        </CustomDialogTitle>
        <DialogContent className="new-branch-dialog-body">
          <DialogContentText>
            <FormattedMessage
              id={
                customContentText ?? "RepoView.Dialog.CreateBranch.BranchName"
              }
            />
          </DialogContentText>
          <TextField
            onChange={updateInput}
            value={newBranchName}
            onKeyDown={(e) => {
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
        <DialogActions className="new-branch-dialog-actions">
          <Button variant="secondary" onClick={onCancel} disabled={isSending}>
            <FormattedMessage id="Common.Cancel" />
          </Button>
          <Button variant="primary" onClick={onAdd} disabled={isSending}>
            <FormattedMessage id="Common.Add" />
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
}

const mapDispatchToProps = { setReference: setReferenceAction };
export default connect(null, mapDispatchToProps)(NewBranchDialog);
