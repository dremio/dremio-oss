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
import { useDispatch } from "react-redux";
import { FormattedMessage, useIntl } from "react-intl";
import { Button, ModalContainer, DialogContent } from "dremio-ui-lib/dist-esm";
import { setReference } from "@app/actions/nessie/nessie";
import { TextField } from "@mui/material";
import { Reference } from "@app/types/nessie";
import { useNessieContext } from "../../utils/context";

import "./NewBranchDialog.less";

type NewBranchDialogProps = {
  open: boolean;
  closeDialog: () => void;
  forkFrom: Reference;
  allRefs?: Reference[];
  setAllRefs?: React.Dispatch<React.SetStateAction<Reference[]>>;
  setSuccessMessage?: React.Dispatch<React.SetStateAction<JSX.Element | null>>;
  fromType?: "TAG" | "BRANCH" | "COMMIT";
};

function NewBranchDialog({
  open,
  closeDialog,
  forkFrom,
  allRefs,
  setAllRefs,
  setSuccessMessage,
  fromType,
}: NewBranchDialogProps): JSX.Element {
  const intl = useIntl();
  const { api, stateKey } = useNessieContext();
  const [newBranchName, setNewBranchName] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [errorText, setErrorText] = useState<JSX.Element | null>(null);
  const dispatch = useDispatch();

  const updateInput = (event: any) => {
    setNewBranchName(event.target.value);
  };

  function updateBranchFromRef(ref: Reference) {
    dispatch(setReference({ reference: ref }, stateKey));
  }

  const onCancel = () => {
    closeDialog();
    setNewBranchName("");
    setErrorText(null);
  };

  const onAdd = async () => {
    setIsSending(true);
    try {
      const reference = (await api.createReference({
        sourceRefName: forkFrom ? forkFrom.name : undefined,
        reference: {
          type: "BRANCH",
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
    <ModalContainer open={() => {}} isOpen={open} close={closeDialog}>
      <DialogContent
        className="new-branch-dialog"
        title={intl.formatMessage({
          id: "RepoView.Dialog.CreateBranch.CreateBranch",
        })}
        actions={
          <>
            <Button variant="secondary" onClick={onCancel} disabled={isSending}>
              <FormattedMessage id="Common.Cancel" />
            </Button>
            <Button variant="primary" onClick={onAdd} disabled={isSending}>
              <FormattedMessage id="Common.Add" />
            </Button>
          </>
        }
      >
        <div className="new-branch-dialog-body">
          <div className="new-branch-dialog-body-from">
            <span className="new-branch-dialog-body-from-label">
              <FormattedMessage id="RepoView.Dialog.CreateBranch.CreateBranchFrom" />
            </span>
            <span className="new-branch-dialog-body-from-name">
              {fromType === "COMMIT" ? (
                <>
                  <dremio-icon name="vcs/commit" />
                  <span className="new-tag-dialog-body-commit-hash">
                    {forkFrom?.hash?.substring(0, 30)}
                  </span>
                </>
              ) : (
                <>
                  <dremio-icon
                    name={`vcs/${fromType === "TAG" ? "tag" : "branch"}`}
                  />
                  {forkFrom.name}
                </>
              )}
            </span>
          </div>
          <FormattedMessage id="RepoView.Dialog.CreateBranch.BranchName" />
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
          ></TextField>
        </div>
      </DialogContent>
    </ModalContainer>
  );
}

export default NewBranchDialog;
