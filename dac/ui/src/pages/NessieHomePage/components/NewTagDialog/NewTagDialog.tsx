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
import { intl } from "@app/utils/intl";
import { FormattedMessage } from "react-intl";
import { Button } from "dremio-ui-lib/dist-esm";
import { setReference as setReferenceAction } from "@app/actions/nessie/nessie";
import {
  Dialog,
  DialogTitle,
  DialogActions,
  DialogContent,
  DialogContentText,
  TextField,
} from "@mui/material";
import { Reference } from "@app/types/nessie";
import { useNessieContext } from "../../utils/context";
import { addNotification } from "actions/notification";

import "./NewTagDialog.less";

type NewTagDialogProps = {
  open: boolean;
  closeDialog: () => void;
  forkFrom: Reference;
  addNotification: typeof addNotification;
};

type ConnectedProps = {
  setReference: typeof setReferenceAction;
};

function NewTagDialog({
  open,
  closeDialog,
  forkFrom,
  addNotification,
}: NewTagDialogProps & ConnectedProps): JSX.Element {
  const { api } = useNessieContext();
  const [newTagName, setNewTagName] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [errorText, setErrorText] = useState<JSX.Element | null>(null);

  const updateInput = (event: any) => {
    setNewTagName(event.target.value);
  };

  const onCancel = () => {
    closeDialog();
    setNewTagName("");
  };

  const onAdd = async () => {
    setIsSending(true);
    try {
      await api.createReference({
        sourceRefName: forkFrom ? forkFrom.name : undefined,
        reference: {
          type: "TAG",
          hash: forkFrom ? forkFrom.hash : null,
          name: newTagName,
        } as Reference,
      });

      setErrorText(null);
      closeDialog();
      addNotification(
        intl.formatMessage(
          { id: "ArcticCatalog.Tags.Dialog.SuccessMessage" },
          { tag_name: newTagName }
        ),
        "success"
      );
      setIsSending(false);
      setNewTagName("");
    } catch (error: any) {
      if (error.status === 400) {
        setErrorText(
          <FormattedMessage id="RepoView.Dialog.CreateTag.Error.InvalidName" />
        );
      } else if (error.status === 409) {
        setErrorText(
          <FormattedMessage id="RepoView.Dialog.CreateTag.Error.Conflict" />
        );
      } else {
        setErrorText(<FormattedMessage id="RepoView.Dialog.DeleteTag.Error" />);
      }

      setIsSending(false);
    }
  };

  return (
    <div>
      <Dialog open={open} onClose={closeDialog} className="new-tag-dialog">
        <DialogTitle className="new-tag-dialog-header">
          <span className="new-tag-dialog-header-title">
            <FormattedMessage id="RepoView.Dialog.CreateTag.NewTag" />
          </span>
        </DialogTitle>
        <DialogContent className="new-tag-dialog-body">
          {forkFrom.hash && (
            <DialogContentText>
              <FormattedMessage id="Common.Commit" />
              <span className="new-tag-dialog-body-commit">
                <dremio-icon name="vcs/commit" />
                <span className="new-tag-dialog-body-commit-hash">
                  {forkFrom.hash.substring(0, 30)}
                </span>
              </span>
            </DialogContentText>
          )}
          <DialogContentText>
            <FormattedMessage id="RepoView.Dialog.CreateTag.TagName" />
          </DialogContentText>
          <TextField
            onChange={updateInput}
            value={newTagName}
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
        <DialogActions className="new-tag-dialog-actions">
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

const mapDispatchToProps = {
  setReference: setReferenceAction,
  addNotification,
};
export default connect(null, mapDispatchToProps)(NewTagDialog);
