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
import {
  Button,
  ModalContainer,
  DialogContent,
} from "dremio-ui-lib/components";
import { TextField } from "@mui/material";
import { Reference } from "@app/types/nessie";
import { useNessieContext } from "../../utils/context";
import { addNotification } from "actions/notification";
import { ReferenceType } from "@app/services/nessie/client/index";

import "./NewTagDialog.less";

type NewTagDialogProps = {
  open: boolean;
  closeDialog: () => void;
  forkFrom: Reference;
  refetch?: () => void;
};

function NewTagDialog({
  open,
  closeDialog,
  forkFrom,
  refetch,
}: NewTagDialogProps): JSX.Element {
  const { apiV2 } = useNessieContext();
  const [newTagName, setNewTagName] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [errorText, setErrorText] = useState<JSX.Element | null>(null);
  const intl = useIntl();
  const dispatch = useDispatch();

  const updateInput = (event: any) => {
    setNewTagName(event.target.value);
  };

  const onCancel = () => {
    closeDialog();
    setNewTagName("");
    setErrorText(null);
  };

  const onAdd = async () => {
    setIsSending(true);
    try {
      await apiV2.createReferenceV2({
        name: newTagName,
        type: ReferenceType.Tag,
        reference: forkFrom,
      });

      setErrorText(null);
      closeDialog();
      dispatch(
        addNotification(
          intl.formatMessage(
            { id: "ArcticCatalog.Tags.Dialog.SuccessMessage" },
            { tag_name: newTagName }
          ),
          "success"
        )
      );
      refetch?.();
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
    <ModalContainer open={() => {}} isOpen={open} close={closeDialog}>
      <DialogContent
        className="new-tag-dialog"
        title={intl.formatMessage({
          id: "RepoView.Dialog.CreateTag.NewTag",
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
        <div className="new-tag-dialog-body">
          {forkFrom.hash && (
            <div className="new-tag-dialog-body-commitSection">
              <FormattedMessage id="Common.Commit" />
              <span className="new-tag-dialog-body-commit">
                <dremio-icon name="vcs/commit" />
                <span className="new-tag-dialog-body-commit-hash">
                  {forkFrom.hash.substring(0, 8)}
                </span>
              </span>
            </div>
          )}
          <FormattedMessage id="RepoView.Dialog.CreateTag.TagName" />
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
          ></TextField>
        </div>
      </DialogContent>
    </ModalContainer>
  );
}
export default NewTagDialog;
