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
import { FormattedMessage, useIntl } from "react-intl";
import {
  Button,
  ModalContainer,
  DialogContent,
} from "dremio-ui-lib/components";
import { useDispatch } from "react-redux";
import { setReference } from "@app/actions/nessie/nessie";
import { Reference } from "@app/types/nessie";
import { useNessieContext } from "../../utils/context";
import { ReferenceType } from "@app/services/nessie/client";

import "./DeleteBranchDialog.less";

type DeleteBranchDialogProps = {
  open: boolean;
  referenceToDelete: Reference;
  closeDialog: () => void;
  allRefs?: Reference[];
  setAllRefs?: React.Dispatch<React.SetStateAction<Reference[]>>;
  specialHandling?: () => void;
};

function DeleteBranchDialog({
  open,
  referenceToDelete,
  closeDialog,
  allRefs,
  setAllRefs,
  specialHandling,
}: DeleteBranchDialogProps): JSX.Element {
  const intl = useIntl();
  const dispatch = useDispatch();
  const [isSending, setIsSending] = useState(false);
  const [errorText, setErrorText] = useState<JSX.Element | null>(null);
  const {
    apiV2,
    stateKey,
    state: { reference, defaultReference },
  } = useNessieContext();

  const onDelete = async () => {
    setIsSending(true);

    try {
      await apiV2.deleteReferenceV2({
        ref: referenceToDelete.hash
          ? `${referenceToDelete.name}@${referenceToDelete.hash}`
          : referenceToDelete.name,
        type: ReferenceType.Branch,
      });

      if (allRefs && setAllRefs) {
        const indexToRemove = allRefs.findIndex(
          (ref: Reference) => ref.name === referenceToDelete.name
        );

        setAllRefs([
          ...allRefs.slice(0, indexToRemove),
          ...allRefs.slice(indexToRemove + 1),
        ]);
      }

      if (specialHandling) specialHandling();

      setErrorText(null);

      if (reference && referenceToDelete.name === reference.name) {
        dispatch(setReference({ reference: defaultReference }, stateKey));
      }

      closeDialog();
      setIsSending(false);
    } catch (error: any) {
      const errorMessage = await error.json();
      setErrorText(errorMessage.message);
      setIsSending(false);
    }
  };

  return (
    <ModalContainer open={() => {}} isOpen={open} close={closeDialog}>
      <DialogContent
        className="delete-branch-dialog"
        title={intl.formatMessage(
          { id: "RepoView.Dialog.DeleteBranch.DeleteBranch" },
          { branchName: referenceToDelete.name }
        )}
        actions={
          <>
            <Button
              variant="secondary"
              onClick={() => {
                closeDialog();
                setErrorText(null);
              }}
              disabled={isSending}
            >
              <FormattedMessage id="Common.Cancel" />
            </Button>
            <Button variant="primary" onClick={onDelete} disabled={isSending}>
              <FormattedMessage id="Common.Delete" />
            </Button>
          </>
        }
      >
        <div className="delete-branch-dialog-body">
          <FormattedMessage id="RepoView.Dialog.DeleteBranch.Confirmation" />
          <div className="delete-branch-dialog-body-error">{errorText}</div>
        </div>
      </DialogContent>
    </ModalContainer>
  );
}

export default DeleteBranchDialog;
