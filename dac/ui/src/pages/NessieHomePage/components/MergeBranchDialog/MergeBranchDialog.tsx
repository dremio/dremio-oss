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
import { FormattedMessage } from "react-intl";
import { Button } from "dremio-ui-lib/dist-esm";

import {
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
} from "@mui/material";

import { Merge, MergeRefIntoBranchRequest } from "@app/services/nessie/client";
import { Reference } from "@app/types/nessie";

import { CustomDialogTitle } from "../NewBranchDialog/utils";
import { useNessieContext } from "../../utils/context";

import "./MergeBranchDialog.less";

type MergeBranchDialogProps = {
  open: boolean;
  mergeFrom: Reference;
  mergeTo: Reference;
  closeDialog: () => void;
  setSuccessMessage?: React.Dispatch<React.SetStateAction<JSX.Element | null>>;
  allRefs?: Reference[];
  setAllRefs?: React.Dispatch<React.SetStateAction<Reference[]>>;
};

function MergeBranchDialog({
  open,
  mergeFrom,
  mergeTo,
  closeDialog,
  setSuccessMessage,
  allRefs,
  setAllRefs,
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
          fromHash: mergeFrom.hash as string,
        } as Merge,
      } as MergeRefIntoBranchRequest);

      if (setSuccessMessage) {
        setSuccessMessage(
          <FormattedMessage id="BranchHistory.Dialog.MergeBranch.Success" />
        );
      }

      if (allRefs && setAllRefs) {
        const indexToRemove = allRefs.findIndex(
          (ref: Reference) => ref.name === mergeFrom.name
        );

        setAllRefs([
          ...allRefs.slice(0, indexToRemove),
          ...allRefs.slice(indexToRemove + 1),
        ]);
      }

      setErrorText(null);
      closeDialog();
      setIsSending(false);
    } catch (error: any) {
      if (error.status === 409) {
        setErrorText(
          <FormattedMessage id="BranchHistory.Dialog.MergeBranch.Error.Conflict" />
        );
      } else if (error.status === 400) {
        setErrorText(
          <FormattedMessage id="BranchHistory.Dialog.MergeBranch.Error.NoHashes" />
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
      <Dialog open={open} onClose={closeDialog} className="merge-branch-dialog">
        <CustomDialogTitle
          onClose={closeDialog}
          className="merge-branch-dialog-header"
        >
          <span className="merge-branch-dialog-header-title">
            <FormattedMessage
              id="BranchHistory.Dialog.MergeBranch.MergeBranch"
              values={{
                srcBranch: mergeFrom.name,
                dstBranch: mergeTo.name,
              }}
            />
          </span>
        </CustomDialogTitle>
        <DialogContent className="merge-branch-dialog-body">
          <DialogContentText>
            <FormattedMessage id="BranchHistory.Dialog.MergeBranch.ContentText" />
          </DialogContentText>
          <div className="merge-branch-dialog-body-error">{errorText}</div>
        </DialogContent>
        <DialogActions className="merge-branch-dialog-actions">
          <Button
            variant="secondary"
            onClick={closeDialog}
            disabled={isSending}
          >
            <FormattedMessage id="Common.Cancel" />
          </Button>
          <Button variant="primary" onClick={onMerge} disabled={isSending}>
            <FormattedMessage id="BranchHistory.Header.Merge" />
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
}

export default MergeBranchDialog;
