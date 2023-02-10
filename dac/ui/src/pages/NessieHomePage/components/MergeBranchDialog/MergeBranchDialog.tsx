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

import { useMemo, useState } from "react";
import { useDispatch } from "react-redux";
import { FormattedMessage, useIntl } from "react-intl";
import { Button, ModalContainer, DialogContent } from "dremio-ui-lib/dist-esm";
import { Merge, MergeRefIntoBranchRequest } from "@app/services/nessie/client";
import { Reference } from "@app/types/nessie";
import { Select } from "@mantine/core";
import { useNessieContext } from "../../utils/context";
import { addNotification } from "actions/notification";

import "./MergeBranchDialog.less";

type MergeBranchDialogProps = {
  open: boolean;
  mergeFrom: Reference;
  mergeTo?: Reference;
  closeDialog: () => void;
  setSuccessMessage?: React.Dispatch<React.SetStateAction<JSX.Element | null>>;
  allRefs?: Reference[];
};

// The reason mergeTo is still here is for the old UI, once that is deprecated the mergeTo conditionals can be removed.

function MergeBranchDialog({
  open,
  mergeFrom,
  mergeTo,
  closeDialog,
  setSuccessMessage,
  allRefs,
}: MergeBranchDialogProps): JSX.Element {
  const [selectedBranch, setSelectedBranch] = useState<string | null>(null);
  const [isSending, setIsSending] = useState(false);
  const [errorText, setErrorText] = useState<JSX.Element | null>(null);
  const { api } = useNessieContext();
  const dispatch = useDispatch();
  const intl = useIntl();

  const onMerge = async () => {
    setIsSending(true);

    try {
      const selectedRef = allRefs?.find((ref) => ref.name === selectedBranch);
      const branchName = mergeTo ? mergeTo.name : selectedRef?.name;
      const expectedHash = mergeTo ? mergeTo.hash : selectedRef?.hash;
      const fromRefName = mergeFrom.name;
      const fromHash = mergeFrom.hash as string;

      await api.mergeRefIntoBranch({
        branchName,
        expectedHash,
        merge: {
          fromRefName,
          fromHash,
        } as Merge,
      } as MergeRefIntoBranchRequest);

      if (setSuccessMessage) {
        setSuccessMessage(
          <FormattedMessage id="BranchHistory.Dialog.MergeBranch.Success" />
        );
      }

      setErrorText(null);
      dispatch(
        addNotification(
          intl.formatMessage(
            { id: "ArcticCatalog.Merge.Dialog.SuccessMessage" },
            { branchName, fromRefName }
          ),
          "success"
        )
      );
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

  const onClose = () => {
    setSelectedBranch(null);
    setErrorText(null);
    closeDialog();
  };

  const mergeOptions = useMemo(
    () =>
      (allRefs ?? [])
        .filter((ref) => ref.name !== mergeFrom.name && ref.type === "BRANCH")
        .map((ref) => {
          return { label: ref.name, value: ref.name };
        }),
    [allRefs, mergeFrom.name]
  );

  return (
    <ModalContainer
      open={() => {}}
      isOpen={open}
      close={onClose}
      className="modal-container-overflow"
    >
      <DialogContent
        className="merge-branch-dialog"
        title={intl.formatMessage(
          {
            id: mergeTo
              ? "RepoView.Dialog.CreateBranch.CreateBranch"
              : "BranchHistory.Dialog.MergeBranch",
          },
          {
            ...(mergeTo && {
              srcBranch: mergeFrom.name,
              dstBranch: mergeTo.name,
            }),
          }
        )}
        actions={
          <>
            <Button variant="secondary" onClick={onClose} disabled={isSending}>
              <FormattedMessage id="Common.Cancel" />
            </Button>
            <Button variant="primary" onClick={onMerge} disabled={isSending}>
              <FormattedMessage id="BranchHistory.Header.Merge" />
            </Button>
          </>
        }
      >
        <div className="merge-branch-dialog-body" key={selectedBranch ?? ""}>
          <>
            {mergeTo ? (
              <FormattedMessage id="BranchHistory.Dialog.MergeBranch.ContentText" />
            ) : (
              <>
                <div className="merge-branch-dialog-into">
                  <FormattedMessage id="BranchHistory.Dialog.MergeBranch.Into" />
                </div>
                <Select
                  className="merge-branch-dialog-select"
                  data={mergeOptions}
                  value={selectedBranch}
                  onChange={(value: string) => setSelectedBranch(value)}
                  styles={() => ({ item: styles.options })}
                />
                <div className="merge-branch-dialog-body-error">
                  {errorText}
                </div>
              </>
            )}
          </>
        </div>
      </DialogContent>
    </ModalContainer>
  );
}

// Example: https://mantine.dev/core/select/#styles-api
const styles = {
  options: {
    height: 32,
    // applies styles to selected item
    "&[data-selected]": {
      "&, &:hover": {
        backgroundColor: "var(--dremio--color--pale--navy)",
        color: "var(--dremio--color--text--main)",
      },
    },

    // applies styles to hovered item (with mouse or keyboard)
    "&[data-hovered]": {
      backgroundColor: "#f5fcff",
    },
  },
};

export default MergeBranchDialog;
