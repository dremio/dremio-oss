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
import {
  Button,
  ModalContainer,
  DialogContent,
  SectionMessage,
  MessageDetails,
} from "dremio-ui-lib/components";
import { Reference } from "@app/types/nessie";
import { Select } from "@mantine/core";
import { useNessieContext } from "../../utils/context";
import { addNotification } from "actions/notification";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";

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
type Err = Record<string, unknown> | null;

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
  const [error, setError] = useState<Err>(null);
  const { apiV2 } = useNessieContext();
  const dispatch = useDispatch();
  const { formatMessage } = useIntl();

  const onMerge = async () => {
    setIsSending(true);

    try {
      const selectedRef = allRefs?.find((ref) => ref.name === selectedBranch);
      const branchName = mergeTo ? mergeTo.name : selectedRef?.name;
      const expectedHash = mergeTo ? mergeTo.hash : selectedRef?.hash;
      const fromRefName = mergeFrom.name;
      const fromHash = mergeFrom.hash as string;
      const user = localStorageUtils?.getUserData?.();
      const author = user?.email || user?.userName;

      await apiV2.mergeV2({
        branch: expectedHash ? `${branchName}@${expectedHash}` : branchName,
        merge: {
          fromRefName,
          fromHash,
          ...(author && {
            commitMeta: { authors: [author], message: "" },
          }),
        },
      });

      if (setSuccessMessage) {
        setSuccessMessage(
          <FormattedMessage id="BranchHistory.Dialog.MergeBranch.Success" />,
        );
      }

      setError(null);
      dispatch(
        addNotification(
          formatMessage(
            { id: "ArcticCatalog.Merge.Dialog.SuccessMessage" },
            { branchName, fromRefName },
          ),
          "success",
        ),
      );
      closeDialog();
      setIsSending(false);
    } catch (error: any) {
      try {
        const body = await error.json();
        setError(body);
      } catch (e: any) {
        setError(e);
      } finally {
        setIsSending(false);
      }
    }
  };

  const getErrorProps = (err: Err) => {
    let id = "RepoView.Dialog.DeleteBranch.Error";
    if (err?.status === 409) {
      id = "BranchHistory.Dialog.MergeBranch.Error.Conflict";
    } else if (err?.status === 400) {
      id = "BranchHistory.Dialog.MergeBranch.Error.NoHashes";
    } else if (err?.errorCode === "REFERENCE_NOT_FOUND") {
      id = "BranchHistory.Dialog.MergeBranch.Error.RefNotFound";
    }

    return {
      message: formatMessage({ id }),
      details: err instanceof TypeError ? undefined : (err?.message as string),
    };
  };

  const onClose = () => {
    setSelectedBranch(null);
    setError(null);
    closeDialog();
  };

  const mergeOptions = useMemo(
    () =>
      (allRefs ?? [])
        .filter((ref) => ref.name !== mergeFrom.name && ref.type === "BRANCH")
        .map((ref) => {
          return { label: ref.name, value: ref.name };
        }),
    [allRefs, mergeFrom.name],
  );

  return (
    <ModalContainer
      open={() => {}}
      isOpen={open}
      close={onClose}
      className="modal-container-overflow"
    >
      <DialogContent
        error={
          error ? (
            <SectionMessage appearance="danger">
              <MessageDetails {...getErrorProps(error)} />
            </SectionMessage>
          ) : (
            <></>
          )
        }
        className="merge-branch-dialog"
        title={formatMessage(
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
          },
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
