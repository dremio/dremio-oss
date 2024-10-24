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
import { useIntl, FormattedMessage } from "react-intl";
import NewBranchDialog from "#oss/pages/NessieHomePage/components/NewBranchDialog/NewBranchDialog";
import { Button, IconButton } from "dremio-ui-lib/components";
import { VersionedPageTabsType } from "#oss/exports/pages/VersionedHomePage/VersionedHomePage";
import { Reference } from "#oss/types/nessie";
import CopyButton from "#oss/components/Buttons/CopyButton";
import { CatalogPrivilegeSwitch } from "#oss/exports/components/CatalogPrivilegeSwitch/CatalogPrivilegeSwitch";

import * as classes from "./VersionedCommitDetailsHeader.module.less";

type VersionedCommitDetailsHeaderProps = {
  commitId: string;
  reference: Reference | null;
  handleTabNavigation: (tab: VersionedPageTabsType) => void;
};

function VersionedCommitDetailsHeader({
  commitId,
  reference,
  handleTabNavigation,
}: VersionedCommitDetailsHeaderProps) {
  const [dialogState, setDialogState] = useState(false);
  const { formatMessage } = useIntl();
  const closeDialog = () => {
    setDialogState(false);
  };

  return (
    <>
      <div className={classes["commit-details-header"]}>
        <span className={classes["commit-details-header__left"]}>
          <IconButton
            tooltip={formatMessage({ id: "Common.Back" })}
            onClick={() => handleTabNavigation("commits")}
          >
            <dremio-icon name="interface/circled-arrow-left" />
          </IconButton>
          <span className={classes["commit-details-header__left--hash"]}>
            {formatMessage(
              { id: "VersionedEntity.Commits.Commit.WithHash" },
              { hash: commitId.substring(0, 8) },
            )}
          </span>
          <span>
            <CopyButton
              text={commitId}
              title={formatMessage({
                id: "VersionedEntity.Commits.CommitID.Copy",
              })}
            />
          </span>
        </span>
        <span className={classes["commit-details-header__right"]}>
          <Button
            variant="secondary"
            onClick={() => handleTabNavigation("data")}
            className={classes["commit-details-header__right--button"]}
          >
            <dremio-icon name="interface/goto-dataset" alt="" />
            <FormattedMessage id="VersionedEntity.Commits.GoToData" />
          </Button>
          <CatalogPrivilegeSwitch
            privilege={["branch", "canCreate"]}
            renderEnabled={() => (
              <IconButton
                tooltip={formatMessage({ id: "RepoView.CreateBranch" })}
                disabled={reference === null}
                onClick={() => {
                  setDialogState(true);
                }}
              >
                <dremio-icon name="vcs/create-branch" alt="" />
              </IconButton>
            )}
          />
        </span>
      </div>
      <NewBranchDialog
        open={dialogState}
        forkFrom={
          ({ ...reference, hash: commitId } as Reference) ?? ({} as Reference)
        }
        closeDialog={closeDialog}
        fromType="COMMIT"
      />
    </>
  );
}

export default VersionedCommitDetailsHeader;
