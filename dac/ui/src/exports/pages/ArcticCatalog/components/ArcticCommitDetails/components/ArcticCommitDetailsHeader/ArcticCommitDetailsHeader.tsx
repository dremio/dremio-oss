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
import NewBranchDialog from "@app/pages/NessieHomePage/components/NewBranchDialog/NewBranchDialog";
// @ts-ignore
import { Button, IconButton } from "dremio-ui-lib";
import { ArcticCatalogTabsType } from "@app/exports/pages/ArcticCatalog/ArcticCatalog";
import { Reference } from "@app/types/nessie";

import * as classes from "./ArcticCommitDetailsHeader.module.less";

type ArcticCommitDetailsHeaderProps = {
  commitId: string;
  reference: Reference | null;
  handlePush: (tab: ArcticCatalogTabsType) => void;
};

function ArcticCommitDetailsHeader({
  commitId,
  reference,
  handlePush,
}: ArcticCommitDetailsHeaderProps) {
  const [dialogState, setDialogState] = useState(false);

  const closeDialog = () => {
    setDialogState(false);
  };

  return (
    <>
      <div className={classes["commit-details-header"]}>
        <span className={classes["commit-details-header__left"]}>
          <IconButton
            tooltip="Common.Back"
            onClick={() => handlePush("commits")}
          >
            <dremio-icon name="interface/circled-arrow-left" />
          </IconButton>
          <span className={classes["commit-details-header__left--hash"]}>
            {commitId}
          </span>
        </span>
        <span className={classes["commit-details-header__right"]}>
          <Button
            color="secondary"
            onClick={() => handlePush("data")}
            disableMargin
            className={classes["commit-details-header__right--button"]}
          >
            <dremio-icon name="interface/goto-dataset" />
            <FormattedMessage id="ArcticCatalog.Commits.GoToData" />
          </Button>
          <IconButton
            tooltip="RepoView.CreateBranch"
            disabled={reference === null}
            onClick={() => {
              setDialogState(true);
            }}
          >
            <dremio-icon name="vcs/create-branch" />
          </IconButton>
        </span>
      </div>
      <NewBranchDialog
        open={dialogState}
        forkFrom={
          ({ ...reference, hash: commitId } as Reference) ?? ({} as Reference)
        }
        closeDialog={closeDialog}
      />
    </>
  );
}

export default ArcticCommitDetailsHeader;
