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

import { useContext, useState } from "react";
import { FormattedMessage } from "react-intl";

import { NessieState } from "@app/reducers/nessie/nessie";
import FontIcon from "@app/components/Icon/FontIcon";
import NewBranchDialog from "../../../NewBranchDialog/NewBranchDialog";
import BranchButton from "../../../BranchButton/BranchButton";

import { RepoViewContext } from "../../RepoView";

import "./RepoViewHeader.less";

function RepoViewHeader({
  reference,
}: {
  reference: NessieState["reference"];
}): JSX.Element {
  const { allRefs, setAllRefs } = useContext(RepoViewContext);
  const [open, setOpen] = useState(false);

  const openDialog = (): void => {
    setOpen(true);
  };

  const closeDialog = (): void => {
    setOpen(false);
  };

  return (
    <div className="repo-view-header">
      <span className="repo-view-header-git-branch">
        <FontIcon
          type={"GitBranch"}
          theme={{ Icon: { width: "19px", height: "20px" } }}
        />
      </span>
      <span className="repo-view-header-name">
        <FormattedMessage id="RepoView.AllBranches" />
      </span>
      {reference && (
        <>
          <BranchButton onClick={openDialog} />
          <NewBranchDialog
            open={open}
            closeDialog={closeDialog}
            allRefs={allRefs}
            setAllRefs={setAllRefs}
            forkFrom={reference}
          />
        </>
      )}
    </div>
  );
}

export default RepoViewHeader;
