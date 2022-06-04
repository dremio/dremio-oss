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

import { useContext, useMemo, useState } from "react";
import { useIntl } from "react-intl";

import { SearchField } from "@app/components/Fields";
import RepoViewBranchList from "./components/RepoViewBranchList/RepoViewBranchList";
import NewBranchDialog from "../../../NewBranchDialog/NewBranchDialog";
import DeleteBranchDialog from "../../../DeleteBranchDialog/DeleteBranchDialog";

import { Reference } from "@app/services/nessie/client";
import { RepoViewContext } from "../../RepoView";

import "./RepoViewBody.less";

function RepoViewBody(): JSX.Element {
  const { defaultRef, allRefs, setAllRefs } = useContext(RepoViewContext);
  const [search, setSearch] = useState("");
  const intl = useIntl();

  const [createBranchState, setCreateBranchState] = useState({
    open: false,
    branch: {
      type: "BRANCH",
    } as Reference,
  });

  const [deleteBranchState, setDeleteBranchState] = useState({
    open: false,
    branch: {
      type: "BRANCH",
    } as Reference,
  });

  const openCreateDialog = (branch: Reference) => {
    setCreateBranchState({
      open: true,
      branch: branch,
    });
  };

  const openDeleteDialog = (branch: Reference) => {
    setDeleteBranchState({
      open: true,
      branch: branch,
    });
  };

  const closeCreateDialog = () => {
    setCreateBranchState({
      open: false,
      branch: { type: "BRANCH" } as Reference,
    });
  };

  const closeDeleteDialog = () => {
    setDeleteBranchState({
      open: false,
      branch: { type: "BRANCH" } as Reference,
    });
  };

  const filteredRows = useMemo((): Reference[] => {
    if (defaultRef) {
      return !search
        ? allRefs.filter((ref) => {
            return ref.type !== "TAG" && ref.name !== defaultRef.name;
          })
        : allRefs.filter((ref) => {
            return (
              ref.name.toLowerCase().includes(search.trim()) &&
              ref.type !== "TAG" &&
              ref.name !== defaultRef.name
            );
          });
    } else {
      return [];
    }
  }, [search, allRefs, defaultRef]);

  return (
    <div className="branch-body">
      <div className="branch-body-search">
        <SearchField
          onChange={setSearch}
          placeholder={intl.formatMessage({
            id: "BranchPicker.BranchSearchPlaceholder",
          })}
        />
      </div>
      <div className="branch-body-default-branch">
        <RepoViewBranchList
          rows={[defaultRef]}
          openCreateDialog={openCreateDialog}
          isDefault
        />
      </div>
      <div className="branch-body-all-branch-list">
        <RepoViewBranchList
          rows={filteredRows}
          openCreateDialog={openCreateDialog}
          openDeleteDialog={openDeleteDialog}
        />
      </div>
      <NewBranchDialog
        open={createBranchState.open}
        forkFrom={createBranchState.branch}
        closeDialog={closeCreateDialog}
        allRefs={allRefs}
        setAllRefs={setAllRefs}
      />
      <DeleteBranchDialog
        open={deleteBranchState.open}
        referenceToDelete={deleteBranchState.branch}
        closeDialog={closeDeleteDialog}
        allRefs={allRefs}
        setAllRefs={setAllRefs}
      />
    </div>
  );
}

export default RepoViewBody;
