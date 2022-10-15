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

import { useContext, useMemo, useState, useEffect } from "react";
import { intl } from "@app/utils/intl";
import ArcticTableHeader from "@app/exports/components/ArcticTableHeader/ArcticTableHeader";
import RepoViewBranchList from "./components/RepoViewBranchList/RepoViewBranchList";
import NewTagDialog from "../../../NewTagDialog/NewTagDialog";
import NewBranchDialog from "../../../NewBranchDialog/NewBranchDialog";
import DeleteBranchDialog from "../../../DeleteBranchDialog/DeleteBranchDialog";
import MergeBranchDialog from "../../../MergeBranchDialog/MergeBranchDialog";
import { Reference } from "@app/types/nessie";
import { RepoViewContext } from "../../RepoView";

import "./RepoViewBody.less";

function RepoViewBody({ hideTitle }: { hideTitle: boolean }): JSX.Element {
  const { allRefs, setAllRefs, defaultRef } = useContext(RepoViewContext);
  const [search, setSearch] = useState("");
  const [defaultReference, setDefaultReference] = useState<any>({});

  useEffect(() => {
    if (allRefs.length > 0 && defaultRef.name) {
      setDefaultReference(
        allRefs[
          allRefs.findIndex((ref: Reference) => ref.name === defaultRef.name)
        ]
      );
    }
  }, [allRefs, defaultRef]);

  const [createTagState, setCreateTagState] = useState({
    open: false,
    isDefault: false,
    branch: {
      type: "TAG",
    } as Reference,
  });

  const [createBranchState, setCreateBranchState] = useState({
    open: false,
    isDefault: false,
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

  const [mergeBranchState, setMergeBranchState] = useState({
    open: false,
    branch: {
      type: "BRANCH",
    } as Reference,
  });

  const openTagDialog = (branch: Reference, isDefault?: boolean) => {
    setCreateTagState({
      open: true,
      isDefault: isDefault || false,
      branch: branch,
    });
  };

  const openCreateDialog = (branch: Reference, isDefault?: boolean) => {
    setCreateBranchState({
      open: true,
      isDefault: isDefault || false,
      branch: branch,
    });
  };

  const openDeleteDialog = (branch: Reference) => {
    setDeleteBranchState({
      open: true,
      branch: branch,
    });
  };

  const openMergeDialog = (branch: Reference) => {
    setMergeBranchState({
      open: true,
      branch: branch,
    });
  };

  const closeTagDialog = () => {
    setCreateTagState({
      open: false,
      isDefault: false,
      branch: { type: "BRANCH" } as Reference,
    });
  };

  const closeCreateDialog = () => {
    setCreateBranchState({
      open: false,
      isDefault: false,
      branch: { type: "BRANCH" } as Reference,
    });
  };

  const closeDeleteDialog = () => {
    setDeleteBranchState({
      open: false,
      branch: { type: "BRANCH" } as Reference,
    });
  };

  const closeMergeDialog = () => {
    setMergeBranchState({
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
              ref.name.toLowerCase().includes(search.trim().toLowerCase()) &&
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
      <ArcticTableHeader
        placeholder="BranchPicker.BranchSearchPlaceholder"
        onSearchChange={setSearch}
        name={
          hideTitle ? " " : intl.formatMessage({ id: "RepoView.AllBranches" })
        }
      />

      <div className="branch-body-default-branch">
        <RepoViewBranchList
          rows={[defaultReference]}
          openCreateDialog={openCreateDialog}
          openTagDialog={openTagDialog}
          isDefault
          isArcticSource={hideTitle}
        />
      </div>
      <div
        className="branch-body-all-branch-list"
        style={{
          height:
            filteredRows.length > 0
              ? // rows vs item height + header diff
                `${filteredRows.length * 82 + 36}px`
              : // Empty state height
                "207px",
        }}
      >
        <RepoViewBranchList
          rows={filteredRows}
          openTagDialog={openTagDialog}
          openCreateDialog={openCreateDialog}
          openDeleteDialog={openDeleteDialog}
          openMergeDialog={openMergeDialog}
          defaultReference={defaultReference}
          isArcticSource={hideTitle}
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
      <MergeBranchDialog
        open={mergeBranchState.open}
        mergeFrom={mergeBranchState.branch}
        mergeTo={defaultReference}
        closeDialog={closeMergeDialog}
      />
      <NewTagDialog
        open={createTagState.open}
        forkFrom={createTagState.branch}
        closeDialog={closeTagDialog}
      />
    </div>
  );
}

export default RepoViewBody;
