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

import { useCallback, useMemo, useState } from "react";
import NewBranchDialog from "#oss/pages/NessieHomePage/components/NewBranchDialog/NewBranchDialog";
import NewTagDialog from "#oss/pages/NessieHomePage/components/NewTagDialog/NewTagDialog";
import { VersionedPageTabsType } from "#oss/exports/pages/VersionedHomePage/VersionedHomePage";
import { Tag } from "#oss/services/nessie/client";
import { Reference } from "#oss/types/nessie";
import DeleteTagDialog from "#oss/pages/NessieHomePage/components/DeleteTagDialog/DeleteTagDialog";
import { CatalogTagsTable } from "#oss/exports/pages/VersionedHomePage/components/VersionedPageTags/components/CatalogTagsTable/CatalogTagsTable";
import { catalogTagsTableColumns } from "#oss/exports/pages/VersionedHomePage/components/VersionedPageTags/components/CatalogTagsTable/catalogTagsTableColumns";

import "./VersionedPageTagsTable.less";

const INITIAL_BRANCH_STATE_VALUE = {
  openDialog: false,
  fromRef: { type: "BRANCH" } as Reference,
};

const INITIAL_TAG_STATE_VALUE = {
  openDialog: false,
  fromRef: { type: "TAG" } as Reference,
};

type VersionedPageTagsProps = {
  references: any[];
  refetch: () => void;
  goToDataTab: (tab: VersionedPageTabsType, tag: { type: "TAG" } & Tag) => void;
};

function VersionedPageTagsTable({
  references,
  refetch,
  goToDataTab,
}: VersionedPageTagsProps) {
  const [branchDialogState, setBranchDialogState] = useState(
    INITIAL_BRANCH_STATE_VALUE,
  );
  const [addTagDialogState, setAddTagDialogState] = useState(
    INITIAL_TAG_STATE_VALUE,
  );
  const [deleteTagDialogState, setDeleteTagDialogState] = useState(
    INITIAL_TAG_STATE_VALUE,
  );
  const [sort, setSort] = useState<null | Map<string, string>>(null);

  const closeDialog = () => {
    setBranchDialogState(INITIAL_BRANCH_STATE_VALUE);
    setAddTagDialogState(INITIAL_TAG_STATE_VALUE);
    setDeleteTagDialogState(INITIAL_TAG_STATE_VALUE);
  };

  const handleOpenDialog = useCallback(
    (type: "TAG" | "BRANCH", dialogState: any) => {
      if (type === "TAG") {
        if (dialogState?.deleteTag) {
          setDeleteTagDialogState(dialogState);
        } else {
          setAddTagDialogState(dialogState);
        }
      } else {
        setBranchDialogState(dialogState);
      }
    },
    [setDeleteTagDialogState, setAddTagDialogState, setBranchDialogState],
  );

  const columns = useMemo(() => {
    return catalogTagsTableColumns(goToDataTab, handleOpenDialog);
  }, [goToDataTab, handleOpenDialog]);

  const handleColumnSorted = useCallback(
    (sortedColumns: any) => {
      setSort(sortedColumns);
    },
    [setSort],
  );

  const sortedTags = useMemo(() => {
    if (sort?.get("commitTime") === "ascending") {
      const arr = [...references].sort((a, b) => {
        const timeA = a.metadata?.commitMetaOfHEAD?.commitTime;
        const timeB = b.metadata?.commitMetaOfHEAD?.commitTime;
        return timeA < timeB ? -1 : timeA > timeB ? 1 : 0;
      });
      return arr;
    } else return references;
  }, [sort, references]);

  const getRow = useCallback(
    (rowIndex: number) => {
      const data = sortedTags?.[rowIndex];
      return {
        id: `${data?.hash}-${rowIndex}`,
        data: { ...data, id: `${data?.hash}-${rowIndex}` },
      };
    },
    [sortedTags],
  );

  return (
    <div className="versioned-tags__content">
      <CatalogTagsTable
        columns={columns}
        getRow={getRow}
        rowCount={sortedTags.length}
        onColumnsSorted={handleColumnSorted}
      />
      <NewBranchDialog
        open={branchDialogState.openDialog}
        forkFrom={branchDialogState.fromRef}
        closeDialog={closeDialog}
        fromType="TAG"
      />
      <NewTagDialog
        open={addTagDialogState.openDialog}
        forkFrom={addTagDialogState.fromRef}
        closeDialog={closeDialog}
        refetch={refetch}
      />
      <DeleteTagDialog
        open={deleteTagDialogState.openDialog}
        forkFrom={deleteTagDialogState.fromRef}
        closeDialog={closeDialog}
        refetch={refetch}
      />
    </div>
  );
}

export default VersionedPageTagsTable;
