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
import { LogEntryV2 as LogEntry } from "@app/services/nessie/client";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { Reference } from "@app/types/nessie";
import NewBranchDialog from "@app/pages/NessieHomePage/components/NewBranchDialog/NewBranchDialog";
import NewTagDialog from "@app/pages/NessieHomePage/components/NewTagDialog/NewTagDialog";
import { VersionedPageTabsType } from "@app/exports/pages/VersionedHomePage/VersionedHomePage";
import { useResourceSnapshot } from "smart-resource/react";
import { ArcticCatalogPrivilegesResource } from "@inject/arctic/resources/ArcticCatalogPrivilegesResource";
import { SmartResource } from "smart-resource1";
import { CatalogCommitsTable } from "@app/exports/pages/VersionedHomePage/components/VersionedPageCommits/CatalogCommitsTable/CatalogCommitsTable";
import { catalogCommitsTableColumns } from "@app/exports/pages/VersionedHomePage/components/VersionedPageCommits/CatalogCommitsTable/catalogCommitsTableColumns";

const INITIAL_BRANCH_STATE_VALUE = {
  openDialog: false,
  fromRef: { type: "BRANCH" } as Reference,
};

const INITIAL_TAG_STATE_VALUE = {
  openDialog: false,
  fromRef: { type: "TAG" } as Reference,
};

type VersionedPageCommitsTableProps = {
  commits: LogEntry[];
  getCommitLink: (id: string) => string;
  loadNextPage: (index?: number) => void;
  goToDataTab: (tab: VersionedPageTabsType, item: LogEntry) => void;
};

function VersionedPageCommitsTable({
  commits,
  getCommitLink,
  loadNextPage,
  goToDataTab,
}: VersionedPageCommitsTableProps) {
  const {
    state: { reference },
  } = useNessieContext();
  const [sort, setSort] = useState<null | Map<string, string>>(null);

  const [branchDialogState, setBranchDialogState] = useState(
    INITIAL_BRANCH_STATE_VALUE,
  );
  const [tagDialogState, setTagDialogState] = useState(INITIAL_TAG_STATE_VALUE);

  const [catalogPrivileges] = useResourceSnapshot(
    ArcticCatalogPrivilegesResource || new SmartResource(() => null),
  );
  const closeDialog = () => {
    setBranchDialogState(INITIAL_BRANCH_STATE_VALUE);
    setTagDialogState(INITIAL_TAG_STATE_VALUE);
  };

  const handleOpenDialog = (type: "TAG" | "BRANCH", dialogState: any) => {
    type === "TAG"
      ? setTagDialogState(dialogState)
      : setBranchDialogState(dialogState);
  };

  const columns = useMemo(() => {
    return catalogCommitsTableColumns(
      getCommitLink,
      goToDataTab,
      handleOpenDialog,
      {
        name: reference?.name,
      },
      catalogPrivileges as any,
    );
  }, [getCommitLink, reference, goToDataTab, catalogPrivileges]);

  const handleColumnSorted = useCallback(
    (sortedColumns: any) => {
      setSort(sortedColumns);
    },
    [setSort],
  );

  const sortedCommits = useMemo(() => {
    if (sort?.get("commitTime") === "ascending") {
      const arr = [...commits].sort((a, b) => {
        const timeA = a.commitMeta?.commitTime || "";
        const timeB = b.commitMeta?.commitTime || "";
        return timeA < timeB ? -1 : timeA > timeB ? 1 : 0;
      });
      return arr;
    } else return commits;
  }, [sort, commits]);

  const getRow = useCallback(
    (rowIndex: number) => {
      const data = sortedCommits?.[rowIndex];
      return {
        id: data?.commitMeta?.hash || rowIndex,
        data: data,
      };
    },
    [sortedCommits],
  );

  return (
    <>
      <CatalogCommitsTable
        columns={columns}
        getRow={getRow}
        onScrolledBottom={loadNextPage}
        rowCount={sortedCommits.length}
        onColumnsSorted={handleColumnSorted}
      />
      <NewBranchDialog
        open={branchDialogState.openDialog}
        forkFrom={branchDialogState.fromRef}
        closeDialog={closeDialog}
        fromType="COMMIT"
      />
      <NewTagDialog
        open={tagDialogState.openDialog}
        forkFrom={tagDialogState.fromRef}
        closeDialog={closeDialog}
      />
    </>
  );
}

export default VersionedPageCommitsTable;
