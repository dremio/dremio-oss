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
import StatefulTableViewer from "@app/components/StatefulTableViewer";
import { LogEntry } from "@app/services/nessie/client";
import { generateTableRows, getCommitsTableColumns } from "./utils";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { Reference } from "@app/types/nessie";
import NewBranchDialog from "@app/pages/NessieHomePage/components/NewBranchDialog/NewBranchDialog";
import NewTagDialog from "@app/pages/NessieHomePage/components/NewTagDialog/NewTagDialog";
import { ArcticCatalogTabsType } from "@app/exports/pages/ArcticCatalog/ArcticCatalog";

import "./ArcticCatalogCommitsTable.less";

const INITIAL_BRANCH_STATE_VALUE = {
  openDialog: false,
  fromRef: { type: "BRANCH" } as Reference,
};

const INITIAL_TAG_STATE_VALUE = {
  openDialog: false,
  fromRef: { type: "TAG" } as Reference,
};

type ArcticCatalogCommitsTableProps = {
  commits: LogEntry[];
  onRowClick: (rowId: Record<string, any>) => void;
  loadNextPage: (index: number) => void;
  goToDataTab: (tab: ArcticCatalogTabsType, item: LogEntry) => void;
};

function ArcticCatalogCommitsTable({
  commits,
  onRowClick,
  loadNextPage,
  goToDataTab,
}: ArcticCatalogCommitsTableProps) {
  const {
    state: { reference },
  } = useNessieContext();

  const [branchDialogState, setBranchDialogState] = useState(
    INITIAL_BRANCH_STATE_VALUE
  );
  const [tagDialogState, setTagDialogState] = useState(INITIAL_TAG_STATE_VALUE);

  const closeDialog = () => {
    setBranchDialogState(INITIAL_BRANCH_STATE_VALUE);
    setTagDialogState(INITIAL_TAG_STATE_VALUE);
  };

  const handleOpenDialog = (type: "TAG" | "BRANCH", dialogState: any) => {
    type === "TAG"
      ? setTagDialogState(dialogState)
      : setBranchDialogState(dialogState);
  };

  const tableData = useMemo(() => {
    return generateTableRows(commits, goToDataTab, handleOpenDialog, {
      name: reference?.name,
    });
  }, [commits, reference, goToDataTab]);

  return (
    <>
      <StatefulTableViewer
        virtualized
        disableZebraStripes
        columns={getCommitsTableColumns()}
        tableData={tableData}
        rowHeight={40}
        onClick={onRowClick}
        className="arctic-commits-table"
        loadNextRecords={loadNextPage}
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

export default ArcticCatalogCommitsTable;
