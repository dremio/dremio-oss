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
import NewBranchDialog from "@app/pages/NessieHomePage/components/NewBranchDialog/NewBranchDialog";
import NewTagDialog from "@app/pages/NessieHomePage/components/NewTagDialog/NewTagDialog";
import { ArcticCatalogTabsType } from "@app/exports/pages/ArcticCatalog/ArcticCatalog";
import { Tag } from "@app/services/nessie/client";
import { Reference } from "@app/types/nessie";
import { generateTableRows, getTagsTableColumns } from "./utils";
import DeleteTagDialog from "@app/pages/NessieHomePage/components/DeleteTagDialog/DeleteTagDialog";
import "./ArcticCatalogTagsTable.less";

const INITIAL_BRANCH_STATE_VALUE = {
  openDialog: false,
  fromRef: { type: "BRANCH" } as Reference,
};

const INITIAL_TAG_STATE_VALUE = {
  openDialog: false,
  fromRef: { type: "TAG" } as Reference,
};

type ArcticCatalogTagsProps = {
  references: any[];
  refetch: () => void;
  goToDataTab: (tab: ArcticCatalogTabsType, tag: { type: "TAG" } & Tag) => void;
};

function ArcticCatalogTagsTable({
  references,
  refetch,
  goToDataTab,
}: ArcticCatalogTagsProps) {
  const [branchDialogState, setBranchDialogState] = useState(
    INITIAL_BRANCH_STATE_VALUE
  );
  const [addTagDialogState, setAddTagDialogState] = useState(
    INITIAL_TAG_STATE_VALUE
  );
  const [deleteTagDialogState, setDeleteTagDialogState] = useState(
    INITIAL_TAG_STATE_VALUE
  );

  const closeDialog = () => {
    setBranchDialogState(INITIAL_BRANCH_STATE_VALUE);
    setAddTagDialogState(INITIAL_TAG_STATE_VALUE);
    setDeleteTagDialogState(INITIAL_TAG_STATE_VALUE);
  };

  const handleOpenDialog = (type: "TAG" | "BRANCH", dialogState: any) => {
    if (type === "TAG") {
      if (dialogState?.deleteTag) {
        setDeleteTagDialogState(dialogState);
      } else {
        setAddTagDialogState(dialogState);
      }
    } else {
      setBranchDialogState(dialogState);
    }
  };

  const tableData = useMemo(() => {
    // We can assume Tag type here since we are filtering
    const tags: ({ type: "TAG" } & Tag)[] = references;

    return generateTableRows(tags, goToDataTab, handleOpenDialog);
  }, [references, goToDataTab]);

  return (
    <div className="arctic-tags__content">
      <StatefulTableViewer
        virtualized
        disableZebraStripes
        columns={getTagsTableColumns()}
        tableData={tableData}
        rowHeight={40}
        className="arctic-tags-table"
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

export default ArcticCatalogTagsTable;
