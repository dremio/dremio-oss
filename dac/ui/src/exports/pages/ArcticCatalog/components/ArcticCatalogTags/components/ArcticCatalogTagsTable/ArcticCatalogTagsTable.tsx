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
import { ArcticCatalogTabsType } from "@app/exports/pages/ArcticCatalog/ArcticCatalog";
import { Tag } from "@app/services/nessie/client";
import { Reference } from "@app/types/nessie";
import { generateTableRows, getTagsTableColumns } from "./utils";

import "./ArcticCatalogTagsTable.less";

const INITIAL_STATE_VALUE = {
  openDialog: false,
  newRefType: undefined,
  fromRef: { type: "TAG" } as Reference,
};

type ArcticCatalogTagsProps = {
  references: any[];
  refetch: () => void;
  handlePush: (tag: { type: "TAG" } & Tag, tab: ArcticCatalogTabsType) => void;
};

function ArcticCatalogTagsTable({
  references,
  refetch,
  handlePush,
}: ArcticCatalogTagsProps) {
  const [dialogState, setDialogState] = useState(INITIAL_STATE_VALUE);

  const closeDialog = () => {
    setDialogState(INITIAL_STATE_VALUE);
  };

  const tableData = useMemo(() => {
    // We can assume Tag type here since we are filtering
    const tags: ({ type: "TAG" } & Tag)[] = references;

    return generateTableRows(tags, handlePush, setDialogState);
  }, [references, handlePush]);

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
        open={dialogState.openDialog}
        forkFrom={dialogState.fromRef}
        newRefType={dialogState.newRefType}
        customHeader={
          dialogState.newRefType === "TAG"
            ? "ArcticCatalog.Tags.Dialog.AddTag"
            : undefined
        }
        customContentText={
          dialogState.newRefType === "TAG"
            ? "ArcticCatalog.Tags.Dialog.TagName"
            : undefined
        }
        closeDialog={closeDialog}
        refetch={dialogState.newRefType === "TAG" ? refetch : undefined}
      />
    </div>
  );
}

export default ArcticCatalogTagsTable;
