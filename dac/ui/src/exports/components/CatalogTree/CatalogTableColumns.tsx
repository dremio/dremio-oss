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

import { memo, useContext, type FC } from "react";
import type { CommunityDataset } from "@dremio/dremio-js/interfaces";
import { CatalogColumnDisplay } from "dremio-ui-common/catalog/CatalogColumnDisplay.js";
import { IconButton } from "dremio-ui-lib/components";
import { TreeConfigContext } from "#oss/components/Tree/treeConfigContext";
import { useTreeItemLevel } from "../Tree";

const CatalogTableColumn: FC<{
  tableColumn: CommunityDataset["fields"][number];
}> = (props) => {
  const { addToEditor } = useContext(TreeConfigContext);

  return (
    <div
      aria-level={useTreeItemLevel()}
      id={props.tableColumn.name}
      role="treeitem"
    >
      <div
        className="flex flex-row items-center h-full position-relative overflow-hidden"
        style={{ marginLeft: "10px" }}
      >
        <CatalogColumnDisplay tableColumn={props.tableColumn} />
      </div>
      <div className="px-05 ml-auto catalog-treeitem__actions">
        <IconButton
          tooltip="Add to query"
          tooltipPortal
          onClick={(e: React.MouseEvent<HTMLElement>) => {
            e.stopPropagation();
            addToEditor?.(props.tableColumn.name);
          }}
        >
          <dremio-icon name="interface/add-small" />
        </IconButton>
      </div>
    </div>
  );
};

const renderCatalogTableColumn = (
  tableColumn: CommunityDataset["fields"][number],
) => <CatalogTableColumn tableColumn={tableColumn} />;

export const CatalogTableColumns: FC<{
  dataset: CommunityDataset;
}> = memo(function CatalogTableColumns(props) {
  return props.dataset.fields
    .sort((a, b) => a.name.localeCompare(b.name))
    .map(renderCatalogTableColumn);
});
