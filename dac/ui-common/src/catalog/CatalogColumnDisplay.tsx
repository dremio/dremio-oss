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

import { forwardRef } from "react";
import { CommunityDataset } from "@dremio/dremio-js/interfaces";
import { CatalogColumnIcon } from "./CatalogColumnIcon";

export const CatalogColumnDisplay = forwardRef<
  HTMLDivElement,
  { tableColumn: CommunityDataset["fields"][number] }
>((props, ref) => (
  <div
    className="flex flex-row items-center gap-05"
    ref={ref}
    draggable="true"
    onDragStart={(e) => {
      e.dataTransfer.setData("text/plain", props.tableColumn.name);
      e.dataTransfer.setData(
        "text/json",
        JSON.stringify({
          type: "TableColumn",
          data: {
            name: props.tableColumn.name,
            type: props.tableColumn.type.name,
          },
        }),
      );
    }}
  >
    <div>
      <CatalogColumnIcon tableColumn={props.tableColumn} />
    </div>
    <div className="truncate">{props.tableColumn.name}</div>
  </div>
));

CatalogColumnDisplay.displayName = "CatalogColumnDisplay";
