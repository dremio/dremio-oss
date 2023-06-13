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

import { Skeleton } from "dremio-ui-lib/components";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";

export const renderPrivilegesColumns = ({
  nameColumnLabel,
  availablePrivileges,
  renderPrivilegeTooltip,
  renderNameCell,
  renderCheckboxCell,
}: {
  nameColumnLabel: string;
  availablePrivileges: string[];
  renderPrivilegeTooltip: (privilege: string) => string;
  renderNameCell: (data: any) => JSX.Element;
  renderCheckboxCell: (data: any, privilege: string) => JSX.Element;
}) => {
  return [
    {
      id: nameColumnLabel,
      renderHeaderCell: () => nameColumnLabel,
      class: "leantable-sticky-column leantable-sticky-column--left",
      renderCell: (row: any) =>
        row.data ? (
          renderNameCell(row.data)
        ) : (
          <div className="dremio-icon-label">
            <Skeleton
              width="20px"
              height="20px"
              style={{ marginInlineStart: "2px" }}
            />
            <Skeleton width="10ch" />
          </div>
        ),
    },
    ...(availablePrivileges ?? []).map((privilege: string) => ({
      id: privilege,
      renderHeaderCell: () => {
        return privilege ? (
          <Tooltip title={renderPrivilegeTooltip(privilege)}>
            <span>{privilege.split("_").join(" ")}</span>
          </Tooltip>
        ) : (
          <Skeleton width="12ch" />
        );
      },
      renderCell: (row: any) =>
        row.data ? (
          renderCheckboxCell(row.data, privilege)
        ) : (
          <Skeleton width="12ch" />
        ),
    })),
    {
      id: "ExtraSpace",
      renderHeaderCell: () => "",
      renderCell: () => "",
    },
  ];
};
