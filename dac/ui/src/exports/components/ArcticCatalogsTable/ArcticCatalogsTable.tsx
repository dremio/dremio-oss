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

import { Avatar } from "dremio-ui-lib/dist-esm";
import { createTable, type Columns } from "leantable/core";
import { useMemo } from "react";
import { Link } from "react-router";
import { type ArcticCatalog } from "../../endpoints/ArcticCatalogs/ArcticCatalog.type";
import { nameToInitials } from "../../utilities/nameToInitials";
import * as PATHS from "../../paths";
import { formatFixedDateTimeLong } from "../../utilities/formatDate";

const { render } = createTable({ plugins: [] });

const columns: Columns = [
  {
    id: "name",
    cell: "Name",
  },
  {
    id: "owner",
    cell: "Owner",
  },
  {
    id: "createdAt",
    cell: "Created on",
  },
];

type ArcticCatalogsTableProps = {
  catalogs: ArcticCatalog[] | null;
};

export const ArcticCatalogsTable = (
  props: ArcticCatalogsTableProps
): JSX.Element => {
  const { catalogs } = props;
  const rows = useMemo(() => {
    if (!catalogs) {
      return [];
    }
    return catalogs.map((catalog) => ({
      id: catalog.id,
      cells: {
        name: (
          <Link
            to={PATHS.arcticCatalog({
              arcticCatalogId: catalog.id,
            })}
          >
            <span
              style={{
                display: "inline-flex",
                flexDirection: "row",
                alignItems: "center",
                gap: "var(--dremio--spacing--05)",
                fontWeight: "500",
                marginLeft: "-3px",
              }}
            >
              <dremio-icon
                name="brand/arctic-catalog-source"
                alt=""
              ></dremio-icon>
              {catalog.name}
            </span>
          </Link>
        ),
        owner: (
          <span
            style={{
              display: "inline-flex",
              flexDirection: "row",
              alignItems: "center",
            }}
          >
            <Avatar
              initials={nameToInitials(catalog.owner)}
              style={{ marginRight: "var(--dremio--spacing--05)" }}
            />
            {catalog.owner}
          </span>
        ),
        createdAt: (
          <span className="dremio-typography-tabular-numeric">
            {formatFixedDateTimeLong(catalog.createdAt)}
          </span>
        ),
      },
    }));
  }, [catalogs]);
  return render({ columns, rows });
};
