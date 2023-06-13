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

import { Avatar } from "dremio-ui-lib/components";
import { createTable, type Columns } from "leantable2/core";
import { useMemo } from "react";
import { useIntl } from "react-intl";
import { Link } from "react-router";
import { type ArcticCatalog } from "../../endpoints/ArcticCatalogs/ArcticCatalog.type";
import { nameToInitials } from "../../utilities/nameToInitials";
import * as PATHS from "../../paths";
import { formatFixedDateTimeLong } from "../../utilities/formatDate";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
//@ts-ignore
import { IconButton } from "dremio-ui-lib";
import classes from "./ArcticCatalogsTable.less";

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
  {
    id: "settings",
    cell: "",
  },
];

type ArcticCatalogsTableProps = {
  catalogs: ArcticCatalog[] | null;
};

export const ArcticCatalogsTable = (
  props: ArcticCatalogsTableProps
): JSX.Element => {
  const { catalogs } = props;
  const { formatMessage } = useIntl();
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
            <span className={classes["arctic-catalog-row__name"]}>
              <dremio-icon
                name="brand/arctic-catalog-source"
                alt=""
              ></dremio-icon>
              {catalog.name}
            </span>
          </Link>
        ),
        owner: (
          <span className={classes["arctic-catalog-row__owner"]}>
            <Avatar
              initials={nameToInitials(catalog.ownerName)}
              //@ts-ignore
              style={{ marginRight: "var(--dremio--spacing--05)" }}
            />
            {catalog.ownerName}
          </span>
        ),
        createdAt: (
          <span className="dremio-typography-tabular-numeric">
            {formatFixedDateTimeLong(catalog.createdAt)}
          </span>
        ),
        settings: (
          <IconButton
            className="arctic-catalog-row__settings"
            as={LinkWithRef}
            to={PATHS.arcticCatalogSettings({ arcticCatalogId: catalog.id })}
            tooltip="Settings.Catalog"
          >
            <dremio-icon
              name="interface/settings"
              alt={formatMessage({ id: "Settings.Catalog" })}
            />
          </IconButton>
        ),
      },
    }));
  }, [catalogs, formatMessage]);
  return render({ columns, rows });
};
