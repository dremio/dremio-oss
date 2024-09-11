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

// @ts-ignore
import { type Column } from "leantable/react";
// @ts-ignore
import { Skeleton } from "dremio-ui-lib/components";
import { SortableHeaderCell } from "../../../components/TableCells/SortableHeaderCell";
import { NumericCell } from "../../../components/TableCells/NumericCell";
import { formatNumber } from "../../../utilities/formatNumber";

export enum CATALOG_LISTING_COLUMNS {
  name = "name",
  jobs = "jobs",
  datasets = "datasets",
  created = "created",
  actions = "actions",
}

export const getCatalogListingColumnLabels = () => {
  return {
    [CATALOG_LISTING_COLUMNS.name]: "Name",
    [CATALOG_LISTING_COLUMNS.jobs]: "Jobs",
    [CATALOG_LISTING_COLUMNS.datasets]: "Datasets",
    [CATALOG_LISTING_COLUMNS.created]: "Created",
    [CATALOG_LISTING_COLUMNS.actions]: "",
  };
};

export const catalogListingColumns = ({
  isViewAll = false,
  isVersioned = false,
}: any): Column<any>[] => {
  const catalogListingLabels = getCatalogListingColumnLabels();
  const showJobs = !isViewAll && !isVersioned;
  const showDatasets = isViewAll && !isVersioned;
  const showCreated = isViewAll;
  return [
    {
      id: CATALOG_LISTING_COLUMNS.name,
      renderHeaderCell: () => (
        <SortableHeaderCell columnId={CATALOG_LISTING_COLUMNS.name}>
          {catalogListingLabels[CATALOG_LISTING_COLUMNS.name]}
        </SortableHeaderCell>
      ),
      class: "name-column",
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        return row.data.name;
      },
      sortable: true,
    },
    ...(showJobs
      ? [
          {
            id: CATALOG_LISTING_COLUMNS.jobs,
            renderHeaderCell: () => (
              <SortableHeaderCell columnId={CATALOG_LISTING_COLUMNS.jobs}>
                {catalogListingLabels[CATALOG_LISTING_COLUMNS.jobs]}
              </SortableHeaderCell>
            ),
            class: "jobs-column",
            renderCell: (row: any) => {
              if (!row.data) {
                return <Skeleton width="5ch" />;
              }
              return row.data.jobs;
            },
            sortable: true,
          },
        ]
      : []),
    ...(showDatasets
      ? [
          {
            id: CATALOG_LISTING_COLUMNS.datasets,
            renderHeaderCell: () => (
              <SortableHeaderCell columnId={CATALOG_LISTING_COLUMNS.datasets}>
                {catalogListingLabels[CATALOG_LISTING_COLUMNS.datasets]}
              </SortableHeaderCell>
            ),
            renderCell: (row: any) => {
              if (!row.data) {
                return <Skeleton width="5ch" />;
              }
              return (
                <NumericCell>
                  {formatNumber(row.data.datasets) || "0"}
                </NumericCell>
              );
            },
            sortable: true,
          },
        ]
      : []),
    ...(showCreated
      ? [
          {
            id: CATALOG_LISTING_COLUMNS.created,
            renderHeaderCell: () => (
              <SortableHeaderCell columnId={CATALOG_LISTING_COLUMNS.created}>
                {catalogListingLabels[CATALOG_LISTING_COLUMNS.created]}
              </SortableHeaderCell>
            ),
            renderCell: (row: any) => {
              if (!row.data) {
                return <Skeleton width="10ch" />;
              }
              return row.data.created || "";
            },
            sortable: true,
          },
        ]
      : []),
    {
      id: CATALOG_LISTING_COLUMNS.actions,
      class: "leantable-row-hover-visibility actions-column",
      renderHeaderCell: () =>
        catalogListingLabels[CATALOG_LISTING_COLUMNS.actions],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="10ch" />;
        }
        return row.data.actions || "";
      },
    },
  ];
};
