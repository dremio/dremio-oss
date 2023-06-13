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

import { useCallback, useMemo, useRef } from "react";
import { getArcticJobColumns } from "./utils";
import { Table, columnSorting, infiniteScrolling } from "leantable/react";
import { createTable } from "leantable/core";
import { SortDirection } from "../../../components/TableCells/ColumnSortIcon";

type ArcticCatalogJobsTableProps = {
  jobs: any[];
  order: SortDirection;
  onScrolledBottom: () => void;
};

const ArcticCatalogJobsTable = ({
  jobs,
  order,
  onScrolledBottom = () => {},
}: ArcticCatalogJobsTableProps) => {
  const columns = useMemo(() => getArcticJobColumns(order), [order]);

  const scrolledBottomRef = useRef(onScrolledBottom);
  scrolledBottomRef.current = onScrolledBottom;

  const arcticCatalogJobsTable = useMemo(() => {
    return createTable([
      columnSorting(),
      infiniteScrolling(() => scrolledBottomRef.current()),
    ]);
  }, []);

  const getRow = useCallback(
    (rowIndex: number) => {
      const data = jobs[rowIndex];
      return {
        id: data?.id || rowIndex,
        data: data || null,
      };
    },
    [jobs]
  );

  return (
    <Table
      {...arcticCatalogJobsTable}
      className="leantable--fixed-header"
      columns={columns}
      getRow={getRow}
      rowCount={jobs.length}
    />
  );
};

export default ArcticCatalogJobsTable;
