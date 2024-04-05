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

import { useEffect, useMemo } from "react";
import { createTable } from "leantable/core";
import { Table, columnSorting, useExternalStoreState } from "leantable/react";

type CatalogListingTableProps = {
  columns: any;
  getRow: (rowIndex: number) => any;
  rowCount: number;
  onColumnsSorted?: (sortedColumns: any) => void;
};

export const CatalogListingTable = ({
  columns,
  getRow,
  rowCount,
  onColumnsSorted,
}: CatalogListingTableProps) => {
  const catalogListingListingTable = useMemo(() => {
    return createTable([columnSorting()]);
  }, []);

  const sortedColumns = useExternalStoreState(
    catalogListingListingTable.store,
    (state: any) => state.sortedColumns
  );

  useEffect(() => {
    onColumnsSorted?.(sortedColumns);
  }, [onColumnsSorted, sortedColumns]);

  return (
    <Table
      {...catalogListingListingTable}
      className="leantable--fixed-header"
      columns={columns}
      getRow={getRow}
      rowCount={rowCount}
    />
  );
};
