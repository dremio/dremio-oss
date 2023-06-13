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
import { useCallback, useEffect, useMemo, useRef } from "react";
import { createTable } from "leantable/core";
import {
  Table,
  columnSorting,
  infiniteScrolling,
  useExternalStoreState,
} from "leantable/react";

type ReflectionsTableProps = {
  columns: any;
  onScrolledBottom?: () => void;
  onColumnsSorted: (sortedColumns: any) => void;
  pollingCache: Map<string, any>;
  getRow: (rowIndex: number) => any;
  rowCount: number;
};

export const ReflectionsTable = (props: ReflectionsTableProps) => {
  const { columns, onScrolledBottom = () => {}, getRow, rowCount } = props;

  const scrolledBottomRef = useRef(onScrolledBottom);
  scrolledBottomRef.current = onScrolledBottom;

  const reflectionsTable = useMemo(() => {
    return createTable([
      columnSorting(),
      infiniteScrolling(() => scrolledBottomRef.current()),
    ]);
  }, []);

  const sortedColumns = useExternalStoreState(
    reflectionsTable.store,
    (state: any) => state.sortedColumns
  );

  useEffect(() => {
    props.onColumnsSorted(sortedColumns);
  }, [sortedColumns]);

  return (
    <Table
      {...reflectionsTable}
      className="leantable--fixed-header"
      columns={columns}
      getRow={getRow}
      rowCount={rowCount}
    />
  );
};
