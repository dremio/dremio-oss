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

import { useEffect, useMemo, useRef } from "react";
import { createTable } from "leantable/core";
import {
  Table,
  columnSorting,
  infiniteScrolling,
  useExternalStoreState,
} from "leantable/react";
import clsx from "clsx";
import EmptyStateContainer from "#oss/pages/HomePage/components/EmptyStateContainer";
import { intl } from "#oss/utils/intl";

import * as classes from "./CatalogCommitsTable.module.less";

type CatalogCommitsTableProps = {
  columns: any;
  onScrolledBottom?: () => void;
  onColumnsSorted: (sortedColumns: any) => void;
  getRow: (rowIndex: number) => any;
  rowCount: number;
};

export const CatalogCommitsTable = (props: CatalogCommitsTableProps) => {
  const { columns, onScrolledBottom = () => {}, getRow, rowCount } = props;

  const scrolledBottomRef = useRef(onScrolledBottom);
  scrolledBottomRef.current = onScrolledBottom;

  const catalogTable = useMemo(() => {
    return createTable([
      columnSorting(),
      infiniteScrolling(() => scrolledBottomRef.current()),
    ]);
  }, []);

  const sortedColumns = useExternalStoreState(
    catalogTable.store,
    (state: any) => state.sortedColumns,
  );

  useEffect(() => {
    props.onColumnsSorted(sortedColumns);
  }, [sortedColumns]);

  return rowCount === 0 ? (
    <div className={classes["catalog-commits-table__empty"]}>
      <EmptyStateContainer
        icon="interface/empty-content"
        title={intl.formatMessage({
          id: "Catalog.Source.Empty.Title",
        })}
      />
    </div>
  ) : (
    <Table
      {...catalogTable}
      className={clsx(
        "leantable--fixed-header",
        classes["catalog-commits-table"],
      )}
      columns={columns}
      getRow={getRow}
      rowCount={rowCount}
    />
  );
};
