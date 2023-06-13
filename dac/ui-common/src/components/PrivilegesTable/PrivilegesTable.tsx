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

import { useMemo, useRef } from "react";
import { createTable } from "leantable/core";
import { Table, columnSorting, infiniteScrolling } from "leantable/react";

type PrivilegesTableProps = {
  columns: any;
  getRow: (rowIndex: number) => any;
  rowCount: number;
};

export const PrivilegesTable = (props: PrivilegesTableProps) => {
  const { columns, getRow, rowCount } = props;

  const scrolledBottomRef = useRef(() => {});
  scrolledBottomRef.current = () => {};

  const privilegesTable = useMemo(() => {
    return createTable([
      columnSorting(),
      infiniteScrolling(() => scrolledBottomRef.current()),
    ]);
  }, []);

  return (
    <Table
      {...privilegesTable}
      className="leantable--fixed-header"
      columns={columns}
      getRow={getRow}
      rowCount={rowCount}
    />
  );
};
