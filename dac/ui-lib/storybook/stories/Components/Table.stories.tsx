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

import { createTable } from "leantable/core";
import { Column, columnSorting, Table } from "leantable/react";

const defaultTable = createTable([columnSorting()]);

export default {
  title: "Components/Table",
};

const mockData = [
  {
    id: "fa9fc823f800",
    user: "sampleuser@dremio.com",
    dataset: "Unavailable",
    queryType: "JDBC Client",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select * from Samples."http://samples.dremio.com"."NYC-taxi-trips-iceberg"`,
    cost: "1",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
  {
    id: "dba4d144b300",
    user: "sampleuser@dremio.com",
    dataset: "trips_pickupdate",
    queryType: "JDBC Client",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select * from S3.Taxi."trips_pickupdate"`,
    cost: "20K",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
  {
    id: "423fd4154c00",
    user: "sampleuser@dremio.com",
    dataset: "NYC-taxi-trips-iceberg",
    queryType: "UI (run)",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select * from Samples."samples.dremio.com"."NYC-taxi-trips-iceberg"`,
    cost: "20K",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
  {
    id: "d32f6fb63200",
    user: "sampleuser@dremio.com",
    dataset: "returns_report",
    queryType: "UI (run)",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select r_reason_desc, count(1) num_returns, sum(cr_return_amount) return_amt from catalog_returns cr inner join reason r on cr.cr_reason_fk = r.r_reason_sk group by 1 order by 2 desc`,
    cost: "20K",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
  {
    id: "8eb73052b100",
    user: "sampleuser@dremio.com",
    dataset: "trips_pickupdate",
    queryType: "JDBC Client",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select * from S3.Taxi."trips_pickupdate"`,
    cost: "20K",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
  {
    id: "b587c070f300",
    user: "sampleuser@dremio.com",
    dataset: "NYC-taxi-trips-iceberg",
    queryType: "UI (run)",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select * from Samples."samples.dremio.com"."NYC-taxi-trips-iceberg"`,
    cost: "20K",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
  {
    id: "2b5b347b5500",
    user: "sampleuser@dremio.com",
    dataset: "returns_report",
    queryType: "UI (run)",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select r_reason_desc, count(1) num_returns, sum(cr_return_amount) return_amt from catalog_returns cr inner join reason r on cr.cr_reason_fk = r.r_reason_sk group by 1 order by 2 desc`,
    cost: "20K",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
  {
    id: "51666df1f800",
    user: "sampleuser@dremio.com",
    dataset: "Unavailable",
    queryType: "UI (run)",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select * from Samples."http://samples.dremio.com"."NYC-taxi-trips-iceberg"`,
    cost: "20K",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
  {
    id: "6bd306e61100",
    user: "sampleuser@dremio.com",
    dataset: "NYC-taxi-trips-iceberg",
    queryType: "UI (run)",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select * from Samples."samples.dremio.com"."NYC-taxi-trips-iceberg"`,
    cost: "20K",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
  {
    id: "dbe47b75ce00",
    user: "sampleuser@dremio.com",
    dataset: "returns_report",
    queryType: "JDBC Client",
    engine: "engine1",
    startTime: new Date().toDateString(),
    duration: "00:00:08",
    sql: `select r_reason_desc, count(1) num_returns, sum(cr_return_amount) return_amt from catalog_returns cr inner join reason r on cr.cr_reason_fk = r.r_reason_sk group by 1 order by 2 desc`,
    cost: "20K",
    time: "<1s",
    scanned: 100,
    returned: 100,
  },
];

const columns: Column<typeof mockData[0]>[] = [
  {
    id: "id",
    class: "leantable-sticky-column leantable-sticky-column--left",
    renderHeaderCell: () => "Job ID",
    renderCell: (row) => <code>{row.data.id}</code>,
    sortable: true,
  },
  {
    id: "user",
    renderHeaderCell: () => "User",
    renderCell: (row) => row.data.user,
    sortable: true,
  },
  {
    id: "dataset",
    renderHeaderCell: () => "Dataset",
    renderCell: (row) => row.data.dataset,
  },
  {
    id: "queryType",
    renderHeaderCell: () => "Query Type",
    renderCell: (row) => row.data.queryType,
  },
  {
    id: "engine",
    renderHeaderCell: () => "Engine",
    renderCell: (row) => row.data.engine,
  },
  {
    id: "startTime",
    renderHeaderCell: () => "Start Time",
    renderCell: (row) => row.data.startTime,
  },
  {
    id: "duration",
    renderHeaderCell: () => "Duration",
    renderCell: (row) => row.data.duration,
  },
  {
    id: "sql",
    renderHeaderCell: () => "SQL",
    renderCell: (row) => (
      <code
        style={{
          display: "-webkit-box",
          minWidth: "500px",
          whiteSpace: "normal",
          "-webkit-line-clamp": 3,
          "-webkit-box-orient": "vertical",
          textOverflow: "ellipsis",
          overflow: "hidden",
        }}
      >
        {row.data.sql}
      </code>
    ),
  },
  {
    id: "cost",
    renderHeaderCell: () => "Planner Cost Estimate",
    renderCell: (row) => row.data.cost,
  },
  {
    id: "time",
    renderHeaderCell: () => "Planning Time",
    renderCell: (row) => row.data.time,
  },
  {
    id: "scanned",
    renderHeaderCell: () => "Rows Scanned",
    renderCell: (row) => row.data.scanned,
  },
  {
    id: "returned",
    renderHeaderCell: () => "Rows Returned",
    renderCell: (row) => row.data.returned,
  },
];

const getRow = (i: number) => {
  const data = mockData[i];
  return {
    id: data.id,
    data,
  };
};

export const Default = () => {
  return (
    <div style={{ maxHeight: "100%" }}>
      <Table
        {...defaultTable}
        className="leantable--table-layout leantable--fixed-header"
        columns={columns}
        rowCount={mockData.length}
        getRow={getRow}
      />
    </div>
  );
};

Default.storyName = "Table";
