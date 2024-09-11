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

import { type Column } from "leantable/react";
import { createJobId } from "../jobs/components/JobId";
import { DurationCell } from "../../components/TableCells/DurationCell";
import { JobMetrics } from "../components/JobsTable/components/JobMetrics";
import { JobStatus } from "../components/JobStatus";
import { getQueryTypeLabel } from "../components/JobsTable/jobs-table-utils";
import { DateTime } from "../../components/DateTime";
import { SyntaxHighlighter, Tooltip } from "dremio-ui-lib/components";

export const id = (Link: any) => {
  const JobId = createJobId(Link);
  return {
    id: "id",
    class: "leantable-sticky-column leantable-sticky-column--left",
    renderHeaderCell: () => "Job ID",
    renderCell: (row) => (
      <div className="flex h-full flex-row gap-05 items-center">
        <div className="inline-flex" style={{ marginInlineStart: "-5px" }}>
          <JobStatus state={(row.data as any).state} />
        </div>
        <JobId job={row.data} truncate className="flex h-full items-center" />
      </div>
    ),
  } as const satisfies Column<any>;
};

export const duration = {
  id: "duration",
  renderHeaderCell: () => "Duration",
  renderCell: (row) => (
    <DurationCell
      duration={row.data.duration}
      metricContent={<JobMetrics job={row.data} />}
    />
  ),
} as const satisfies Column<any>;

export const startTime = {
  id: "startTime",
  class: "dremio-typography-tabular-numeric",
  renderHeaderCell: () => "Start time",
  renderCell: (row) => (
    <DateTime date={new Date(row.data.startTime)} format="tabular" />
  ),
} as const satisfies Column<any>;

export const queue = {
  id: "queue",
  renderHeaderCell: () => "Queue",
  renderCell: (row) => row.data.wlmQueue,
} as const satisfies Column<any>;

export const queryType = {
  id: "queryType",
  renderHeaderCell: () => "Query type",
  renderCell: (row) => getQueryTypeLabel(row.data),
} as const satisfies Column<any>;

export const sql = {
  id: "sql",
  renderHeaderCell: () => "SQL",
  renderCell: (row) => (
    <Tooltip
      rich
      content={
        <div
          style={{
            maxWidth: "35ch",
            maxHeight: "200px",
            overflowY: "auto",
            overflowX: "hidden",
            overflowWrap: "break-word",
          }}
        >
          <SyntaxHighlighter language="sql" wrapLongLines>
            {row.data.queryText}
          </SyntaxHighlighter>
        </div>
      }
      interactive
    >
      <div
        className="font-mono dremio-typography-less-important text-sm"
        style={{
          maxInlineSize: "50ch",
          overflow: "hidden",
          textOverflow: "ellipsis",
        }}
      >
        {row.data.queryText}
      </div>
    </Tooltip>
  ),
} as const satisfies Column<any>;
