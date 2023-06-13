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
import { CopyButton, Skeleton, Tooltip } from "dremio-ui-lib/components";
// @ts-ignore
import { Tooltip as RichTooltip } from "dremio-ui-lib";
import {
  ColumnSortIcon,
  SortDirection,
} from "../../../components/TableCells/ColumnSortIcon";
import ArcticDataset, { DatasetType } from "../../../components/ArcticDataset";
import DatasetSummaryOverlay from "../../../components/DatasetSummaryOverlay";

const JobTypeLabels: any = {
  ["OPTIMIZE"]: "Optimize",
  ["VACUUM"]: "Vacuum",
};

const JobStatusIcons: any = {
  SETUP: "setup",
  QUEUED: "queued",
  STARTING: "starting",
  RUNNING: { src: "running", className: "spinner" },
  COMPLETED: "job-completed",
  CANCELLED: "canceled",
  FAILED: "error-solid",
};

const JobStatusLabels: any = {
  SETUP: "Setup",
  QUEUED: "Queued",
  STARTING: "Starting",
  RUNNING: "Running",
  COMPLETED: "Completed",
  CANCELLED: "Cancelled",
  FAILED: "Failed",
};

const getJobStatusLabel = (job: any): string | JSX.Element => {
  const status = JobStatusLabels[job.state];
  if (job.state === "FAILED") {
    return `${status}. ${job?.errorMessage ? `(${job?.errorMessage})` : ""}`;
  } else if (job.state === "COMPLETED") {
    if (job.type === "OPTIMIZE" && job?.metrics) {
      return (
        <>
          <div>{status}.</div>
          <div>{`Files rewritten: ${job.metrics.rewrittenDataFiles}`}</div>
          <div>{`New data files: ${job.metrics.newDataFiles}`}</div>
          <div>{`Delete files rewritten: ${job.metrics.rewrittenDeleteFiles}`}</div>
        </>
      );
    } else if (job.type === "VACUUM") {
      const deletedDataFiles = job?.metrics?.deletedDataFiles
        ? `Files deleted: ${job.metrics.deletedDataFiles ?? ""}`
        : "";
      const retentionPeriod = job?.config?.retentionPeriodMinutes
        ? `Data retention period: ${job.config.retentionPeriodMinutes} minutes`
        : "";
      return (
        <>
          {status}. {deletedDataFiles && <div>{deletedDataFiles}</div>}
          {retentionPeriod && <div>{retentionPeriod}</div>}
        </>
      );
    } else {
      return status;
    }
  } else {
    return status;
  }
};

const ArcticCatalogJobTarget = (props: { item: any }) => {
  const { item } = props;
  const tablePath = item.config?.tableId?.split(".");
  const name = tablePath?.pop();
  const path = tablePath?.join(".");
  const target =
    item.type === "OPTIMIZE"
      ? `${tablePath[tablePath.length - 1]} (ref: ${item.config.reference})`
      : item.catalogName;
  return item.type === "OPTIMIZE" ? (
    <RichTooltip
      type="richTooltip"
      enterDelay={1000}
      title={
        <DatasetSummaryOverlay
          name={name}
          path={path}
          reference={item.config.reference}
          datasetType={DatasetType.IcebergTable}
        />
      }
    >
      <div className="arctic-catalog-job__targets">
        <ArcticDataset type={DatasetType.IcebergTable} />
        <div className="arctic-catalog-job__targets__name">{name}</div>
      </div>
    </RichTooltip>
  ) : (
    <div className="arctic-catalog-job__targets">
      {/* @ts-ignore */}
      <dremio-icon
        name="brand/arctic-catalog-source"
        alt={target}
        style={{
          height: 24,
          width: 24,
        }}
      />
      <div className="arctic-catalog-job__targets__name">{target}</div>
    </div>
  );
};

export const getArcticJobColumns = (order: SortDirection): Column<any>[] => [
  {
    id: "target",
    renderHeaderCell: () => "Target",
    renderCell: (row: any) => {
      if (!row.data) {
        return <Skeleton width="17ch" />;
      }
      return <ArcticCatalogJobTarget item={row.data} />;
    },
  },
  {
    id: "jobStatus",
    renderHeaderCell: () => "Status",
    renderCell: (row: any) => {
      if (!row.data) {
        return <Skeleton width="10ch" />;
      }
      const icon = JobStatusIcons[row.data.state];
      return (
        <Tooltip content={getJobStatusLabel(row.data)}>
          <div className="arctic-catalog-job__status">
            {/* @ts-ignore */}
            <dremio-icon
              name={`job-state/${icon?.src ?? icon}`}
              class={icon?.className ?? ""}
              alt={row.data.state}
              style={{
                height: 24,
                width: 24,
                verticalAlign: "unset",
                marginRight: 3,
              }}
            />
            {JobStatusLabels[row.data.state]}
          </div>
        </Tooltip>
      );
    },
  },
  {
    id: "user",
    renderHeaderCell: () => "User",
    renderCell: (row: any) => {
      if (!row.data) {
        return <Skeleton width="17ch" />;
      }
      return <>{row.data.username}</>;
    },
  },
  {
    id: "jobType",
    renderHeaderCell: () => "Job Type",
    renderCell: (row: any) => {
      if (!row.data) {
        return <Skeleton width="17ch" />;
      }
      return <>{JobTypeLabels[row.data.type]}</>;
    },
  },
  {
    id: "engineSize",
    renderHeaderCell: () => "Engine Size",
    renderCell: (row: any) => {
      if (!row.data) {
        return <Skeleton width="17ch" />;
      }
      return <>{row.data.engineSize}</>;
    },
  },
  {
    id: "startTime",
    renderHeaderCell: () => {
      return (
        <>
          Start Time <ColumnSortIcon sortDirection={order} />
        </>
      );
    },
    renderCell: (row: any) => {
      if (!row.data) {
        return <Skeleton width="17ch" />;
      }
      return <>{row.data.startedAt + " "}</>;
    },
  },
  {
    id: "duration",
    renderHeaderCell: () => "Duration",
    renderCell: (row: any) => {
      if (!row.data) {
        return <Skeleton width="17ch" />;
      }
      return <>{row.data.duration}</>;
    },
  },
  {
    id: "jobId",
    renderHeaderCell: () => "Job ID",
    renderCell: (row: any) => {
      if (!row.data) {
        return <Skeleton width="17ch" />;
      }
      return (
        <>
          {row.data.id}
          <CopyButton contents={row.data.id} className="copy-button" />
        </>
      );
    },
  },
];
