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
import { Skeleton, Tooltip, CopyButton } from "dremio-ui-lib/components";
import { getIntlContext } from "../../../contexts/IntlContext";
import { SortableHeaderCell } from "../../../components/TableCells/SortableHeaderCell";
import { JobStatus } from "../JobStatus";
import { ReflectionIcon } from "../ReflectionIcon";
import { TableCellWithTooltip, getQueryTypeLabel } from "./jobs-table-utils";
import { TimestampCellShortNoTZ } from "../../../components/TableCells/TimestampCell";
import { DurationCell } from "../../../components/TableCells/DurationCell";
import { NumericCell } from "../../../components/TableCells/NumericCell";
import { formatBytes } from "../../../utilities/formatBytes";
import { JobMetrics } from "./components/JobMetrics";
import { formatNumber } from "../../../utilities/formatNumber";

export enum JOB_COLUMNS {
  jobId = "jobId",
  user = "user",
  accelerated = "accelerated",
  dataset = "dataset",
  queryType = "queryType",
  engine = "engine",
  queue = "queue",
  startTime = "startTime",
  duration = "duration",
  sql = "sql",
  plannerCostEstimate = "plannerCostEstimate",
  planningTime = "planningTime",
  rowsScanned = "rowsScanned",
  rowsReturned = "rowsReturned",
}

export const getJobColumnLabels = () => {
  const { t } = getIntlContext();
  return {
    [JOB_COLUMNS.jobId]: t("Sonar.Job.Column.JobId.Label"),
    [JOB_COLUMNS.user]: t("Sonar.Job.Column.User.Label"),
    [JOB_COLUMNS.accelerated]: t("Sonar.Job.Column.Accelerated.Label"),
    [JOB_COLUMNS.dataset]: t("Sonar.Job.Column.Dataset.Label"),
    [JOB_COLUMNS.queryType]: t("Sonar.Job.Column.QueryType.Label"),
    [JOB_COLUMNS.engine]: t("Sonar.Job.Column.Engine.Label"),
    [JOB_COLUMNS.queue]: t("Sonar.Job.Column.Queue.Label"),
    [JOB_COLUMNS.startTime]: t("Sonar.Job.Column.StartTime.Label"),
    [JOB_COLUMNS.duration]: t("Sonar.Job.Column.Duration.Label"),
    [JOB_COLUMNS.sql]: t("Sonar.Job.Column.SQL.Label"),
    [JOB_COLUMNS.plannerCostEstimate]: t("Sonar.Job.Column.CostEstimate.Label"),
    [JOB_COLUMNS.planningTime]: t("Sonar.Job.Column.PlanningTime.Label"),
    [JOB_COLUMNS.rowsScanned]: t("Sonar.Job.Column.RowsScanned.Label"),
    [JOB_COLUMNS.rowsReturned]: t("Sonar.Job.Column.RowsReturned.Label"),
  };
};

export const jobsPageTableColumns = (
  renderJobLink?: (job: any) => React.ReactElement,
  renderDataset?: (row: any) => React.ReactElement,
  renderSQL?: (sql: string) => React.ReactElement,
  isSoftware?: boolean,
): Column<any>[] => {
  const jobColumnLabels = getJobColumnLabels();
  const { t } = getIntlContext();
  return [
    {
      id: JOB_COLUMNS.jobId,
      class:
        "job-id-and-status leantable-sticky-column leantable-sticky-column--left",
      renderHeaderCell: () => (
        <SortableHeaderCell columnId={JOB_COLUMNS.jobId}>
          {jobColumnLabels[JOB_COLUMNS.jobId]}
        </SortableHeaderCell>
      ),
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        return (
          <>
            <div className="job-id-container">
              <JobStatus state={row.data.state} />
              <Tooltip
                portal
                content={
                  <div
                    className="dremio-prose"
                    style={{ width: "max-content", maxWidth: "40ch" }}
                  >
                    {row.data.id}
                  </div>
                }
              >
                <div className="job-id-wrapper">
                  {renderJobLink?.(row.data)}
                </div>
              </Tooltip>
            </div>
            <CopyButton
              contents={row.data.id}
              className="copy-button"
              placement="bottom"
              copyTooltipLabel="Copy Job ID"
            />
          </>
        );
      },
      sortable: true,
    },
    {
      id: JOB_COLUMNS.user,
      renderHeaderCell: () => (
        <SortableHeaderCell columnId={JOB_COLUMNS.user}>
          {jobColumnLabels[JOB_COLUMNS.user]}
        </SortableHeaderCell>
      ),
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        return (
          <TableCellWithTooltip
            tooltipContent={row.data.user || ""}
            styles={{ maxWidth: "10vw" }}
          />
        );
      },
      sortable: true,
    },
    {
      id: JOB_COLUMNS.accelerated,
      renderHeaderCell: () => <ReflectionIcon />,
      class: "job-accelerated-cell",
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="4ch" />;
        }
        return row.data.accelerated ? <ReflectionIcon /> : "";
      },
    },
    {
      id: JOB_COLUMNS.dataset,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.dataset],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="25ch" />;
        }
        return (
          <div style={{ maxWidth: "13vw" }}>
            {renderDataset?.(row.data) ||
              `${row.data.queriedDatasets?.[0]?.datasetName || ""}`}
          </div>
        );
      },
    },
    {
      id: JOB_COLUMNS.queryType,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.queryType],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        return (
          <TableCellWithTooltip
            tooltipContent={getQueryTypeLabel(row.data)}
            styles={{ maxWidth: "15ch" }}
          />
        );
      },
    },
    {
      id: isSoftware ? JOB_COLUMNS.queue : JOB_COLUMNS.engine,
      renderHeaderCell: () =>
        jobColumnLabels[isSoftware ? JOB_COLUMNS.queue : JOB_COLUMNS.engine],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        return (
          <TableCellWithTooltip
            tooltipContent={`${
              isSoftware ? row.data.wlmQueue || "" : row.data.engine || ""
            }`}
            styles={{ maxWidth: "15ch" }}
          />
        );
      },
    },
    {
      id: JOB_COLUMNS.startTime, // default sorted
      renderHeaderCell: () => (
        <SortableHeaderCell columnId={JOB_COLUMNS.startTime}>
          {jobColumnLabels[JOB_COLUMNS.startTime]}
        </SortableHeaderCell>
      ),
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        return (
          <TimestampCellShortNoTZ timestamp={new Date(row.data.startTime)} />
        );
      },
      sortable: true,
    },
    {
      id: JOB_COLUMNS.duration,
      renderHeaderCell: () => (
        <SortableHeaderCell columnId={JOB_COLUMNS.duration}>
          {jobColumnLabels[JOB_COLUMNS.duration]}
        </SortableHeaderCell>
      ),
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        return (
          <>
            <DurationCell
              duration={row.data.duration}
              metricContent={<JobMetrics job={row.data} />}
            />
            {row.data.spilled && (
              <Tooltip
                portal
                content={t("Sonar.Job.Column.Duration.SpilledLabel")}
              >
                {/* @ts-ignore */}
                <dremio-icon
                  name="interface/disk-spill"
                  style={{ height: 20, paddingLeft: 4 }}
                />
              </Tooltip>
            )}
          </>
        );
      },
      sortable: true,
    },
    {
      id: JOB_COLUMNS.sql,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.sql],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="50ch" />;
        }
        return row.data.queryText ? renderSQL?.(row.data.queryText) : "";
      },
    },
    {
      id: JOB_COLUMNS.plannerCostEstimate,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.plannerCostEstimate],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        return (
          <NumericCell>
            {formatBytes(row.data.plannerEstimatedCost)}
          </NumericCell>
        );
      },
    },
    {
      id: JOB_COLUMNS.planningTime,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.planningTime],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        const planningPhase = row.data.durationDetails?.find(
          (phase: any) => phase.phaseName === "PLANNING",
        );
        return planningPhase ? (
          <DurationCell duration={Number(planningPhase.phaseDuration)} />
        ) : (
          ""
        );
      },
    },
    {
      id: JOB_COLUMNS.rowsScanned,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.rowsScanned],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        return (
          <NumericCell>{formatNumber(row.data.rowsScanned) || ""}</NumericCell>
        );
      },
    },
    {
      id: JOB_COLUMNS.rowsReturned,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.rowsReturned],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        return (
          <NumericCell>
            {formatNumber(row.data.outputRecords) || ""}
          </NumericCell>
        );
      },
    },
  ];
};
