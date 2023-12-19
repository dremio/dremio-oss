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
import { TableCellWithTooltip } from "./jobs-table-utils";
import { TimestampCellShortNoTZ } from "../../../components/TableCells/TimestampCell";
import { DurationCell } from "../../../components/TableCells/DurationCell";
import { JOB_COLUMNS, getJobColumnLabels } from "./jobsPageTableColumns";

export const reflectionJobsPageTableColumns = (
  renderJobLink?: (job: any) => React.ReactElement,
  renderDataset?: (job: any) => React.ReactElement,
  renderSQL?: (sql: string) => React.ReactElement
): Column<any>[] => {
  const jobColumnLabels = getJobColumnLabels();
  const { t } = getIntlContext();
  return [
    {
      id: JOB_COLUMNS.jobId,
      class:
        "job-id-and-status leantable-sticky-column leantable-sticky-column--left",
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.jobId],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        return (
          <>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
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
    },
    {
      id: JOB_COLUMNS.user,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.user],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="20ch" />;
        }
        return (
          <TableCellWithTooltip
            tooltipContent={row.data.user || ""}
            styles={{ maxWidth: "20ch" }}
          />
        );
      },
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
        const datasetPathList = row.data.datasetPathList;
        const datasetName = datasetPathList[datasetPathList.length - 1];
        return (
          <div
            style={{
              maxWidth: "25ch",
              width: "max-content",
            }}
          >
            {renderDataset?.({
              ...row.data,
              queriedDatasets: [
                {
                  datasetName: datasetName,
                  datasetPath: datasetPathList.join("."),
                  datasetPathsList: datasetPathList,
                  datasetType: row.data.datasetType,
                },
              ],
            }) || `${datasetName || ""}`}
          </div>
        );
      },
    },
    {
      id: JOB_COLUMNS.startTime, // default sorted
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.startTime],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        return (
          <TimestampCellShortNoTZ timestamp={new Date(row.data.startTime)} />
        );
      },
    },
    {
      id: JOB_COLUMNS.duration,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.duration],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        const duration = Math.abs(row.data.startTime - row.data.endTime);
        return (
          <>
            <DurationCell duration={duration} />
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
    },
    {
      id: JOB_COLUMNS.sql,
      renderHeaderCell: () => jobColumnLabels[JOB_COLUMNS.sql],
      renderCell: (row: any) => {
        if (!row.data) {
          return <Skeleton width="50ch" />;
        }
        return row.data.description ? renderSQL?.(row.data.description) : "";
      },
    },
  ];
};
