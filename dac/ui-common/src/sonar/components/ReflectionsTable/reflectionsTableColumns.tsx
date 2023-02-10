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
import { IconButton, Skeleton, Tooltip } from "dremio-ui-lib/dist-esm";
import { ReflectionType } from "../ReflectionType";
import { formatBytes } from "../../../utilities/formatBytes";
import { formatDuration } from "../../../utilities/formatDuration";
import { NumericCell } from "../../../components/TableCells/NumericCell";
import { TimestampCellShortNoTZ } from "../../../components/TableCells/TimestampCell";
import { ReflectionStatus } from "../ReflectionStatus";
import { getIntlContext } from "../../../contexts/IntlContext";
import { SortableHeaderCell } from "../../../components/TableCells/SortableHeaderCell";
import { NullCell } from "../../../components/TableCells/NullCell";
import { ClickableCell } from "../../../components/TableCells/ClickableCell";

export const getReflectionColumnLabels = () => {
  const { t } = getIntlContext();
  return {
    reflectionName: t("Sonar.Reflection.Column.Name.Label"),
    reflectionType: t("Sonar.Reflection.Column.Type.Label"),
    datasetName: t("Sonar.Reflection.Column.Dataset.Label"),
    refreshStatus: t("Sonar.Reflection.Column.RefreshStatus.Label"),
    currentFootprint: t("Sonar.Reflection.Column.CurrentFootprint.Label"),
    totalFootprint: t("Sonar.Reflection.Column.TotalFootprint.Label"),
    lastRefresh: t("Sonar.Reflection.Column.LastRefreshDuration.Label"),
    refreshMethod: t("Sonar.Reflection.Column.RefreshMethod.Label"),
    availableUntil: t("Sonar.Reflection.Column.AvailableUntil.Label"),
    recordCount: t("Sonar.Reflection.Column.RecordCount.Label"),
    consideredCount: t("Sonar.Reflection.Column.ConsideredCount.Label"),
    matchedCount: t("Sonar.Reflection.Column.MatchedCount.Label"),
    acceleratedCount: t("Sonar.Reflection.Column.AcceleratedCount.Label"),
    refreshHistory: t("Sonar.Reflection.Column.RefreshHistory.Label"),
  };
};

export const reflectionsTableColumns = ({
  canViewJobs,
  onReflectionDelete,
  onRowClick,
  renderDataset,
  getHistoryLink,
}: {
  canViewJobs: boolean;
  getHistoryLink: (id: string) => string;
  onReflectionDelete: (id: string) => void;
  onRowClick: (id: string) => void;
  renderDataset: any;
}): Column<any>[] => {
  const reflectionColumnLabels = getReflectionColumnLabels();
  return [
    {
      id: "reflectionName",
      class: "leantable-sticky-column leantable-sticky-column--left",
      renderHeaderCell: () => (
        <SortableHeaderCell columnId="reflectionName">
          {reflectionColumnLabels["reflectionName"]}
        </SortableHeaderCell>
      ),
      renderCell: (row) => {
        if (!row.data) {
          return <Skeleton width="17ch" />;
        }
        return (
          <ClickableCell onClick={() => onRowClick(row.id)}>
            <div className="dremio-icon-label">
              <ReflectionStatus reflection={row.data} />
              {row.data.name}
            </div>
          </ClickableCell>
        );
      },
      sortable: true,
    },
    {
      id: "reflectionType",
      renderHeaderCell: () => (
        <SortableHeaderCell columnId="reflectionType">
          {reflectionColumnLabels["reflectionType"]}
        </SortableHeaderCell>
      ),
      renderCell: (row) =>
        row.data ? (
          <ReflectionType type={row.data.reflectionType} />
        ) : (
          <Skeleton width="8ch" />
        ),
      sortable: true,
    },
    {
      id: "datasetName",
      renderHeaderCell: () => {
        return (
          <SortableHeaderCell columnId="datasetName">
            {reflectionColumnLabels["datasetName"]}
          </SortableHeaderCell>
        );
      },
      renderCell: (row) => {
        if (!row.data) {
          return <Skeleton width="16ch" />;
        }
        return renderDataset(row);
      },

      sortable: true,
    },
    {
      id: "refreshStatus",
      renderHeaderCell: () => (
        <Tooltip
          portal
          content={
            <div
              className="dremio-prose"
              style={{ width: "max-content", maxWidth: "40ch" }}
            >
              {getIntlContext().t("Sonar.Reflection.Column.RefreshStatus.Hint")}
            </div>
          }
        >
          <span style={{ cursor: "default" }}>
            {reflectionColumnLabels["refreshStatus"]}
          </span>
        </Tooltip>
      ),
      renderCell: (row) =>
        row.data ? (
          <Tooltip
            portal
            content={
              <div
                className="dremio-prose"
                style={{ width: "max-content", maxWidth: "40ch" }}
              >
                {getIntlContext().t(
                  `Sonar.Reflection.RefreshStatus.${row.data.status.refreshStatus}.Hint`
                )}
              </div>
            }
          >
            <span style={{ cursor: "default" }}>
              {getIntlContext().t(
                `Sonar.Reflection.RefreshStatus.${row.data.status.refreshStatus}`
              )}
            </span>
          </Tooltip>
        ) : (
          <Skeleton width="11ch" />
        ),
    },
    {
      id: "currentFootprint",
      class: "leantable--align-right",
      renderHeaderCell: () => (
        <Tooltip
          portal
          content={
            <div
              className="dremio-prose"
              style={{ width: "max-content", maxWidth: "40ch" }}
            >
              {getIntlContext().t(
                "Sonar.Reflection.Column.CurrentFootprint.Hint"
              )}
            </div>
          }
        >
          <span style={{ cursor: "default" }}>
            {reflectionColumnLabels["currentFootprint"]}
          </span>
        </Tooltip>
      ),
      renderCell: (row) => (
        <NumericCell>
          {row.data ? (
            formatBytes(row.data.currentSizeBytes)
          ) : (
            <Skeleton width="9ch" />
          )}
        </NumericCell>
      ),
    },
    {
      id: "totalFootprint",
      class: "leantable--align-right",
      renderHeaderCell: () => (
        <Tooltip
          portal
          content={
            <div
              className="dremio-prose"
              style={{ width: "max-content", maxWidth: "40ch" }}
            >
              {getIntlContext().t(
                "Sonar.Reflection.Column.TotalFootprint.Hint"
              )}
            </div>
          }
        >
          <span style={{ cursor: "default" }}>
            {reflectionColumnLabels["totalFootprint"]}
          </span>
        </Tooltip>
      ),
      renderCell: (row) => (
        <NumericCell>
          {row.data ? (
            formatBytes(row.data.totalSizeBytes)
          ) : (
            <Skeleton width="9ch" />
          )}
        </NumericCell>
      ),
    },
    {
      id: "lastRefresh",
      class: "leantable--align-right",
      renderHeaderCell: () => (
        <Tooltip
          portal
          content={
            <div
              className="dremio-prose"
              style={{ width: "max-content", maxWidth: "40ch" }}
            >
              {getIntlContext().t(
                "Sonar.Reflection.Column.LastRefreshDuration.Hint"
              )}
            </div>
          }
        >
          <span style={{ cursor: "default" }}>
            {reflectionColumnLabels["lastRefresh"]}
          </span>
        </Tooltip>
      ),
      renderCell: (row) => (
        <NumericCell>
          {row.data ? (
            formatDuration(row.data.status.lastRefreshDurationMillis)
          ) : (
            <Skeleton width="9ch" />
          )}
        </NumericCell>
      ),
    },
    {
      id: "refreshMethod",
      renderHeaderCell: () => (
        <Tooltip
          portal
          content={
            <div
              className="dremio-prose"
              style={{ width: "max-content", maxWidth: "40ch" }}
            >
              {getIntlContext().t("Sonar.Reflection.Column.RefreshMethod.Hint")}
            </div>
          }
        >
          <span style={{ cursor: "default" }}>
            {reflectionColumnLabels["refreshMethod"]}
          </span>
        </Tooltip>
      ),
      renderCell: (row) =>
        row.data ? (
          getIntlContext().t(
            `Sonar.Reflection.RefreshMethod.${row.data.status.refreshMethod}`
          )
        ) : (
          <Skeleton width="9ch" />
        ),
    },
    {
      id: "availableUntil",
      class: "leantable--align-right",
      renderHeaderCell: () => (
        <Tooltip
          portal
          content={
            <div
              className="dremio-prose"
              style={{ width: "max-content", maxWidth: "40ch" }}
            >
              {getIntlContext().t(
                "Sonar.Reflection.Column.AvailableUntil.Hint"
              )}
            </div>
          }
        >
          <span style={{ cursor: "default" }}>
            {reflectionColumnLabels["availableUntil"]}
          </span>
        </Tooltip>
      ),
      renderCell: (row) =>
        row.data ? (
          row.data.status.expiresAt === null ? (
            <NullCell />
          ) : (
            <TimestampCellShortNoTZ
              timestamp={new Date(row.data.status.expiresAt)}
              applyFormat={(value) => value.replace(",", "")}
            />
          )
        ) : (
          <NumericCell>
            <Skeleton width="23ch" />
          </NumericCell>
        ),
    },
    {
      id: "recordCount",
      class: "leantable--align-right",
      renderHeaderCell: () => (
        <Tooltip
          portal
          content={
            <div
              className="dremio-prose"
              style={{ width: "max-content", maxWidth: "40ch" }}
            >
              {getIntlContext().t("Sonar.Reflection.Column.RecordCount.Hint")}
            </div>
          }
        >
          <span style={{ cursor: "default" }}>
            {reflectionColumnLabels["recordCount"]}
          </span>
        </Tooltip>
      ),
      renderCell: (row) => (
        <NumericCell>
          {row.data ? (
            row.data.outputRecords === -1 ? (
              <NullCell />
            ) : (
              row.data.outputRecords.toLocaleString()
            )
          ) : (
            <Skeleton width="6ch" />
          )}
        </NumericCell>
      ),
    },
    {
      id: "consideredCount",
      class: "leantable--align-right",
      renderHeaderCell: () => (
        <Tooltip
          portal
          content={
            <div
              className="dremio-prose"
              style={{ width: "max-content", maxWidth: "40ch" }}
            >
              {getIntlContext().t(
                "Sonar.Reflection.Column.ConsideredCount.Hint"
              )}
            </div>
          }
        >
          <span style={{ cursor: "default" }}>
            {reflectionColumnLabels["consideredCount"]}
          </span>
        </Tooltip>
      ),
      renderCell: (row) => (
        <NumericCell>
          {row.data ? (
            canViewJobs ? (
              <a href={row.data.consideredJobsLink} target="_blank">
                {row.data.consideredCount.toLocaleString()}
              </a>
            ) : (
              row.data.consideredCount.toLocaleString()
            )
          ) : (
            <Skeleton width="3ch" />
          )}
        </NumericCell>
      ),
    },
    {
      id: "matchedCount",
      class: "leantable--align-right",
      renderHeaderCell: () => (
        <Tooltip
          portal
          content={
            <div
              className="dremio-prose"
              style={{ width: "max-content", maxWidth: "40ch" }}
            >
              {getIntlContext().t("Sonar.Reflection.Column.MatchedCount.Hint")}
            </div>
          }
        >
          <span style={{ cursor: "default" }}>
            {reflectionColumnLabels["matchedCount"]}
          </span>
        </Tooltip>
      ),
      renderCell: (row) => (
        <NumericCell>
          {row.data ? (
            canViewJobs ? (
              <a href={row.data.matchedJobsLink} target="_blank">
                {row.data.matchedCount.toLocaleString()}
              </a>
            ) : (
              row.data.matchedCount.toLocaleString()
            )
          ) : (
            <Skeleton width="3ch" />
          )}
        </NumericCell>
      ),
    },
    {
      id: "acceleratedCount",
      class: "leantable--align-right",
      renderHeaderCell: () => {
        return (
          <Tooltip
            portal
            content={
              <div
                className="dremio-prose"
                style={{ width: "max-content", maxWidth: "40ch" }}
              >
                {getIntlContext().t(
                  "Sonar.Reflection.Column.AcceleratedCount.Hint"
                )}
              </div>
            }
          >
            <span style={{ cursor: "default" }}>
              {reflectionColumnLabels["acceleratedCount"]}
            </span>
          </Tooltip>
        );
      },
      renderCell: (row) => (
        <NumericCell>
          {row.data ? (
            canViewJobs ? (
              <a href={row.data.chosenJobsLink} target="_blank">
                {row.data.chosenCount.toLocaleString()}
              </a>
            ) : (
              row.data.chosenCount.toLocaleString()
            )
          ) : (
            <Skeleton width="3ch" />
          )}
        </NumericCell>
      ),
    },
    {
      id: "refreshHistory",
      renderHeaderCell: () => reflectionColumnLabels["refreshHistory"],
      renderCell: (row) => {
        if (!row.data) {
          return <Skeleton width="7ch" />;
        }
        return (
          <a href={getHistoryLink(row.data.id)} target="_blank">
            {getIntlContext().t("Sonar.Reflection.Column.RefreshHistory.Link")}
          </a>
        );
      },
    },
    {
      id: "actions",
      class:
        "leantable-row-hover-visibility leantable-sticky-column leantable-sticky-column--right",
      renderHeaderCell: () => null,
      renderCell: (row) =>
        row.data && row.data.isCanAlter ? (
          <div className="dremio-button-group">
            <IconButton
              tooltip="Delete"
              tooltipPortal
              tooltipDelay={500}
              onClick={() => onReflectionDelete(row.id)}
            >
              {/* @ts-ignore */}
              <dremio-icon name="interface/delete"></dremio-icon>
            </IconButton>
          </div>
        ) : null,
    },
  ];
};
