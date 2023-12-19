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

import {
  jobsPageTableColumns,
  JOB_COLUMNS,
  getJobColumnLabels,
  //@ts-ignore
} from "dremio-ui-common/sonar/components/JobsTable/jobsPageTableColumns.js";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import { isNotSoftware } from "dyn-load/utils/versionUtils";
import { isEqual } from "lodash";

export enum GenericFilters {
  jst = "jst",
  qt = "qt",
  usr = "usr",
  st = "st",
  qn = "qn",
}

export const FILTER_LABEL_IDS = {
  [GenericFilters.jst]: "Common.Status",
  [GenericFilters.qt]: "Common.Type",
  [GenericFilters.usr]: "Common.User",
  [GenericFilters.qn]: "Common.Queue",
  [GenericFilters.st]: "",
};

export const itemsForStateFilter = [
  { id: "SETUP", label: "Setup", icon: "job-state/setup" },
  { id: "QUEUED", label: "Queued", icon: "job-state/queued" },
  { id: "ENGINE_START", label: "Engine Start", icon: "job-state/engine-start" },
  { id: "RUNNING", label: "Running", icon: "job-state/running" },
  { id: "COMPLETED", label: "Completed", icon: "job-state/completed" },
  { id: "CANCELED", label: "Canceled", icon: "job-state/canceled" },
  { id: "FAILED", label: "Failed", icon: "job-state/failed" },
];

export const itemsForQueryTypeFilter = [
  { id: "UI", label: "UI", default: true },
  { id: "EXTERNAL", label: "External Tools", default: true },
  { id: "ACCELERATION", label: "Accelerator", default: false },
  { id: "INTERNAL", label: "Internal", default: false },
  { id: "DOWNLOAD", label: "Downloads", default: false },
];

const disabledColumns = [
  JOB_COLUMNS.accelerated,
  JOB_COLUMNS.user,
  JOB_COLUMNS.startTime,
  JOB_COLUMNS.queryType,
  JOB_COLUMNS.jobId,
];

const nonDefaultSelectedColumns = [
  JOB_COLUMNS.plannerCostEstimate,
  JOB_COLUMNS.planningTime,
  JOB_COLUMNS.rowsScanned,
  JOB_COLUMNS.rowsReturned,
];

export const getShowHideColumns = () => {
  const cols = localStorageUtils?.getJobColumns();
  const tableCols = jobsPageTableColumns(
    undefined,
    undefined,
    undefined,
    !isNotSoftware()
  );
  const isValidColIds = isEqual(
    (cols || []).map(({ id }: any) => id),
    tableCols.map(({ id }) => id)
  );

  if (
    cols &&
    cols.find((col: any) => col.id === JOB_COLUMNS.jobId) &&
    isValidColIds
  ) {
    return cols;
  } else {
    localStorageUtils?.clearJobColumns();
    const newCols = tableCols.map((col: any, idx: number) => ({
      id: col.id,
      disabled: disabledColumns.includes(col.id),
      sort: idx,
      selected: !nonDefaultSelectedColumns.includes(col.id),
    }));
    return newCols;
  }
};

export const transformToItems = (columns: any[]) => {
  const jobColumnLabels = getJobColumnLabels();
  return columns.map((col: any) => ({
    key: col.id,
    id: col.id,
    label: jobColumnLabels[col.id as keyof typeof JOB_COLUMNS],
    disableSort: undefined,
    isSelected: col.selected,
    width: 122,
    height: 40,
    flexGrow: "0",
    flexShrink: "0",
    isFixedWidth: false,
    isDraggable: col.id !== JOB_COLUMNS.jobId,
    headerClassName: "",
    disabled: col.disabled,
  }));
};

export const transformToSelectedItems = (columns: any[]) => {
  return columns.filter((col: any) => col.selected).map((col: any) => col.id);
};
