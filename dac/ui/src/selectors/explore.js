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
import { createSelector } from "reselect";
import Immutable from "immutable";
import { getModuleState } from "@app/selectors/moduleState";
import { getLocation } from "selectors/routing";
import { getExploreViewState, getEntity } from "selectors/resources";
import {
  splitFullPath,
  getRouteParamsFromLocation,
  constructFullPathAndEncode,
} from "utils/pathUtils";
import { isNewQueryUrl } from "@app/utils/explorePageTypeUtils";

const emptyTable = Immutable.fromJS({
  columns: [],
  rows: [],
});

function getJoinTableData(state) {
  // here data should be mutable for better perfomance
  const { entities } = state.resources;
  const joinVersion = getExploreState(state).join.getIn([
    "custom",
    "joinVersion",
  ]);

  const table = entities.getIn(["tableData", joinVersion]);

  if (table) {
    return Immutable.Map({
      rows: table.get("rows"),
      columns: table.get("columns"),
    });
  }
  return emptyTable;
}

export function getTableDataRaw(state, datasetVersion) {
  const { entities } = state.resources;
  return entities.getIn(["tableData", datasetVersion]);
}

function getTableData(state, datasetVersion) {
  return getTableDataRaw(state, datasetVersion) || emptyTable;
}

export function getColumnFilter(state, version) {
  const { entities } = state.resources;
  return entities.getIn(["tableData", version, "columnFilter"]) || "";
}

export function getJobProgress(state, version) {
  const { entities } = state.resources;
  return entities.getIn(["tableData", version, "jobProgress"]) || null;
}

export function getRunStatus(state) {
  const { entities } = state.resources;
  return entities.getIn(["tableData", "jobProgress"]) || { isRun: undefined };
}

export function getJobOutputRecords(state, version) {
  const jobProgress = getJobProgress(state, version);
  const datasetVersion = jobProgress && jobProgress.datasetVersion;
  const tableData = getTableDataRaw(state, datasetVersion);
  return tableData && tableData.get("outputRecords");
}

export function getPeekData(state, previewVersion) {
  const { entities } = state.resources;
  return entities.getIn(["previewTable", previewVersion]) || emptyTable;
}

export const getFullDataset = (state, datasetVersion) =>
  state.resources.entities.getIn(["fullDataset", datasetVersion]);

export function getPaginationUrl(state, datasetVersion) {
  const { entities } = state.resources;
  const paginationUrl = entities.getIn([
    "fullDataset",
    datasetVersion,
    "paginationUrl",
  ]);
  return paginationUrl || datasetVersion;
}

export function oldGetExploreJobId(state) {
  // this selector will have to change once we move jobId out of fullDataset and load it prior to metadata
  const location = getLocation(state);
  const version = getDatasetVersionFromLocation(location);
  const fullDataset = getFullDataset(state, version);
  return fullDataset ? fullDataset.getIn(["jobId", "id"], "") : "";
}

export function getPaginationJobId(state, datasetVersion) {
  const { entities } = state.resources;
  return entities.getIn(["fullDataset", datasetVersion, "jobId", "id"]);
}

export function getApproximate(state, datasetVersion) {
  const { entities } = state.resources;
  return entities.getIn(["fullDataset", datasetVersion, "approximate"]);
}

export const getDatasetVersionFromLocation = (location) =>
  location.query && location.query.version;

function _getDatasetFromLocation(state, location) {
  return getDatasetData(state, getDatasetVersionFromLocation(location));
}

export const getDatasetEntityId = (state, location) => {
  const datasetVersion = getDatasetVersionFromLocation(location);
  const dataset = getDataset(state, datasetVersion);
  return dataset ? dataset.get("entityId") : null;
};

export const getHistoryFromLocation = (state, location) => {
  const { query } = location || {};
  //tipVersion is used to get history. If by some reason tipVersion is not provided use
  // version (data set version) as fallback
  return getHistory(state, query.tipVersion || query.version);
};

// For now it is decided to show wiki and graph links only in following cases
// 1) Dataset edit mode - show
// 2) Creation of vds based on other data set - show only if we in original data set version (last
// history dot)
export const isWikAvailable = (state, location) => {
  const history = getHistoryFromLocation(state, location);
  const lastItemId = history ? history.get("items").last() : null;
  const {
    query: { version, mode },
  } = location;

  return (
    getDatasetEntityId(state, location) &&
    (mode === "edit" || version === lastItemId)
  );
};

function getDatasetData(state, version) {
  const { entities } = state.resources;
  const dataset = entities.getIn(["datasetUI", version]);
  return dataset || undefined;
}

const getQueryContext = (state) => {
  const location = getLocation(state);
  return location.query && location.query.context;
};

const makeNewDataset = (context) => {
  return Immutable.fromJS({
    isNewQuery: true,
    fullPath: ["tmp", "UNTITLED"],
    displayFullPath: ["tmp", "New Query"],
    context: context ? splitFullPath(context).map(decodeURIComponent) : [],
    sql: "",
    datasetType: "SCRIPT",
    apiLinks: {
      self: "/dataset/tmp/UNTITLED/new_untitled_sql",
    },
    needsLoad: false,
  });
};

export const getNewDatasetFromState = createSelector(
  [getQueryContext],
  makeNewDataset,
);

const getInitialDataset = (location, viewState) => {
  const routeParams = getRouteParamsFromLocation(location);
  const version = location.query.version || location.query.tipVersion;
  const displayFullPath =
    viewState.getIn(["error", "details", "displayFullPath"]) ||
    (routeParams.resourceId !== "undefined"
      ? [routeParams.resourceId, ...splitFullPath(routeParams.tableId)] //Handle old /tmp/tmp/UNTITLED URL
      : ["tmp", "UNTITLED"]);

  const fullPath =
    location.query.mode === "edit" ? displayFullPath : ["tmp", "UNTITLED"];

  return Immutable.fromJS({
    fullPath,
    displayFullPath,
    sql: viewState.getIn(["error", "details", "sql"]) || "",
    context: viewState.getIn(["error", "details", "context"]) || [],
    datasetVersion: version,
    datasetType: viewState.getIn(["error", "details", "datasetType"]),
    links: {
      self: location.pathname + "?version=" + encodeURIComponent(version),
    },
    apiLinks: {
      self:
        `/dataset/${constructFullPathAndEncode(fullPath)}` +
        (version ? `/version/${encodeURIComponent(version)}` : ""),
    },
    needsLoad: true,
  });
};

export const getIntialDatasetFromState = createSelector(
  [getLocation, getExploreViewState],
  getInitialDataset,
);

export const getExplorePageDataset = (state, curDataset) => {
  const location = getLocation(state);
  const { query } = location || {};
  const isNewQuery = !curDataset && isNewQueryUrl(location);
  const curQuery = curDataset ? curDataset : query;

  let dataset;
  if (isNewQuery) {
    dataset = getNewDatasetFromState(state);
  } else {
    dataset = getDataset(state, curQuery.version);
    if (dataset) {
      const fullDataset = getEntity(state, curQuery.version, "fullDataset");
      dataset = dataset.set(
        "needsLoad",
        Boolean(fullDataset && fullDataset.get("error")),
      );
    } else {
      dataset = getIntialDatasetFromState(state);
    }
  }

  if (curQuery.jobId) {
    dataset = dataset.set("jobId", curQuery.jobId);
  }
  dataset = dataset.set(
    "tipVersion",
    curQuery.tipVersion || curQuery.version || dataset.get("datasetVersion"),
  );

  // workaround for a routing issue where failed jobs in other scripts don't update query path correctly
  if (!dataset.get("datasetType")) {
    dataset = dataset.set(
      "datasetVersion",
      curQuery.version || dataset.get("datasetVersion"),
    );
  }

  return dataset;
};

export function getHistoryData(state, id) {
  return state.resources.entities.getIn(["history", id]);
}

export function getHistoryItem(state, id) {
  return state.resources.entities.getIn(["historyItem", id]);
}

export function getHistoryItemsForHistoryId(state, id) {
  const history = state.resources.entities.getIn(["history", id]);
  if (!history) return Immutable.List();
  return history.get("items").map((itemId) => getHistoryItem(state, itemId));
}

export const getImmutableTable = createSelector([getTableData], (table) => {
  return table;
});

export const getJoinTable = createSelector([getJoinTableData], (table) => {
  return table;
});

export const getTableColumns = createSelector([getTableData], (table) => {
  return table.get("columns") || emptyTable.get("columns");
});

export const getDataset = createSelector([getDatasetData], (dataset) => {
  return dataset;
});

export const getDatasetFromLocation = createSelector(
  [_getDatasetFromLocation],
  (dataset) => {
    return dataset;
  },
);

export const getHistory = createSelector(
  [getHistoryData],
  (history) => history,
);

export const getHistoryItems = createSelector(
  [getHistoryItemsForHistoryId],
  (historyItems) => historyItems,
);

export function getCurrentEngine(state) {
  const exploreState = getExploreState(state);
  return exploreState.view.currentEngin;
}

export const exploreStateKey = "explorePage"; // a key that would be used for dynamic redux state
export const getExploreState = (state) =>
  getModuleState(state, exploreStateKey);
export const getCurrentRouteParams = (state) => {
  const exploreState = getExploreState(state);
  return exploreState ? exploreState.currentRouteState : null;
};

export const selectTabView = (state, scriptId) => {
  const tabViews = getExploreState(state)?.tabViews;
  if (!tabViews) return null;
  return tabViews[scriptId];
};

const selectMultiQueryDataset = (view, queryTabNumber = 1) => {
  const { queryStatuses = [] } = view || {};
  return queryStatuses[queryTabNumber - 1];
};

export const selectTabDataset = (state, scriptId) => {
  const metadata = selectMultiQueryDataset(selectTabView(state, scriptId));
  return metadata ? getExplorePageDataset(state, metadata) : null;
};
