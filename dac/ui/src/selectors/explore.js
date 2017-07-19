/*
 * Copyright (C) 2017 Dremio Corporation
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
import { createSelector } from 'reselect';
import Immutable from 'immutable';

const emptyTable = Immutable.fromJS({
  columns: [],
  rows: []
});

function getJoinTableData(state, props) {
  // here data should be mutable for better perfomance
  const { entities } = state.resources;
  const joinVersion = state.explore.join.getIn(['custom', 'joinVersion']);

  const table = entities.getIn(['tableData', joinVersion]);

  if (table) {
    return Immutable.Map({
      rows: table.get('rows'),
      columns: table.get('columns')
    });
  }
  return emptyTable;
}

function getTableData(state, datasetVersion) {
  const { entities } = state.resources;
  return entities.getIn(['tableData', datasetVersion]) || emptyTable;
}

export function getPeekData(state, previewVersion) {
  const { entities } = state.resources;
  return entities.getIn(['previewTable', previewVersion])  || emptyTable;
}

export function getTableViewData(state, datasetVersion) {
  return state.explore.view.getIn(['tables', datasetVersion]) || Immutable.Map();
}

export function getTransformViewData(state, href) {
  return state.explore.view.getIn(['transform', href]);
}

export function getPaginationUrl(state, datasetVersion) {
  const { entities } = state.resources;
  const paginationUrl = entities.getIn(['fullDataset', datasetVersion, 'paginationUrl']);
  return paginationUrl || datasetVersion;
}

export function getApproximate(state, datasetVersion) {
  const { entities } = state.resources;
  return entities.getIn(['fullDataset', datasetVersion, 'approximate']);
}

function _getDatasetFromLocation(state, location) {
  return getDatasetData(state, location.query && location.query.version);
}

function getDatasetData(state, version) {
  const { entities } = state.resources;
  const dataset = entities.getIn(['datasetUI', version]);
  return dataset || undefined;
}

export function getHistoryData(state, id) {
  return state.resources.entities.getIn(['history', id]);
}

export function getHistoryItem(state, id) {
  return state.resources.entities.getIn(['historyItem', id]);
}

export function getHistoryItemsForHistoryId(state, id) {
  const history = state.resources.entities.getIn(['history', id]);
  if (!history) return Immutable.List();
  return history.get('items').map((itemId) => getHistoryItem(state, itemId));
}

export const getTable = createSelector(
  [ getTableData ],
  table => {
    return table;
  }
);

export const getImmutableTable = createSelector(
  [ getTableData ],
  table => {
    return table;
  }
);

export const getJoinTable = createSelector(
  [ getJoinTableData ],
  table => {
    return table;
  }
);

export const getTableColumns = createSelector(
  [ getTableData ],
  table => {
    return table.get('columns');
  }
);

export const getDataset = createSelector(
  [ getDatasetData ],
  dataset => {
    return dataset;
  }
);

export const getDatasetFromLocation = createSelector(
  [ _getDatasetFromLocation ],
  dataset => {
    return dataset;
  }
);

export const getHistory = createSelector(
  [ getHistoryData ],
  history => history
);

export const getHistoryItems = createSelector(
  [ getHistoryItemsForHistoryId ],
  historyItems => historyItems
);
