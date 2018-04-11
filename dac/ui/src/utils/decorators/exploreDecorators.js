/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import Immutable from 'immutable';
import { HIGHLIGHTED_TABLE, PALE_ORANGE } from 'uiTheme/radium/colors';

const columnUIProps = Immutable.Map({
  width: 0,
  hidden: false
});

function getColumnColor(code) {
  const hash = {
    'HIGHLIGHTED': HIGHLIGHTED_TABLE,
    'DELETED': PALE_ORANGE,
    'DELETION_MARKER': PALE_ORANGE
  };
  return hash[code] || 'none';
}

export function getColumnStatus(response, column) {
  const highlightedColumns = response.get('highlightedColumns');
  const deletedColumns = response.get('deletedColumns');
  const deletionMarkerColumns = response.get('rowDeletionMarkerColumns');
  if (deletionMarkerColumns.indexOf(column.get('name')) !== -1) {
    return 'DELETION_MARKER';
  }
  if (highlightedColumns.indexOf(column.get('name')) !== -1) {
    return 'HIGHLIGHTED';
  }
  if (deletedColumns.indexOf(column.get('name')) !== -1) {
    return 'DELETED';
  }
  return 'ORIGINAL';
}

export function isRowDeleted(row, columns) {
  const deletedColumns = columns.filter(column => column.get('status') === 'DELETION_MARKER');
  return deletedColumns.some(column => {
    return row.get('row').get(column.get('index')).get('v') === null;
  });
}

export function decorateTable(response) {
  const table = response.get('data');
  const columns = table.get('columns').map(column => {
    const status = getColumnStatus(response, column);
    return column.merge(columnUIProps).set('status', status).set('color', getColumnColor(status));
  });
  const rows = table.get('rows').map((row) => {
    return row.set('isDeleted', isRowDeleted(row, columns));
  });
  return table.set('columns', columns).set('rows', rows);
}

export const decorateHistory = (history) => {
  const historyItems = history.get('items') || Immutable.List();
  return history.set('items', historyItems.reverse());
};
