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
import $ from 'jquery';
import uuid from 'uuid';
import { HISTORY_PANEL_SIZE } from 'uiTheme/radium/sizes';
import { HIGHLIGHTED_TABLE, PALE_ORANGE } from 'uiTheme/radium/colors';

const MARGIN_LEFT = 0;

const MIN_WIDTH = 150;

class GridMap {
  // TODO: Andrey: Old table not being used.
  // "isRunInTest" is needed only for run unit tests in browser
  // when we run tests in browser, we have "window"
  // but for tests we need static window.width() (see 29 line)
  mapJson(oldTable, json, numOfTables, isRunInTest) {
    // this color value is used to process split rows, but can have future additions.
    // it can be changed later to color other operations.
    function getColumnColor(code) {
      const hash = {
        'HIGHLIGHTED': HIGHLIGHTED_TABLE,
        'DELETED': PALE_ORANGE,
        'DELETION_MARKER': PALE_ORANGE
      };
      return hash[code] || 'none';
    }
    const result = {};
    let index = 0;
    const windowWidth = !isRunInTest && typeof window !== 'undefined' && $(window).width() || 1280;
    result.width = windowWidth / (numOfTables) - (HISTORY_PANEL_SIZE / numOfTables);
    if (numOfTables > 1) {
      result.width -= MARGIN_LEFT;
    }
    const jsonData = json.data ? json.data : json;
    result.columns = jsonData && jsonData.columns && jsonData.columns.map((value) => {
      const defaultWidth = result.width / Math.max(jsonData.columns.length, 1);
      return {
        name: value.name,
        type: value.type,
        defaultWidth: defaultWidth < MIN_WIDTH
          ? MIN_WIDTH
          : defaultWidth,
        width: 0,
        height: 0,
        visible: true,
        status: value.status,
        color: getColumnColor(value.status),
        index: index++
      };
    });
    result.rows = jsonData.rows;
    return result;
  }

  mapTableConfig(json) {
    return {
      name: json.datasetConfig.name,
      sql: json.datasetConfig.sql,
      version: json.datasetConfig.version,
      state: {} // TODO will be improved in future
    };
  }

  mapGridRows(rows) {
    const res = [];
    rows.forEach((row) => {
      for (const key in row) {
        const newRow = {
          columnName: key,
          rowName: row[key]
        };
        res.push(newRow);
      }
    });
    return res;
  }

  mapSelections(json) {
    return json.map((item) => {
      return {
        id: item.id,
        pattern: item.pattern,
        matchNumber: item.matchNumber,
        unmatchNumber: item.unmatchNumber,
        examples: item.examples,
        hits: item.hits
      };
    });
  }

  mapDrop() {
    return [
      {
        id: 'ExtractID',
        transform: 'extract',
        name: 'Extract'
      },
      {
        id: 'ReplaceID',
        transform: 'replace',
        name: 'Replace'
      },
      {
        id: 'SplitID',
        transform: 'split',
        name: 'Split'
      },
      {
        id: 'KeepID',
        transform: 'keeponly',
        name: 'Keep Only'
      },
      {
        id: 'ExcludeID',
        transform: 'exclude',
        name: 'Exclude'
      }
    ];
  }

  mapJoinRows(json) {
    return this.mapGridRows(json);
  }

  mapHelpFunctions(json) {
    const result = {};
    result.funcs = json && json.map((item) => {
      return {
        id: uuid.v4(),
        ...item
      };
    });
    result.helpSimilar = json && json.helpSimilar && json.helpSimilar.map((item) => {
      return {
        id: item.id,
        percents: item.percents,
        name: item.name,
        path: item.path,
        owner: item.owner,
        columns: item.columns
      };
    });
    this.sortFunctions(result);
    return Immutable.fromJS(result);
  }

  sortFunctions(result) {
    result.funcs.sort((a, b) => {
      // some function should be wrapped in double quotes in SQL Editor (e.g. CORR, LEFT, RIGHT)
      // but when we sort array of func, we need to ignore double quotes
      const curFuncName = a.name.replace(/"/g, '');
      const nextFuncName = b.name.replace(/"/g, '');

      if (curFuncName < nextFuncName) {
        return -1;
      }
      if (curFuncName > nextFuncName) {
        return 1;
      }
      return 0;
    });
  }

  mapConfig(json) {
    return Immutable.fromJS({
      id: json.id,
      query: json.query
    });
  }

  mapSearch(old, newRows) {
    old.rows = newRows.map((row) => {
      const arr = [];
      for (const key in row) {
        arr.push(row[key]);
      }
      return arr;
    });
    return  Immutable.fromJS(old);
  }

  _mapLayer(layer, nextLayerKey, layers) {
    return layer.map((child, index) => {
      return {
        path: child.path,
        name: child.name,
        type: child.type,
        connectedToThis: this._getNumberOfItemsConnectedToItem(layers, child.path),
        parentIds: child.parents.map((parent) => {
          return nextLayerKey !== 'nextChildren'
            ? parent.path
            : 'nextChildren-' + index;
        }),
        fields: {
          owner: child.owner,
          created: child.created,
          queries: child.queries,
          descendants: child.descendants,
          columns: child.columns
        }
      };
    });
  }

  _getNumberOfItemsConnectedToItem(layers, path) {
    let count = 0;
    for (const key in layers) {
      const wrapLayers = layers;
      if (!(wrapLayers[key] instanceof Array)) {
        wrapLayers[key] = [wrapLayers[key]];
      }
      wrapLayers[key].forEach((item) => {
        if (!item.parents) {
          return null;
        }
        const parents = item.parents.map((parent) => {
          return parent.path;
        });
        if (parents.indexOf(path) !== -1) {
          count++;
        }
      });
    }
    return count;
  }

  mapJoinRecommended(json) {
    return json.map((item) => {
      return {
        current: item.first,
        next: item.second,
        type: item.type
      };
    });
  }
  /**
   * Map data to valid format to send to server
   * @param  {Immutable.List} columns Selected columns in format [{String: String, String: String}]
   * @param  {String}         dpath   path to current table
   * @param  {String}         current Current active table name
   * @param  {String}         next    Join table name
   * @return {Object} valid object to send to java
   */
  mapJoinDataToSend(columns, dpath, current, next) {
    return {
      dpath,
      columns: columns.map(column => {
        return {
          [current]: column.get('cur'),
          [next]: column.get('next')
        };
      }).toJS()
    };
  }

  mapGroupByDataToSend(groupColumns, valueColumns) {
    return {
      groupColumns: groupColumns.map(column => column.get('name')).toJS(),
      valueColumns: valueColumns.map(column => {
        return {
          name: column.get('name'),
          type: column.get('type')
        };
      }).toJS()
    };
  }

  mapSearchDatasets(json) {
    return json.map((item) => {
      return {
        name: item.name,
        path: item.path,
        search: item.criteria
      };
    });
  }

  mapNextLevelOfColumn() {
    // TODO add real api request
    return [
      {
        defaultWidth: 445,
        height: 0,
        index: 0,
        name: 'phone',
        type: 'A',
        visible: true,
        width: 0,
        isFile: 'file'
      },
      {
        defaultWidth: 445,
        height: 0,
        index: 1,
        name: 'phone2',
        type: 'A',
        visible: true,
        width: 0,
        isFile: 'file'
      },
      {
        defaultWidth: 445,
        height: 0,
        index: 2,
        name: 'email',
        type: 'A',
        visible: true,
        width: 0,
        isFile: 'file'
      },
      {
        defaultWidth: 445,
        height: 0,
        index: 3,
        name: 'fax',
        type: 'A',
        visible: true,
        width: 0,
        isFile: 'file'
      }
    ];
  }

}

const gridMap = new GridMap();

export default gridMap;
