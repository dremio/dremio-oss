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
import { findIndex } from 'lodash/array';
import uuid from 'uuid';
import { HIGHLIGHTED_TABLE, PALE_ORANGE } from 'uiTheme/radium/colors';

const DISTANCE_BETWEEN_NEW_COLUMNS = 5;

// todo: loc

class Transforms {
  constructor() {
    this.defaultTransform = this.defaultTransform.bind(this);
    this.previewTransforms = this.previewTransforms.bind(this);
    this.addCalculatedField = this.addCalculatedField.bind(this);
  }

  dropColumn(table, item) {
    const colName = item.droppedColumnName;
    const columns = table.get('columns').filter(col => col.get('name') !== colName);
    return table.set('columns', columns);
  }

  renameColumn(table, item) {
    const { oldColumnName, newColumnName } = item;
    const colIndex = findIndex(table.get('columns'), col => col.get('name') === oldColumnName);
    const oldColumn = table.get('columns').get(colIndex);
    return table.setIn(['columns', colIndex], oldColumn.set('name', newColumnName));
  }

  _getColumnsWithoutPreviewColumns(columns) {
    return columns.filter(col => col.get('status') !== 'HIGHLIGHTED')
                  .map(col => col.set('color', undefined));
  }

  _getIndexOfPreviewColumn(columns) {
    return columns.findIndex(col => col.get('status') === 'HIGHLIGHTED');
  }

  _getColumnsWithPreviewColumn(cols, sourceColumnName, newColumnName, colIndex) {
    let columns = cols;
    const oldColumn = columns.get(colIndex);
    columns = columns.set(colIndex, oldColumn.set('color', PALE_ORANGE));
    const newColName = sourceColumnName === newColumnName ? `${newColumnName} (new)` : newColumnName;
    const colToInsert = oldColumn.set('color', HIGHLIGHTED_TABLE).set('name', newColName).set('status', 'HIGHLIGHTED');
    columns = columns.splice(colIndex + 1, 0, colToInsert);

    return columns.map( (col, index) => col.set('index', index));
  }

  _getRowsForPreview(rows, addedByUiColumnIndex, colIndex) {
    return rows.map(row => {
      let tmpRow = row;
      if (addedByUiColumnIndex !== -1) {
        tmpRow = tmpRow.splice(addedByUiColumnIndex, 1);
      }
      return tmpRow.splice(colIndex + 1, 0, 'loading…');
    });
  }

  defaultTransform(table, item) {
    const { sourceColumnName, newColumnName } = item;
    const addedByUiColumnIndex = this._getIndexOfPreviewColumn(table.get('columns'));
    let columns = this._getColumnsWithoutPreviewColumns(table.get('columns'));

    const colIndex = findIndex(columns, col => col.get('name') === sourceColumnName);
    columns = this._getColumnsWithPreviewColumn(columns, sourceColumnName, newColumnName, colIndex);

    const rows = this._getRowsForPreview(table.get('rows'), addedByUiColumnIndex, colIndex);
    return table.set('rows', rows).set('columns', columns);
  }

  addCalculatedField(table, item) {
    const { columnName } = item;
    const addedByUiColumnIndex = this._getIndexOfPreviewColumn(table.get('columns'));
    let columns = this._getColumnsWithoutPreviewColumns(table.get('columns'));

    const column = columns.get('0')
                          .set('name', columnName)
                          .set('status', 'HIGHLIGHTED')
                          .set('color', HIGHLIGHTED_TABLE)
                          .set('index', columns.size)
                          .set('type', '');
    columns = columns.push(column);
    const rows = this._getRowsForPreview(table.get('rows'), addedByUiColumnIndex, columns.size - 1);
    return table.set('rows', rows).set('columns', columns);
  }

  previewTransforms(table, item) {
    const hash = {
      convertCase: this.defaultTransform,
      trim: this.defaultTransform,
      extract: this.defaultTransform,
      ReplaceValue: this.defaultTransform,
      ReplacePattern: this.defaultTransform,
      Split: this.defaultTransform
    };
    const type = item.fieldTransformation && item.fieldTransformation.type;
    if (item.fieldTransformation && item.fieldTransformation.type.indexOf('Convert') !== -1) {
      return this.defaultTransform(table, item);
    }
    if (hash[type]) {
      return hash[type](table, item);
    }
    return table;
  }

  tryToMakeFastTransform(table, item) {
    const hash = {
      drop: this.dropColumn,
      rename: this.renameColumn,
      addCalculatedField: this.addCalculatedField,
      field: this.previewTransforms
    };
    if (hash[item.type]) {
      return hash[item.type](table, item);
    }
    return table;
  }

  isClever(item) {
    return !item.filter;
  }

  getListOfCleverTransformations() {
    return ['drop', 'rename', 'convert_case', 'trim_white_spaces',
      'calculated_field', 'transform', 'convert_data_type'];
  }

  getIndexNewColumn(newTable) {
    const columns = newTable.data ? newTable.data.columns : newTable.columns;
    const newCol = columns.filter(col => col.status);
    if (newCol && newCol.length === 1) {
      return newCol[0].index;
    } else if (newCol.length > 1) {
      return newCol.reduce((prevCol, col) => {
        return (col.index - newCol[0].index) <= DISTANCE_BETWEEN_NEW_COLUMNS
        ? Math.round(((prevCol.index || prevCol) + col.index) / 2)
        : newCol[0].index;
      });
    }
    return null;
  }

  setDesiredTypes(types) {
    return types.map(type => {
      return {...type, id: uuid.v4()};
    });
  }

}

const transforms = new Transforms();
export default transforms;
