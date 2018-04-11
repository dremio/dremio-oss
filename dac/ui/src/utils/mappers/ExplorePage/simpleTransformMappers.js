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
class SimpleTransformMappers {
  mapSortItem(item) {
    return {
      type: 'sort',
      sortedColumnName: item.columnName,
      order: item.type
    };
  }

  mapUnnest(item) {
    return {
      type: 'field',
      sourceColumnName: item.columnName,
      newColumnName: item.columnName,
      dropSourceColumn: true,
      fieldTransformation: {
        type: 'UnnestList'
      }
    };
  }

  mapGroupBy(item) {
    return {
      type: 'groupBy',
      columnsDimensionsList: item.columnsDimensions.map(col => {
        return {
          column: col.column
        };
      }),
      columnsMeasuresList: item.columnsMeasures.map(col => {
        return {
          column: col.column,
          type: col.measure || 'Sum'
        };
      })
    };
  }

  mapRename(item) {
    return {
      type: 'rename',
      oldColumnName: item.columnName,
      newColumnName: item.newColumnName
    };
  }

  mapDrop(item) {
    return {
      type: 'drop',
      droppedColumnName: item.columnName
    };
  }

  mapMultySortItem(item) {
    return {
      type: 'sorts',
      columns: item.sortColumns
    };
  }

  mapCovertCase(item) {
    const caseHash = {
      UPPERCASE: 'UPPER_CASE',
      lowercase: 'LOWER_CASE',
      TITLECASE: 'TITLE_CASE'
    };
    return {
      type: 'field',
      sourceColumnName: item.columnName,
      newColumnName: item.newFieldName,
      dropSourceColumn: item.dropSourceField,
      fieldTransformation: {
        type: 'convertCase',
        convertCase: caseHash[item.action]
      }
    };
  }

  mapTrim(item) {
    return {
      type: 'field',
      sourceColumnName: item.columnName,
      newColumnName: item.newFieldName,
      dropSourceColumn: item.dropSourceField,
      fieldTransformation: {
        type: 'trim',
        trimType: item.action
      }
    };
  }

  mapCalculated(item) {
    return {
      type: 'addCalculatedField',
      dropSourceColumn: item.dropSourceField,
      sourceColumnName: item.sourceColumnName,
      newColumnName: item.newFieldName,
      expression: item.expression
    };
  }

  mapCleanData(form) {
    const { typeMixed, newFieldName, dropSourceField, columnName, columnType, newColumnNamePrefix, ...data } = form;

    if (typeMixed === 'convertToSingleType' && columnType === 'MIXED') {
      return {
        ...data,
        type: typeMixed,
        sourceColumnName: columnName,
        newColumnName: newFieldName,
        dropSourceColumn: dropSourceField
      };
    }
    return {
      ...data,
      type: typeMixed,
      sourceColumnName: columnName,
      newColumnNamePrefix,
      dropSourceColumn: dropSourceField
    };
  }

  mapJoin(form) {
    return {
      type: 'join',
      joinType: form.joinType || 'Inner',
      rightTableFullPathList: form.activeDataset,
      joinConditionsList: ( form.columns || [] ).map(item => {
        return {
          leftColumn: item.joinedColumn,
          rightColumn: item.joinedTableKeyColumnName
        };
      })
    };
  }
}

const simpleTransformMappers = new SimpleTransformMappers();
export default simpleTransformMappers;
