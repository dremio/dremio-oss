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
const measuresOptions  = [
  {
    label: 'Sum',
    option: 'Sum',
    columnType: ['INTEGER', 'FLOAT', 'DECIMAL', 'DECIMAL']
  },
  {
    label: 'Average',
    option: 'Average',
    columnType: ['INTEGER', 'FLOAT', 'DECIMAL']
  },
  {
    label: 'Count',
    option: 'Count'
  },
  {
    label: 'Count (*)',
    option: 'Count_Star'
  },
  {
    label: 'Count (distinct)',
    option: 'Count_Distinct'
  },
  {
    label: 'Minimum',
    option: 'Minimum'
  },
  {
    label: 'Maximum',
    option: 'Maximum'
  },
  {
    label: 'Standard Deviation',
    option: 'Standard_Deviation',
    columnType: ['INTEGER', 'FLOAT', 'DECIMAL']
  },
  {
    label: 'Standard Deviation (population)',
    option: 'Standard_Deviation_Population',
    columnType: ['INTEGER', 'FLOAT', 'DECIMAL']
  },
  {
    label: 'Variance',
    option: 'Variance',
    columnType: ['INTEGER', 'FLOAT', 'DECIMAL']
  },
  {
    label: 'Variance (Population)',
    option: 'Variance_Population',
    columnType: ['INTEGER', 'FLOAT', 'DECIMAL']
  }
];

export function getColumnByName(allColumns, columnName) {
  if (!columnName) {
    return;
  }
  return allColumns.find(c => c.get('name') === columnName);
}

export function isAlreadySelected(columnsField, columnName) {
  if (!columnName) {
    return false;
  }

  return Boolean(columnsField.find(c => c.column.value === columnName));
}

export function getMeasuresForColumnType(columnType) {
  return measuresOptions.filter(option =>
    !columnType || !option.columnType || option.columnType.indexOf(columnType) !== -1
  );
}

export function isMeasureValidForColumnType(measure, columnType) {
  const curMeasureOption = measuresOptions.find(measuresOption => measuresOption.option === measure);
  return !curMeasureOption || !curMeasureOption.columnType || curMeasureOption.columnType.indexOf(columnType) !== -1;
}


