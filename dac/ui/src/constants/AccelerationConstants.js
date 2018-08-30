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
import { BINARY, DATE, DATETIME, TEXT, TIME, VARCHAR } from './DataTypes';


export const allMeasureTypes = {
  APPROX_COUNT_DISTINCT: 'APPROX_COUNT_DISTINCT',
  COUNT: 'COUNT',
  MAX: 'MAX',
  MIN: 'MIN',
  SUM: 'SUM'
};
export const measureTypeLabels = {
  MIN: 'MIN',
  MAX: 'MAX',
  SUM: 'SUM',
  COUNT: 'COUNT',
  APPROX_COUNT_DISTINCT: 'Approximate distinct count'
};

export const cellTypesWithNoSum = [
  DATE,
  TIME,
  DATETIME,
  TEXT,
  BINARY,
  VARCHAR
];

export const fieldTypes = {
  blank: '',
  dimension: 'dimensionFields',
  sort: 'sortFields',
  measure: 'measureFields',
  partition: 'partitionFields',
  distribution: 'distributionFields',
  display: 'displayFields'
};

export const cellType = {
  blank: '',
  dimension: 'dimension',
  sort: 'sort',
  measure: 'measure',
  partition: 'partition',
  distribution: 'distribution'
};

export const granularityValue = {
  date: 'DATE',
  normal: 'NORMAL'
};
