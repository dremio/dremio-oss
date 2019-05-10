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

import localStorageUtils from 'utils/storageUtils/localStorageUtils';

const DEFAULT_HEIGHT_SQL = 206; // 11 lines of text
const NEW_QUERY_DEFAULT_SQL_HEIGHT = 466; // 25 lines
const DEFAULT_HEIGHT_SORT = 293;
const LARGE_HEIGHT_SUBPAGE = 252;
const DEFAULT_HEIGHT_CALCULATED = 228;
const GROUP_BY_HEIGHT = 320;
const CONVERT_CASE = 252;
const HEIGHT_CLEAN_MIXED = 436;
const JOIN = 330;

export const hashHeightTopSplitter = {
  getDefaultSqlHeight: () => localStorageUtils.getDefaultSqlHeight() || DEFAULT_HEIGHT_SQL,
  getNewQueryDefaultSqlHeight: () => localStorageUtils.getDefaultSqlHeight() || NEW_QUERY_DEFAULT_SQL_HEIGHT,
  transformCards: LARGE_HEIGHT_SUBPAGE,
  transform: LARGE_HEIGHT_SUBPAGE,
  MULTIPLE: DEFAULT_HEIGHT_SORT,
  CALCULATED_FIELD: DEFAULT_HEIGHT_CALCULATED,
  GROUP_BY: GROUP_BY_HEIGHT,
  CONVERT_CASE,
  SINGLE_DATA_TYPE: HEIGHT_CLEAN_MIXED,
  JOIN,
  SPLIT_BY_DATA_TYPE: HEIGHT_CLEAN_MIXED
};
