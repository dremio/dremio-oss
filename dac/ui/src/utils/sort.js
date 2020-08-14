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
export const humanSorter = (a, b) => {
  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b, undefined, {numeric: true}); // properly handles "1-foo", "12-foo", "2-foo"
  }
  // strings precede numbers in ascending order
  if (typeof a === 'string' && typeof b === 'number') return -1;
  if (typeof a === 'number' && typeof b === 'string') return 1;

  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
};

/**
 * get virtualized table element value used for sorting
 * @param item
 * @param sortBy
 * @param sortDirection
 * @return {*}
 */
export const getSortValue = (item, sortBy, sortDirection) => {
  if (!item || !item.data || !item.data[sortBy]) return undefined;

  const itemElement = item.data[sortBy];
  const value = (
    typeof itemElement.value === 'undefined'
    && typeof itemElement.node === 'function'
  ) ? itemElement.node()
    : itemElement.value;
  if (typeof value === 'function') {
    return value.call(item.data[sortBy], sortDirection, sortBy);
  }
  return value;
};
