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
import param from 'jquery-param';
import Immutable from 'immutable';

export const parseQueryState = (query) => {
  return Immutable.fromJS({
    filters: query.filters ? JSON.parse(query.filters) : {},
    sort: query.sort || 'st',
    order: query.order || 'DESCENDING'
  });
};

export const renderQueryState = (queryState) => {
  const {filters, sort, order} = queryState.toJS();
  return {
    ...( sort && order ? {sort, order} : {}),
    filters: JSON.stringify(filters)
  };
};

const escapeFilterValue = (value) => value.replace(/\\/, '\\\\').replace(/"/g, '\\"');

export const renderQueryStateForServer = (queryState) => {
  const filters = queryState.get('filters');
  const filterStrings = filters.entrySeq().map(([key, values]) => {
    if (!values) {
      return null;
    }
    if (key === 'st' && values instanceof Immutable.List) {           //start time
      return `(st=gt=${values.get(0)};st=lt=${values.get(1)})`;
    } else if (key === 'contains' && values) {
      return `*=contains="${escapeFilterValue(values.get(0))}"`;
    }
    if (values.size) {
      return '(' + values.map((value) => `${key}=="${escapeFilterValue(value)}"`).join(',') + ')';
    }
  }).filter(x => x);

  const sort = queryState.get('sort');
  const order = queryState.get('order');
  return param({
    ...(sort && order ? {sort, order} : {}),
    filter: filterStrings.join(';')
  });
};
