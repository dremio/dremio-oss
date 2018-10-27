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
import PropTypes from 'prop-types';

//assume that we have a base url 'http://localhost:3005/home/%40dremio/all_types_dremio'. Values below shows what we should we add to a path
export const PageTypes = {
  default: 'default', //TODO switch to empty string, when all components will use PropTypes enum | // we should not add any element into a path.
  graph: 'graph',
  details: 'details', // deTails, do not miss with deFaults. When both these values stands together, at first glance they look like duplicates. Graph was put between them intentionaly.
  wiki: 'wiki'
};

const values = Object.values(PageTypes);

export const pageTypeValuesSet = new Set(values);

export const pageTypesProp = PropTypes.oneOf(values);
