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
import { Schema } from 'normalizr';

const datasetUI = new Schema('datasetUI', {
  // id and datasetVersion used to be the same,
  // so there is a lot of code that has a DS version and uses it to look up
  // the datasetUI object - so can't us #id, even though it has one
  idAttribute: 'datasetVersion'
});

export default datasetUI;
