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
import tableData from './tableData';
import datasetUI from './datasetUI';
import history from './history';

const fullDataset = new Schema('fullDataset', {
  idAttribute: 'version'
});

fullDataset.define({
  data: tableData,
  dataset: datasetUI,
  history
});

export default fullDataset;

export {
  // we are in process of refactoring of dataset data loading. We are going to get rid of 'data'
  // property in fullDataset schema as we are going to load data asynchronously. Also we are going to
  // bubble 'columns' property from data one level above.
  // Right now we in intermidiate state. Some endpoints load the
  // data and some not. So endpoints that does NOT load the data in intital dataset response are
  // using datasetWithoutData. The old code uses fullDataset schema. So looking on the schema you
  // could understand was particulsr enpoint refactored to load data asynchronously or not.
  fullDataset as datasetWithoutData
};
