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
import {CALL_API} from 'redux-api-middleware';
import {API_URL_V2, API_URL_V3} from 'constants/Api';
import summaryDatasetSchema from 'schemas/v2/summaryDataset';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import {Schema} from 'normalizr';

export const LOAD_SUMMARY_DATASET_START = 'LOAD_SUMMARY_DATASET_START';
export const LOAD_SUMMARY_DATASET_SUCCESS = 'LOAD_SUMMARY_DATASET_SUCCESS';
export const LOAD_SUMMARY_DATASET_FAILURE = 'LOAD_SUMMARY_DATASET_FAILURE';

// todo: can we nix this DS shape variation? (handle its needs with one of the other "DS" shapes)
function fetchSummaryDataset(fullPath, viewId) {
  const meta = {
    viewId,
    fullPath,
    errorMessage: la('Cannot provide more information about this dataset.')
  };
  return {
    [CALL_API]: {
      types: [
        { type: LOAD_SUMMARY_DATASET_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_SUMMARY_DATASET_SUCCESS, summaryDatasetSchema, meta),
        { type: LOAD_SUMMARY_DATASET_FAILURE, meta }
      ],
      method: 'GET',
      endpoint: `${API_URL_V2}/datasets/summary/${encodeURIComponent(fullPath)}`
    }
  };
}

export const LOAD_DATASET_START = 'LOAD_DATASET_START';
export const LOAD_DATASET_SUCCESS = 'LOAD_DATASET_SUCCESS';
export const LOAD_DATASET_FAILURE = 'LOAD_DATASET_FAILURE';

const datasetSchema = new Schema('dataset', {
  // id and datasetVersion used to be the same,
  // so there is a lot of code that has a DS version and uses it to look up
  // the datasetUI object - so can't us #id, even though it has one
  idAttribute: 'id'
});

function fetchDataset(id, viewId) {
  const meta = {
    viewId,
    id,
    errorMessage: la('Cannot provide more information about this dataset.')
  };
  return {
    [CALL_API]: {
      types: [
        { type: LOAD_DATASET_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(LOAD_DATASET_SUCCESS, datasetSchema, meta),
        { type: LOAD_DATASET_FAILURE, meta }
      ],
      method: 'GET',
      endpoint: `${API_URL_V3}/catalog/${encodeURIComponent(id)}`
    }
  };
}

export const loadDataset = (id, viewId) => (dispatch) => dispatch(fetchDataset(id, viewId));

export const loadSummaryDataset = (fullPath, viewId) => (dispatch) => dispatch(fetchSummaryDataset(fullPath, viewId));
