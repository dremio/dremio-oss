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
import { CALL_API } from 'redux-api-middleware';
import invariant from 'invariant';
import { debounce } from 'lodash/function';

import { API_URL_V2 } from 'constants/Api';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import { datasetWithoutData } from 'schemas/v2/fullDataset';
import exploreUtils from 'utils/explore/exploreUtils';

export const RUN_DATASET_START = 'RUN_DATASET_START';
export const RUN_DATASET_SUCCESS = 'RUN_DATASET_SUCCESS';
export const RUN_DATASET_FAILURE = 'RUN_DATASET_FAILURE';

function fetchRunDataset(dataset, viewId) {
  const tipVersion = dataset.get('tipVersion');
  const href = `${dataset.getIn(['apiLinks', 'self'])}/run` + (tipVersion ? `?tipVersion=${tipVersion}` : '');

  const meta = { dataset, viewId };
  return {
    [CALL_API]: {
      types: [
        { type: RUN_DATASET_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(RUN_DATASET_SUCCESS, datasetWithoutData, meta),
        { type: RUN_DATASET_FAILURE, meta }
      ],
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      },
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export const runDataset = (dataset, tipVersion, viewId) =>
  (dispatch) => dispatch(fetchRunDataset(dataset, tipVersion, viewId));

export const TRANSFORM_AND_RUN_DATASET_START = 'TRANSFORM_AND_RUN_DATASET_START';
export const TRANSFORM_AND_RUN_DATASET_SUCCESS = 'TRANSFORM_AND_RUN_DATASET_SUCCESS';
export const TRANSFORM_AND_RUN_DATASET_FAILURE = 'TRANSFORM_AND_RUN_DATASET_FAILURE';

export const transformAndRunActionTypes = [
  TRANSFORM_AND_RUN_DATASET_START, TRANSFORM_AND_RUN_DATASET_SUCCESS, TRANSFORM_AND_RUN_DATASET_FAILURE
];

function fetchTransformAndRun(dataset, transformData, viewId) {
  invariant(dataset.get('datasetVersion'), 'Can\'t run new dataset. Create dataset with newUntitled first');
  const newVersion = exploreUtils.getNewDatasetVersion();
  const href = `${dataset.getIn(['apiLinks', 'self'])}/transformAndRun?newVersion=${newVersion}`;

  const meta = { viewId, entity: dataset};
  return {
    [CALL_API]: {
      types: [
        { type: TRANSFORM_AND_RUN_DATASET_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(TRANSFORM_AND_RUN_DATASET_SUCCESS, datasetWithoutData, meta),
        { type: TRANSFORM_AND_RUN_DATASET_FAILURE, meta }
      ],
      method: 'POST',
      body:  JSON.stringify(transformData),
      headers: {
        'Content-Type': 'application/json'
      },
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export const transformAndRunDataset = (dataset, transformData, viewId) =>
  (dispatch) => dispatch(fetchTransformAndRun(dataset, transformData, viewId));


export const PERFORM_TRANSFORM_AND_RUN = 'PERFORM_TRANSFORM_AND_RUN';
export const performTransformAndRun = (payload) => ({ type: PERFORM_TRANSFORM_AND_RUN, payload });


export const RUN_DATASET_SQL = 'RUN_DATASET_SQL';

const getRunAction = (dispatch, isPreview) => {
  const action = { type: RUN_DATASET_SQL };
  if (isPreview) {
    action.isPreview = true;
  }
  dispatch(action);
};
const runDebounced = debounce(getRunAction, 500, {leading: true, trailing: false});

export const runDatasetSql = () => (dispatch) => {
  runDebounced(dispatch);
};

export const previewDatasetSql = () => (dispatch) => {
  const isPreview = true;
  runDebounced(dispatch, isPreview);
};
