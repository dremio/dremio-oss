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

import { API_URL_V2 } from 'constants/Api';
import transformModelMapper from 'utils/mappers/ExplorePage/Transform/transformModelMapper';


export const RESET_RECOMMENDED_TRANSFORMS = 'RESET_RECOMMENDED_TRANSFORMS';
export function resetRecommendedTransforms() {
  return (dispatch) => {
    return dispatch({type: RESET_RECOMMENDED_TRANSFORMS});
  };
}


export const RUN_SELECTION_TRANSFORM_START = 'RUN_SELECTION_TRANSFORM_START';
export const RUN_SELECTION_TRANSFORM_SUCCESS = 'RUN_SELECTION_TRANSFORM_SUCCESS';
export const RUN_SELECTION_TRANSFORM_FAILURE = 'RUN_SELECTION_TRANSFORM_FAILURE';

export const LOAD_TRANSFORM_CARDS_VIEW_ID = 'LOAD_TRANSFORM_CARDS_VIEW_ID';

function fetchTransformCards(data, transform, dataset, actionType) {
  const meta = {
    transformType: transform.get('transformType'),
    method: transform.get('method') || 'default',
    actionType,
    viewId: LOAD_TRANSFORM_CARDS_VIEW_ID
  };
  return {
    [CALL_API]: {
      types: [
        { type: RUN_SELECTION_TRANSFORM_START, meta },
        { type: RUN_SELECTION_TRANSFORM_SUCCESS, meta },
        { type: RUN_SELECTION_TRANSFORM_FAILURE, meta }
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(transformModelMapper.transformExportPostMapper(data)),
      endpoint: `${API_URL_V2}${dataset.getIn(['apiLinks', 'self'])}/${actionType}`
    }
  };
}

export function loadTransformCards(data, transform, dataset, actionType) {
  return (dispatch) => {
    return dispatch(fetchTransformCards(data, transform, dataset, actionType));
  };
}

export const TRANSFORM_CARD_PREVIEW_START = 'TRANSFORM_CARD_PREVIEW_START';
export const TRANSFORM_CARD_PREVIEW_SUCCESS = 'TRANSFORM_CARD_PREVIEW_SUCCESS';
export const TRANSFORM_CARD_PREVIEW_FAILURE = 'TRANSFORM_CARD_PREVIEW_FAILURE';

function fetchTransformCardPreview(data, transform, dataset, actionType, index) {
  const meta = {
    transformType: transform.get('transformType'),
    method: transform.get('method') || 'default',
    actionType,
    index
  };
  return {
    [CALL_API]: {
      types: [
        { type: TRANSFORM_CARD_PREVIEW_START, meta },
        { type: TRANSFORM_CARD_PREVIEW_SUCCESS, meta },
        { type: TRANSFORM_CARD_PREVIEW_FAILURE, meta }
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(transformModelMapper.transformDynamicPreviewMapper(data, actionType)),
      endpoint: `${API_URL_V2}${dataset.getIn(['apiLinks', 'self'])}/${actionType}_preview`
    }
  };
}

export const UPDATE_TRANSFORM_CARD = 'UPDATE_TRANSFORM_CARD';
export function updateTransformCard(payload, meta) {
  return {type: UPDATE_TRANSFORM_CARD, payload, meta};
}

export function loadTransformCardPreview(data, transform, dataset, actionType, index) {
  return (dispatch) => {
    return dispatch(fetchTransformCardPreview(data, transform, dataset, actionType, index));
  };
}

export const RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_START = 'RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_START';
export const RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_SUCCESS = 'RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_SUCCESS';
export const RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_FAILURE = 'RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_FAILURE';

function fetchTransformValuesPreview(data, transform, dataset, actionType) {
  const meta = {
    transformType: transform.get('transformType'),
    method: transform.get('method') || 'default'
  };
  return {
    [CALL_API]: {
      types: [
        { type: RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_START, meta },
        { type: RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_SUCCESS, meta },
        { type: RUN_SELECTION_TRANSFORM_PREVIEW_VALUES_FAILURE, meta }
      ],
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(transformModelMapper.mapTransformValuesPreview(data, actionType)),
      endpoint: `${API_URL_V2}${dataset.getIn(['apiLinks', 'self'])}/${actionType}_values_preview`
    }
  };
}

export function loadTransformValuesPreview(data, transform, dataset, actionType) {
  return (dispatch) => {
    return dispatch(fetchTransformValuesPreview(data, transform, dataset, actionType));
  };
}



