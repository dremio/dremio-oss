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
import { replace } from 'react-router-redux';
import uuid from 'uuid';

import { API_URL_V2 } from 'constants/Api';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import exploreUtils from 'utils/explore/exploreUtils';

import previewTableSchema from 'schemas/previewTable';
import apiUtils from '@app/utils/apiUtils/apiUtils';

export const TRANSFORM_PEEK_START   = 'TRANSFORM_PEEK_START';
export const TRANSFORM_PEEK_SUCCESS = 'TRANSFORM_PEEK_SUCCESS';
export const TRANSFORM_PEEK_FAILURE = 'TRANSFORM_PEEK_FAILURE';

export const transformPeek = (dataset, values, detailType, viewId, submitType) =>
  (dispatch) => dispatch(transformPeekFetch(dataset, values, detailType, viewId, submitType));

function transformPeekFetch(dataset, values, detailType, viewId, submitType) {
  const href = exploreUtils.getTransformPeekHref(dataset);
  const body = exploreUtils.getMappedDataForTransform(values, detailType);
  const peekId = uuid.v4();
  const uiPropsForEntity = [{ key: 'id', value: peekId }];
  const meta = { viewId, peekId, submitType };
  return {
    [CALL_API]: {
      types: [
        { type: TRANSFORM_PEEK_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(
          TRANSFORM_PEEK_SUCCESS, previewTableSchema, meta, uiPropsForEntity),
        { type: TRANSFORM_PEEK_FAILURE, meta }
      ],
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...apiUtils.getJobDataNumbersAsStringsHeader()
      },
      body: JSON.stringify(body),
      endpoint: `${API_URL_V2}${href}`
    }
  };
}
export const navigateToTransformPeek = (peekId) =>
  (dispatch, getState) => {
    const location = getState().routing.locationBeforeTransitions;
    return dispatch(
      replace({...location, state: {...location.state, previewVersion: peekId}})
    );
  };
