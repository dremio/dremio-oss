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
import exploreUtils from 'utils/explore/exploreUtils';
import fullDatasetSchema from 'schemas/v2/fullDataset';
import { constructFullPath } from 'utils/pathUtils';

import { CALL_API } from 'redux-api-middleware';
import { API_URL_V2 } from 'constants/Api';
import { postDatasetOperation } from './dataset/common';

export const UPDATE_JOIN_DATASET_VERSION = 'UPDATE_JOIN_DATASET_VERSION';
export const CLEAR_JOIN_DATASET = 'CLEAR_JOIN_DATASET';

export const loadJoinDataset = (datasetPathList, viewId) => (dispatch, getStore) => {
  // do not encode the path as getHrefForUntitledDatasetConfig will do that for us
  const fullPath = constructFullPath(datasetPathList);
  const newVersion = exploreUtils.getNewDatasetVersion();
  const href = exploreUtils.getHrefForUntitledDatasetConfig(fullPath, newVersion);
  dispatch(postDatasetOperation({href, schema: fullDatasetSchema, viewId}));

  dispatch({
    type: UPDATE_JOIN_DATASET_VERSION,
    joinVersion: newVersion,
    joinDatasetPathList: datasetPathList
  });
};

export const clearJoinDataset = () => {
  return {
    type: CLEAR_JOIN_DATASET
  };
};

export const SET_JOIN_TAB = 'SET_JOIN_TAB';

export const setJoinTab = (tabId) => {
  return {
    type: SET_JOIN_TAB,
    tabId
  };
};

export const RESET_JOINS = 'RESET_JOIN_STATE';

export const resetJoins = () => {
  return {
    type: RESET_JOINS
  };
};

export const SET_JOIN_STEP = 'SET_JOIN_STEP';

export const setJoinStep = (step) => {
  return {
    type: SET_JOIN_STEP,
    step
  };
};

export const LOAD_RECOMMENDED_JOIN_START = 'LOAD_RECOMMENDED_JOIN_START';
export const LOAD_RECOMMENDED_JOIN_SUCCESS = 'LOAD_RECOMMENDED_JOIN_SUCCESS';
export const LOAD_RECOMMENDED_JOIN_FAILURE = 'LOAD_RECOMMENDED_JOIN_FAILURE';

function fetchRecommendedJoin({href}) {
  const meta = {viewId: 'RecommendedJoins'};
  return {
    [CALL_API]: {
      types: [
        { type: LOAD_RECOMMENDED_JOIN_START, meta },
        { type: LOAD_RECOMMENDED_JOIN_SUCCESS, meta },
        { type: LOAD_RECOMMENDED_JOIN_FAILURE, meta }
      ],
      method: 'GET',
      headers: {'Content-Type': 'application/json'},
      endpoint: `${API_URL_V2}${href}`
    }
  };
}

export function loadRecommendedJoin({href}) {
  return (dispatch) => {
    return dispatch(fetchRecommendedJoin({href}));
  };
}

export const SET_ACTIVE_RECOMMENDED_JOIN = 'SET_ACTIVE_RECOMMENDED_JOIN';

export const setActiveRecommendedJoin = (recommendation) => {
  return {type: SET_ACTIVE_RECOMMENDED_JOIN, recommendation};
};

export const RESET_ACTIVE_RECOMMENDED_JOIN = 'RESET_ACTIVE_RECOMMENDED_JOIN';
export const resetActiveRecommendedJoin = () => ({type: RESET_ACTIVE_RECOMMENDED_JOIN});

export const EDIT_RECOMMENDED_JOIN = 'EDIT_RECOMMENDED_JOIN';
export const editRecommendedJoin = (recommendation, version) =>
  ({type: EDIT_RECOMMENDED_JOIN, recommendation, version});
