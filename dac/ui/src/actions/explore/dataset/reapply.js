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
import { push, replace } from 'react-router-redux';

import { API_URL_V2 } from 'constants/Api';
import schemaUtils from 'utils/apiUtils/schemaUtils';
import { datasetWithoutData } from 'schemas/v2/fullDataset';
import { performNextAction } from 'actions/explore/nextAction';

export const REAPPLY_DATASET_START   = 'REAPPLY_DATASET_START';
export const REAPPLY_DATASET_SUCCESS = 'REAPPLY_DATASET_SUCCESS';
export const REAPPLY_DATASET_FAILURE = 'REAPPLY_DATASET_FAILURE';

export function editOriginalSql(previousDatasetId, selfApiUrl) {
  return (dispatch) => {
    return dispatch(fetchOriginalSql(previousDatasetId, selfApiUrl));
  };
}

function fetchOriginalSql(previousDatasetId, selfApiUrl, viewId) {
  const meta = { viewId, previousId: previousDatasetId };
  return {
    [CALL_API]: {
      types: [
        { type: REAPPLY_DATASET_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(REAPPLY_DATASET_SUCCESS, datasetWithoutData, meta),
        { type: REAPPLY_DATASET_FAILURE, meta: { ...meta, notification: true }}
      ],
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      endpoint: `${API_URL_V2}${selfApiUrl}/editOriginalSql` // look here
    }
  };
}

export function navigateAfterReapply(response, replaceNav, nextAction) {
  return (dispatch) => {
    const nextDataset = response.payload.getIn(['entities', 'datasetUI', response.payload.get('result')]);
    const link = nextDataset.getIn(['links', 'edit']);

    const action = replaceNav ? replace : push;
    const result = dispatch(action(link));
    if (nextAction) {
      return result.then((nextResponse) => {
        if (!nextResponse.error) {
          return dispatch(performNextAction(nextDataset, nextAction));
        }
        return nextResponse;
      });
    }
    return result;
  };
}
