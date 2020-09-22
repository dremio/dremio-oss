/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { RSAA } from 'redux-api-middleware';
import { push, replace } from 'react-router-redux';

import schemaUtils from 'utils/apiUtils/schemaUtils';
import { datasetWithoutData } from 'schemas/v2/fullDataset';
import { performNextAction } from 'actions/explore/nextAction';
import { APIV2Call } from '@app/core/APICall';

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

  const apiCall = new APIV2Call().paths(`${selfApiUrl}/editOriginalSql`);

  return {
    [RSAA]: {
      types: [
        { type: REAPPLY_DATASET_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(REAPPLY_DATASET_SUCCESS, datasetWithoutData, meta),
        { type: REAPPLY_DATASET_FAILURE, meta: { ...meta, notification: true }}
      ],
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      endpoint: apiCall
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
