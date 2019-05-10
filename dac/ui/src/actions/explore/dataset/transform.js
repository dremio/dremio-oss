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
import { datasetWithoutData } from 'schemas/v2/fullDataset';

import { postDatasetOperation } from './common';

export const runTableTransform = (dataset, transformData, viewId, tableData) =>
  (dispatch) => {
    const newVersion = exploreUtils.getNewDatasetVersion();
    const href = exploreUtils.getPreviewTransformationLink(dataset, newVersion);
    const nextTable = tableData && tableData.set('version', newVersion);

    return dispatch(postDatasetOperation({
      href, dataset, schema: datasetWithoutData, viewId, nextTable, body: transformData
    }));
  };

export const TRANSFORM_HISTORY_CHECK = 'TRANSFORM_HISTORY_CHECK';
export const transformHistoryCheck = (dataset, continueCallback, cancelCallback) => ({
  type: TRANSFORM_HISTORY_CHECK,
  meta: { dataset, continueCallback, cancelCallback }
});

export const PERFORM_TRANSFORM = 'PERFORM_TRANSFORM';
export const performTransform = (payload) => ({ type: PERFORM_TRANSFORM, payload });

export const CONFIRM_TRANSFORM = 'CONFIRM_TRANSFORM';
export const confirmTransform = () => ({ type: CONFIRM_TRANSFORM });

export const CANCEL_TRANSFORM = 'CANCEL_TRANSFORM';
export const cancelTransform = (viewId) => ({ type: CANCEL_TRANSFORM, viewId });

