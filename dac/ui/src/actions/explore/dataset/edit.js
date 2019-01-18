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

import { loadExploreEntities } from './get';

// this action creator is used only in performLoadDataset saga. We do not expect data in initial response
// the data should be load as separate call using 'loadNextRows' action. See code of performLoadDataset saga
// for details
export const loadExistingDataset = (dataset, viewId, tipVersion) =>
  (dispatch) => {
    const jobId = dataset.get('jobId');
    const href = jobId
        ? exploreUtils.getReviewLink(dataset, tipVersion)
        : exploreUtils.getPreviewLink(dataset, tipVersion);
    return dispatch(
      loadExploreEntities({
        href,
        viewId,
        schema: datasetWithoutData
      })
    );
  };
