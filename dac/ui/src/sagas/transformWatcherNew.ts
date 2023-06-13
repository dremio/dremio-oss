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

import { call, put } from "redux-saga/effects";
import {
  performWatchedTransform,
  TransformFailedError,
} from "@app/sagas/transformWatcher";
import { failedExploreJobProgress } from "@app/actions/explore/dataset/data";
import {
  completeDatasetMetadataLoad,
  startDatasetMetadataLoad,
} from "@app/actions/explore/view";

export function* submitTransformationJob(
  action: any,
  viewId: string
): Record<string, any> {
  const response = yield call(performWatchedTransform, action, viewId);

  if (response && !response.error) {
    return response;
  }

  yield put(failedExploreJobProgress());
  throw new TransformFailedError(response);
}

export function* fetchDatasetMetadata(
  action: any,
  viewId: string
): Record<string, any> {
  try {
    yield put(startDatasetMetadataLoad());
    const response = yield call(performWatchedTransform, action, viewId);

    if (response && !response.error) {
      return response;
    }

    yield put(failedExploreJobProgress());
    throw new TransformFailedError(response);
  } finally {
    yield put(completeDatasetMetadataLoad());
  }
}
