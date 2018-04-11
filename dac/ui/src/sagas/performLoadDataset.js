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
import { select, put, call, takeEvery } from 'redux-saga/effects';
import { goBack } from 'react-router-redux';
import { PERFORM_LOAD_DATASET } from 'actions/explore/dataset/get';
import { newUntitled } from 'actions/explore/dataset/new';
import { navigateToNextDataset } from 'actions/explore/dataset/common';
import { loadExistingDataset } from 'actions/explore/dataset/edit';
import { updateViewState } from 'actions/resources';

import { getLocation } from 'selectors/routing';

import apiUtils from 'utils/apiUtils/apiUtils';
import { constructFullPath } from 'utils/pathUtils';

import {
  performWatchedTransform, TransformCanceledError, TransformCanceledByLocationChangeError
} from './transformWatcher';

export default function* watchLoadDataset() {
  yield takeEvery(PERFORM_LOAD_DATASET, handlePerformLoadDataset);
}

export function* handlePerformLoadDataset({ meta }) {
  const { dataset, viewId } = meta;

  try {
    const response = yield call(loadDataset, dataset, viewId);
    if (response && !response.error) {
      // Only navigate if we didn't have datasetVersion before (newUntitled, not review/preview)
      // Not navigating preserves jobId if it's there.
      if (!dataset.get('datasetVersion')) {
        yield put(navigateToNextDataset(response, {replaceNav: true, preserveTip: true}));
      }
      const nextFullDataset = apiUtils.getEntityFromResponse('fullDataset', response);

      if (nextFullDataset && nextFullDataset.get('error')) {
        yield put(updateViewState(viewId, {
          isFailed: true,
          error: { message: nextFullDataset.getIn(['error', 'errorMessage']) }
        }));
      }
    }
  } catch (e) {
    if (e instanceof TransformCanceledError) {
      yield put(goBack());
    } else if (!(e instanceof TransformCanceledByLocationChangeError)) {
      throw e;
    }
  }
}

export function* loadDataset(dataset, viewId) {
  const location = yield select(getLocation);
  const { mode, tipVersion } = location.query || {};
  if (mode === 'edit' || dataset.get('datasetVersion')) {
    const promise = yield put(loadExistingDataset(dataset, viewId, tipVersion));
    return yield promise;
  }
  const pathnameParts = location.pathname.split('/');
  const parentFullPath = decodeURIComponent(constructFullPath([pathnameParts[2]]) + '.' + pathnameParts[3]);
  return yield call(performWatchedTransform, newUntitled(dataset, parentFullPath, viewId), viewId);
}
