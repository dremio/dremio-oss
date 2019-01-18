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
import { select, put, call, takeEvery, race, take, fork } from 'redux-saga/effects';
import { goBack } from 'react-router-redux';
import { PERFORM_LOAD_DATASET } from 'actions/explore/dataset/get';
import { newUntitled } from 'actions/explore/dataset/new';
import { navigateToNextDataset } from 'actions/explore/dataset/common';
import { loadExistingDataset } from 'actions/explore/dataset/edit';
import { updateViewState } from 'actions/resources';
import { handleResumeRunDataset } from 'sagas/runDataset';
import { REAPPLY_DATASET_SUCCESS, navigateAfterReapply } from 'actions/explore/dataset/reapply';
import { EXPLORE_TABLE_ID } from 'reducers/explore/view';

import { getExplorePageLocationChangePredicate } from '@app/sagas/utils';
import { getLocation } from 'selectors/routing';

import apiUtils from 'utils/apiUtils/apiUtils';
import { constructFullPath } from 'utils/pathUtils';

import {
  performWatchedTransform, TransformCanceledError, TransformCanceledByLocationChangeError
} from './transformWatcher';

export default function* watchLoadDataset() {
  yield takeEvery(PERFORM_LOAD_DATASET, handlePerformLoadDataset);
  yield takeEvery(REAPPLY_DATASET_SUCCESS, handleReapply);
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

      if (nextFullDataset) {
        if (nextFullDataset.get('error')) {
          yield put(updateViewState(viewId, {
            isFailed: true,
            error: { message: nextFullDataset.getIn(['error', 'errorMessage']) }
          }));
        } else {
          const version = nextFullDataset.get('version');
          yield call(loadTableData, version);
        }
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

function* handleReapply(response) {
  const { payload, error } = response;
  if (error) return;

  yield put(navigateAfterReapply(response, true));
  yield call(loadTableData, payload.get('result'));
}

//export for testing
export const CANCEL_TABLE_DATA_LOAD = 'CANCEL_TABLE_DATA_LOAD';

//export for testing
export function* loadTableData(datasetVersion, forceReload) {
  yield put({ type: CANCEL_TABLE_DATA_LOAD }); // cancel previous call, when a new load request is sent
  yield put(updateViewState(EXPLORE_TABLE_ID, {
    isInProgress: true,
    isFailed: false,
    error: null
  }));

  //load first page for a table
  const { loadRequestViewState } = yield race({
    loadRequestViewState: call(handleResumeRunDataset, datasetVersion, forceReload),
    isLoadCanceled: take(CANCEL_TABLE_DATA_LOAD), // cancel previous data load
    locationChange: call(resetTableViewStateOnPageLeave)
  });

  if (loadRequestViewState) {
    // if operationViewState is returned, which means 'handleResumeRunDataset' is executed first,
    // we should use a returned viewstate (this state could contain an error). Otherwise just reject load mask.
    yield put(updateViewState(EXPLORE_TABLE_ID, loadRequestViewState));

    yield fork(resetTableViewStateOnPageLeave);
  }
}

//export for testing
export function* resetTableViewStateOnPageLeave() {
  const locationChangedPredicate = yield call(getExplorePageLocationChangePredicate);
  // wait for page leave event once
  yield take(locationChangedPredicate);
  // reset view state, that may contains an error message for previous page
  yield put(updateViewState(EXPLORE_TABLE_ID, {
    isInProgress: false,
    isFailed: false,
    error: null
  }));
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
