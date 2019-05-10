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
import { select, put, call, takeEvery, race, fork, take, spawn } from 'redux-saga/effects';
import { goBack } from 'react-router-redux';
import { PERFORM_LOAD_DATASET } from 'actions/explore/dataset/get';
import { newUntitled } from 'actions/explore/dataset/new';
import { loadExistingDataset } from 'actions/explore/dataset/edit';
import { updateViewState } from 'actions/resources';
import { handleResumeRunDataset, DataLoadError, explorePageChanged } from 'sagas/runDataset';
import { REAPPLY_DATASET_SUCCESS, navigateAfterReapply } from 'actions/explore/dataset/reapply';
import { EXPLORE_TABLE_ID } from 'reducers/explore/view';
import { focusSqlEditor } from '@app/actions/explore/view';
import { getViewStateFromAction } from '@app/reducers/resources/view';
import { getFullDataset, getDatasetVersionFromLocation } from '@app/selectors/explore';
import { getLocation } from 'selectors/routing';
import { TRANSFORM_PEEK_START } from '@app/actions/explore/dataset/peek';
import { EXPLORE_PAGE_LISTENER_START, EXPLORE_PAGE_LISTENER_STOP, EXPLORE_PAGE_EXIT } from '@app/actions/explore/dataset/data';
import { log } from '@app/utils/logger';

import apiUtils from 'utils/apiUtils/apiUtils';
import { constructFullPath } from 'utils/pathUtils';

import {
  transformThenNavigate, TransformCanceledError, TransformCanceledByLocationChangeError,
  TransformFailedError
} from './transformWatcher';

export default function* watchLoadDataset() {
  yield takeEvery(PERFORM_LOAD_DATASET, handlePerformLoadDataset);
  yield takeEvery(REAPPLY_DATASET_SUCCESS, handleReapply);
  yield fork(explorePageDataChecker);
}

//todo merge this logic into performTransform saga
export function* handlePerformLoadDataset({ meta }) {
  const { dataset, viewId } = meta;

  try {
    const apiAction = yield call(loadDataset, dataset, viewId);
    const response = yield call(transformThenNavigate, apiAction, viewId, {
      replaceNav: true,
      preserveTip: true
    });

    const nextFullDataset = apiUtils.getEntityFromResponse('fullDataset', response);
    yield call(focusSqlEditorSaga); // DX-9819 focus sql editor when metadata is loaded
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
  } catch (e) {
    if (e instanceof TransformCanceledError) {
      yield put(goBack());
    } else if (!(e instanceof TransformCanceledByLocationChangeError) &&
      !(e instanceof DataLoadError) &&
      !(e instanceof TransformFailedError) ) {
      throw e;
    }
  }
}

export function* focusSqlEditorSaga() {
  yield put(focusSqlEditor());
}

function* handleReapply(response) {
  const { payload, error } = response;
  if (error) return;

  yield put(navigateAfterReapply(response, true));
  yield call(loadTableData, payload.get('result'));
}

//export for testing
export const CANCEL_TABLE_DATA_LOAD = 'CANCEL_TABLE_DATA_LOAD';
export function* cancelDataLoad() {
  yield put({ type: CANCEL_TABLE_DATA_LOAD });
}

//export for testing
/**
 * Triggers data load for a dataset. Data request will be sent only
 * if dataset with id =  {@see datasetVersion} does not have data or
 * {@see forceReload} parameter set to true
 * Also if metadata for {@see datasetVersion} is not loaded this saga will cancel existing data
 * load request and does nothing after that
 * @param {string} datasetVersion - a dataset version
 * @param {boolean} forceReload - enforce data load if data already exists
 * @yields {void}
 * @throws DataLoadError
 */
export function* loadTableData(datasetVersion, forceReload) {
  log('prerequisites check');
  let resetViewState = true;
  // we should cancel a previous data load request in any case
  yield call(cancelDataLoad); // cancel previous call, when a new load request is sent

  //#region check if metadata is loaded --------------------

  if (!datasetVersion) return;

  const dataset = yield select(getFullDataset, datasetVersion);
  if (!dataset) return; // do not load a data if metadata is not loaded

  const jobId = dataset.getIn(['jobId', 'id'], null);
  const paginationUrl = dataset.get('paginationUrl');
  if (!paginationUrl || !jobId) return;

  //#endregion ---------------------------------------------

  log('loading is about to start');
  try {
    yield put(updateViewState(EXPLORE_TABLE_ID, {
      isInProgress: true,
      isFailed: false,
      error: null
    }));

    // load first page for a table
    const raceResult = yield race({
      dataLoaded: call(handleResumeRunDataset, datasetVersion, jobId, forceReload, paginationUrl),
      isLoadCanceled: take([CANCEL_TABLE_DATA_LOAD, TRANSFORM_PEEK_START]), // cancel previous data load
      locationChange: call(resetTableViewStateOnPageLeave)
    });

    log('race result', raceResult);
  } catch (e) { // handleResumeRunDataset will throw an error in case data load errors
    if (!(e instanceof DataLoadError)) {
      throw e;
    }
    resetViewState = false; // to not hide an error

    const viewState = yield call(getViewStateFromAction, e.response);
    yield put(updateViewState(EXPLORE_TABLE_ID, viewState));
  } finally {
    if (resetViewState) {
      yield call(hideTableSpinner);
    }
  }
}
const defaultViewState = {
  isInProgress: false,
  isFailed: false,
  error: null
};

function* explorePageDataChecker() {
  // use infinite loop to listen start/stop actions
  while (true) { // eslint-disable-line no-constant-condition
    const { doInitialLoad } = yield take(EXPLORE_PAGE_LISTENER_START);
    log('explore page listener is started');

    yield race({
      stop: take(EXPLORE_PAGE_LISTENER_STOP, EXPLORE_PAGE_EXIT),
      infiniteProcess: call(pageChangeListener, doInitialLoad)
    });
    log('explore page listener is stopped');
  }
}


/**
 * An infinite listener for explore page change event
 */
function* pageChangeListener(doInitialLoad) {
  // we should start initial data load immediately, when listener is started
  if (doInitialLoad) {
    log('initial data load is started');
    yield spawn(loadDataForCurrentPage);
  }

  // use infinite loop for a listener
  while (true) { // eslint-disable-line no-constant-condition
    yield call(explorePageChanged);
    log('listener starts data load');
    // spawn non-blocking effect and start listen for next page change action immediately
    yield spawn(loadDataForCurrentPage);
  }
}

/**
 * Initiates data loading for current dataset. Current dataset version is extracted from url.
 */
function* loadDataForCurrentPage() {
  const location = yield select(getLocation);
  const datasetVersion = getDatasetVersionFromLocation(location);
  yield call(loadTableData, datasetVersion);
}

//export for tests
export function* hideTableSpinner() {
  yield put(updateViewState(EXPLORE_TABLE_ID, defaultViewState));
}

//export for testing
export function* resetTableViewStateOnPageLeave() {
  // wait for page leave event once
  yield call(explorePageChanged);
  log('table spinner reset is called');
  // reset view state, that may contains an error message for previous page
  yield call(hideTableSpinner);
}

export function* loadDataset(dataset, viewId) {
  const location = yield select(getLocation);
  const { mode, tipVersion } = location.query || {};
  let apiAction;
  if (mode === 'edit' || dataset.get('datasetVersion')) {
    apiAction = yield call(loadExistingDataset, dataset, viewId, tipVersion);
  } else {
    const pathnameParts = location.pathname.split('/');
    const parentFullPath = decodeURIComponent(constructFullPath([pathnameParts[2]]) + '.' + pathnameParts[3]);
    apiAction = yield call(newUntitled, dataset, parentFullPath, viewId);
  }

  return apiAction;
}
