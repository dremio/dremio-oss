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
import { take, race, put, call, select, takeEvery } from 'redux-saga/effects';
import invariant from 'invariant';

import { loadNextRows, EXPLORE_PAGE_EXIT, updateExploreJobProgress, updateJobRecordCount } from 'actions/explore/dataset/data';
import { updateHistoryWithJobState } from 'actions/explore/history';

import socket, { WS_MESSAGE_JOB_PROGRESS, WS_MESSAGE_JOB_RECORDS, WS_CONNECTION_OPEN } from 'utils/socket';
import { getExplorePageLocationChangePredicate } from '@app/sagas/utils';
import { getTableDataRaw, getCurrentRouteParams } from '@app/selectors/explore';
import { log } from '@app/utils/logger';
import { LOGOUT_USER_SUCCESS } from '@app/actions/account';

const getJobDoneActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_PROGRESS && action.payload.id.id === jobId && action.payload.update.isComplete;

const getJobProgressActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_PROGRESS && action.payload.id.id === jobId && !action.payload.update.isComplete;

const getJobUpdateActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_PROGRESS && action.payload.id.id === jobId;

const getJobRecordsActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_RECORDS && action.payload.id.id === jobId;

/**
 * Load data for a dataset, if data is missing in redux store. Or forces data load if {@see forceReload}
 * set to true
 * @param {string!} datasetVersion - a dataset version
 * @param {string!} jobId - a job id for which data would be requested
 * @param {boolean!} forceReload
 * @param {string!} paginationUrl - put is a last parameter is there is plan to get rid of server
 * generated links
 * @yields {void} An exception may be thrown.
 * @throws DataLoadError
 */
export function* handleResumeRunDataset(datasetVersion, jobId, forceReload, paginationUrl) {
  invariant(datasetVersion, 'dataset version must be provided');
  invariant(jobId, 'jobId must be provided');
  invariant(paginationUrl, 'paginationUrl must be provided');

  // we always load data with column information inside, but rows may be missed in case if
  // we load data asynchronously
  const tableData = yield select(getTableDataRaw, datasetVersion);
  const rows = tableData ? tableData.get('rows') : null;
  log(`rows are present = ${!!rows}`);

  // if forceReload = true and data exists, we should not clear data here. As it would be replaced
  // by response in '/reducers/resources/entityReducers/table.js' reducer.
  if (forceReload || !rows) {
    yield race({
      jobDone: call(waitForRunToComplete, datasetVersion, paginationUrl, jobId),
      locationChange: call(explorePageChanged)
    });
  }
}

export class DataLoadError {
  constructor(response) {
    this.name = 'DataLoadError';
    this.response = response;
  }
}

//export for tests
/**
 * Registers a listener for a job progress and triggers data load when job is completed
 * @param {string} datasetVersion
 * @param {string} paginationUrl
 * @param {string} jobId
 * @yields {void}
 * @throws DataLoadError in case if data request returns an error
 */
export function* waitForRunToComplete(datasetVersion, paginationUrl, jobId) {
  try {
    log('Check if socket is opened:', socket.isOpen);
    if (!socket.isOpen) {
      const raceResult = yield race({
        // When explore page is refreshed, we register 'pageChangeListener'
        // (see oss/dac/ui/src/sagas/performLoadDataset.js), which may call this saga
        // earlier, than application is booted ('APP_INIT' action) and socket is opened.
        // We must wait for WS_CONNECTION_OPEN before 'socket.startListenToJobProgress'
        socketOpen: take(WS_CONNECTION_OPEN),
        stop: take(LOGOUT_USER_SUCCESS)
      });
      log('wait for socket open result:', raceResult);
      if (raceResult.stop) {
        // if a user is logged out before socket is opened, terminate current saga
        return;
      }
    }
    yield call([socket, socket.startListenToJobProgress],
      jobId,
      // force listen request to force a response from server.
      // There's no other way right now to know if job is already completed.
      true
    );
    console.warn(`=+=+= socket listener registered for job id ${jobId}`);

    call(explorePageChanged);
    const { jobDone } = yield race({
      jobProgress: call(watchUpdateHistoryOnJobProgress, datasetVersion, jobId),
      jobDone: take(getJobDoneActionFilter(jobId)),
      locationChange: call(explorePageChanged)
    });

    if (jobDone) {
      const promise = yield put(loadNextRows(datasetVersion, paginationUrl, 0));
      const response = yield promise;

      if (!response || response.error) {
        console.warn(`=+=+= socket returned error for job id ${jobId}`);
        throw new DataLoadError(response);
      }

      console.warn(`=+=+= socket returned payload for job id ${jobId}`);
      yield put(updateHistoryWithJobState(datasetVersion, jobDone.payload.update.state));
      yield put(updateExploreJobProgress(jobDone.payload.update));
    }
  } finally {
    yield call([socket, socket.stopListenToJobProgress], jobId);
  }
}


/**
 * Returns a redux action that treated as explore page url change. The action could be one of the following cases:
 * 1) Navigation out of explore page has happen
 * 2) Current dataset or version of a dataset is changed
 * Note: navigation between data/wiki/graph tabs is not treated as page change
 * @yields {object} an redux action
 */
export function* explorePageChanged() {
  const prevRouteParams = yield select(getCurrentRouteParams);
  return yield take([getExplorePageLocationChangePredicate(prevRouteParams), EXPLORE_PAGE_EXIT]);
}


/**
 * Endless job that monitors job progress with id {@see jobId} and updates job state in redux
 * store for particular {@see datasetVersion}
 * @param {string} datasetVersion
 * @param {string} jobId
 */
export function* watchUpdateHistoryOnJobProgress(datasetVersion, jobId) {
  function *updateHistoryOnJobProgress(action) {
    yield put(updateHistoryWithJobState(datasetVersion, action.payload.update.state));
  }

  yield takeEvery(getJobProgressActionFilter(jobId), updateHistoryOnJobProgress);
}

/**
 * handle job status and job running record watches
 */
export function* jobUpdateWatchers(jobId) {
  yield race({
    recordWatcher: call(watchUpdateJobRecords, jobId),
    statusWatcher: call(watchUpdateJobStatus, jobId),
    locationChange: call(explorePageChanged),
    jobDone: take(EXPLORE_JOB_STATUS_DONE)
  });
}

//export for testing
export const EXPLORE_JOB_STATUS_DONE = 'EXPLORE_JOB_STATUS_DONE';

/**
 * monitor job status updates with jobId from the socket
 */
export function* watchUpdateJobStatus(jobId) {
  function *updateJobStatus(action) {
    yield put(updateExploreJobProgress(action.payload.update));
    if (action.payload.update.isComplete) {
      yield put({type: EXPLORE_JOB_STATUS_DONE});
    }
  }

  yield takeEvery(getJobUpdateActionFilter(jobId), updateJobStatus);
}

/**
 * monitor job records updates with jobId from the socket
 */
export function* watchUpdateJobRecords(jobId) {
  function *updateJobProgressWithRecordCount(action) {
    yield put(updateJobRecordCount(action.payload.recordCount));
  }

  yield takeEvery(getJobRecordsActionFilter(jobId), updateJobProgressWithRecordCount);
}




