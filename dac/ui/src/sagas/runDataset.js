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
import { take, race, put, call, select, takeEvery } from 'redux-saga/effects';

import { updateViewState } from 'actions/resources';

import { loadNextRows } from 'actions/explore/dataset/data';
import { updateHistoryWithJobState } from 'actions/explore/history';
import { getViewStateFromAction, getDefaultViewConfig } from '@app/reducers/resources/view';
import { getExplorePageLocationChangePredicate } from '@app/sagas/utils';

import socket, { WS_MESSAGE_JOB_PROGRESS } from 'utils/socket';
import { addNotification } from 'actions/notification';

import Immutable from 'immutable';

export const getEntities = state => state.resources.entities;

const getJobDoneActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_PROGRESS && action.payload.id.id === jobId && action.payload.update.isComplete;

const getJobProgressActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_PROGRESS && action.payload.id.id === jobId && !action.payload.update.isComplete;

export function* handleResumeRunDataset(datasetVersion, forceReload) {
  const entities = yield select(getEntities);

  // we always load data with column information inside, but rows may be missed in case if
  // we load data asynchronously
  const tableData = entities.getIn(['tableData', datasetVersion, 'rows']);
  let resultViewState;

  // if forceReload = true and data exists, we should not clear data here. As it would be replaced
  // by response in '/reducers/resources/entityReducers/table.js' reducer.
  if (forceReload || !tableData) {
    const datasetUI = entities.getIn(['datasetUI', datasetVersion]);
    const fullDataset = entities.getIn(['fullDataset', datasetVersion]);
    const jobId = fullDataset.getIn(['jobId', 'id'], null);

    if (fullDataset.get('paginationUrl') && jobId) {
      resultViewState = yield call(waitForRunToComplete, datasetUI, fullDataset.get('paginationUrl'), jobId);
    }
  }

  return resultViewState || getDefaultViewConfig(); // must return a not empty value
}

//todo only dataset version is needed here. No need to pass whole dataset
export function* waitForRunToComplete(dataset, paginationUrl, jobId) {
  const viewStateId = 'run-' + jobId;
  yield call([socket, socket.startListenToJobProgress],
    jobId,
    // force listen request to force a response from server.
    // There's no other way right now to know if job is already completed.
    true
  );
  yield put(updateViewState(viewStateId, { isInProgress: true }));

  if (!socket.isOpen) yield put(addNotification(Immutable.Map({code: 'WS_CLOSED'}), 'error'));

  const locationChangedPredicate = yield call(getExplorePageLocationChangePredicate);
  const { jobDone } = yield race({
    jobProgress: call(watchUpdateHistoryOnJobProgress, dataset, jobId),
    jobDone: take(getJobDoneActionFilter(jobId)),
    locationChange: take(locationChangedPredicate)
  });

  let resultViewState = null;
  if (jobDone) {
    const promise = yield put(loadNextRows(dataset.get('datasetVersion'), paginationUrl, 0, viewStateId));
    const lastAction = yield promise;
    resultViewState = yield call(getViewStateFromAction, lastAction);

    yield put(updateHistoryWithJobState(dataset, jobDone.payload.update.state));
  }

  yield call([socket, socket.stopListenToJobProgress], jobId);
  return resultViewState;
}

export function* watchUpdateHistoryOnJobProgress(dataset, jobId) {
  function *updateHistoryOnJobProgress(action) {
    yield put(updateHistoryWithJobState(dataset, action.payload.update.state));
  }

  yield takeEvery(getJobProgressActionFilter(jobId), updateHistoryOnJobProgress);
}




