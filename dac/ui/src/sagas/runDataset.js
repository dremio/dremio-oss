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

import socket, { WS_MESSAGE_JOB_PROGRESS } from 'utils/socket';
import { addNotification } from 'actions/notification';

import Immutable from 'immutable';

import {
  RESUME_RUN_DATASET
} from 'actions/explore/dataset/run';

const LOCATION_CHANGE = '@@router/LOCATION_CHANGE';
export const getLocation = state => state.routing.locationBeforeTransitions;
export const getEntities = state => state.resources.entities;

const getJobDoneActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_PROGRESS && action.payload.id.id === jobId && action.payload.update.isComplete;

const getJobProgressActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_PROGRESS && action.payload.id.id === jobId && !action.payload.update.isComplete;

export default function* watchRunDataset() {
  yield [
    takeEvery(RESUME_RUN_DATASET, handleResumeRunDataset)
  ];
}


export function* handleResumeRunDataset({ datasetId }) {
  const entities = yield select(getEntities);
  const tableData = entities.getIn(['tableData', datasetId]);
  const location = yield select(getLocation);
  const { jobId } = location.query;
  if (jobId && !tableData) {
    const datasetUI = entities.getIn(['datasetUI', datasetId]);
    const fullDataset = entities.getIn(['fullDataset', datasetId]);
    if (fullDataset.get('paginationUrl')) {
      yield call(waitForRunToComplete, datasetUI, fullDataset.get('paginationUrl'), jobId);
    }
  }
}


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

  const { jobDone } = yield race({
    jobProgress: call(watchUpdateHistoryOnJobProgress, dataset, jobId),
    jobDone: take(getJobDoneActionFilter(jobId)),
    locationChange: take(LOCATION_CHANGE)
  });

  if (jobDone) {
    const promise = yield put(loadNextRows(dataset.get('datasetVersion'), paginationUrl, 0, viewStateId));
    yield promise;
    yield put(updateHistoryWithJobState(dataset, jobDone.payload.update.state));
  }

  yield call([socket, socket.stopListenToJobProgress], jobId);
}

export function* watchUpdateHistoryOnJobProgress(dataset, jobId) {
  function *updateHistoryOnJobProgress(action) {
    yield put(updateHistoryWithJobState(dataset, action.payload.update.state));
  }

  yield takeEvery(getJobProgressActionFilter(jobId), updateHistoryOnJobProgress);
}




