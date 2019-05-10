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
import { race, take, put, call, takeEvery } from 'redux-saga/effects';
import { delay } from 'redux-saga';

import socket, { WS_MESSAGE_JOB_PROGRESS } from 'utils/socket';
import { addNotification } from 'actions/notification';


import {
  START_DATASET_DOWNLOAD,
  downloadDataset,
  showDownloadModal
} from 'actions/explore/download';

import { hideConfirmationDialog } from 'actions/confirmation';

import { handleDownloadFile } from './downloadFile';


const LOCATION_CHANGE = '@@router/LOCATION_CHANGE';
const DELAY_BEFORE_MODAL = 1000;

const getJobDoneActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_PROGRESS && action.payload.id.id === jobId && action.payload.update.isComplete;

export default function* () {
  yield takeEvery(START_DATASET_DOWNLOAD, handleStartDatasetDownload);
}

export function* showDownloadModalAfterDelay(jobId) {
  yield call(delay, DELAY_BEFORE_MODAL);
  let action;
  const confirmPromise = new Promise((resolve) => {
    action = showDownloadModal(jobId, resolve);
  });
  yield put(action);

  return yield confirmPromise;
}

export function* handleStartDatasetDownload({ meta }) {
  const { dataset, format } = meta;
  const downloadPromise = yield put(downloadDataset(dataset, format));

  const { response } = yield race({
    response: downloadPromise,
    locationChange: take(LOCATION_CHANGE)
  });
  if (response) {
    if (response.error) {
      return;
    }
    const {downloadUrl, jobId} = response.payload;
    yield call([socket, socket.startListenToJobProgress], jobId.id);
    const { jobDone } = yield race({
      modalShownAndClosed: call(showDownloadModalAfterDelay, jobId.id),
      jobDone: take(getJobDoneActionFilter(jobId.id))
    });

    if (jobDone) {
      yield put(hideConfirmationDialog());
      if (jobDone.payload.update.state !== 'COMPLETED') {
        yield put(addNotification(la('Error preparing job download.'), 'error'));
        return;
      }
    } else {
      yield take(getJobDoneActionFilter(jobId.id));
    }

    yield call(handleDownloadFile, {meta: {url: downloadUrl}});
  }
}
