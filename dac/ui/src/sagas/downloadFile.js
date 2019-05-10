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
import { put, call, takeEvery } from 'redux-saga/effects';

import { API_URL_V2 } from 'constants/Api';

import { updateViewState } from 'actions/resources';
import { addNotification } from 'actions/notification';

import FileUtils from 'utils/FileUtils';

const DOWNLOAD_FILE = 'DOWNLOAD_FILE';

export default function* download() {
  yield takeEvery(DOWNLOAD_FILE, handleDownloadFile);
}

export function* handleDownloadFile(action) {
  const {url, viewId, method = 'GET'} = action.meta;

  if (viewId) yield put(updateViewState(viewId, { isInProgress: true }));

  const headers = FileUtils.getHeaders();
  const res = yield call(fetch, `${API_URL_V2}${url}`, {method, headers});

  try {
    const downloadConfig = yield call([FileUtils, FileUtils.getFileDownloadConfigFromResponse], res);
    yield call(FileUtils.downloadFile, downloadConfig);
    if (viewId) yield put(updateViewState(viewId, { isInProgress: false }));
  } catch (e) {
    yield put(addNotification(e.message, 'error'));
    if (viewId) yield put(updateViewState(viewId, { isInProgress: false, isFailed: true }));
  }
}

export const downloadFile = (meta) => {
  return {type: DOWNLOAD_FILE, meta};
};
