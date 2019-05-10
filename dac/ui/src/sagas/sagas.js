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
import { all, fork } from 'redux-saga/effects';

import wsEvents from './wsEvents';
import qlik from './qlik';
import serverStatus from './serverStatus';
import autoPeek from './autoPeek';
import downloadDataset from './downloadDataset';
import downloadFile from './downloadFile';
import signupUser from './signupUser';
import performTransform from './performTransform';
import performLoadDataset from './performLoadDataset';
import transformHistoryCheck from './transformHistoryCheck';
import transformCardPreview from './transformCardPreview';

export default function* rootSaga() {
  yield all([
    fork(wsEvents),
    fork(qlik),
    fork(serverStatus),
    fork(autoPeek),
    fork(downloadDataset),
    fork(downloadFile),
    fork(signupUser),
    fork(performTransform),
    fork(performLoadDataset),
    fork(transformHistoryCheck),
    fork(transformCardPreview)
  ]);
}
