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
import { all, put, select, takeEvery } from "redux-saga/effects";

import {
  WS_MESSAGE_JOB_DETAILS,
  WS_MESSAGE_JOB_PROGRESS,
  WS_MESSAGE_QV_JOB_PROGRESS,
} from "@inject/utils/socket";

import {
  loadJobDetails,
  loadReflectionJobDetails,
  updateJobState,
  updateQVJobState,
} from "actions/jobs/jobs";
import { getJobList } from "@app/selectors/jobs";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";

const getLocation = (state) => state.routing.locationBeforeTransitions;

function* handleUpdateJobDetails(action) {
  if (action.error) return;

  const location = yield select(getLocation);
  const loc = rmProjectBase(location.pathname);
  if (loc.indexOf("/jobs/reflection/") > -1) {
    const split = loc.split("/");
    const reflectionId = split[split.length - 1];
    yield put(loadReflectionJobDetails(action.payload.jobId.id, reflectionId));
  } else {
    yield put(loadJobDetails(action.payload.jobId.id));
  }
}

function* handleJobProgressChanged(action) {
  if (action.error) return;
  const { payload } = action;
  const id = payload.id.id;
  yield put(updateJobState(id, { ...payload.update, id }));
}

function* handleQVJobProgressChange(action) {
  if (action.error) return;
  const { payload } = action;
  const updatedJob = payload.update;
  const id = payload.id.id;

  const attempts = updatedJob?.attemptDetails || [];
  const hadMultipleAttempts = updatedJob.isComplete && attempts.length > 1;
  yield put(
    updateQVJobState(id, {
      ...payload.update,
      ...(hadMultipleAttempts && {
        jobStatus: attempts[attempts.length - 1].result,
        state: attempts[attempts.length - 1].result,
      }),
      id,
    })
  );
}

export function* entitie() {
  yield all([
    takeEvery(WS_MESSAGE_JOB_DETAILS, handleUpdateJobDetails),
    takeEvery(WS_MESSAGE_JOB_PROGRESS, handleJobProgressChanged),
    takeEvery(WS_MESSAGE_QV_JOB_PROGRESS, handleQVJobProgressChange),
    // takeEvery(RUN_LONG_TRANSFORMATION_SUCCESS, handleStartListenToJobProgress),
  ]);
}

export default entitie;
