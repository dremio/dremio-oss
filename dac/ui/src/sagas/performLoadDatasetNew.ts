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

import { call, put, race, select, spawn, take } from "redux-saga/effects";
import { getLocation } from "@app/selectors/routing";
import {
  cancelDataLoad,
  CANCEL_TABLE_DATA_LOAD,
  hideTableSpinner,
  resetTableViewStateOnPageLeave,
} from "@app/sagas/performLoadDataset";
import { DataLoadError, jobUpdateWatchers } from "@app/sagas/runDataset";
import { loadDatasetMetadata } from "@app/sagas/runDatasetNew";
import { newLoadExistingDataset } from "@app/actions/explore/datasetNew/edit";
import { setExploreJobIdInProgress } from "@app/actions/explore/dataset/data";
import { updateViewState } from "@app/actions/resources/index";
import { TRANSFORM_PEEK_START } from "@app/actions/explore/dataset/peek";
import { EXPLORE_TABLE_ID } from "@app/reducers/explore/view";
import { getViewStateFromAction } from "@app/reducers/resources/view";
// @ts-ignore
import { sonarEvents } from "dremio-ui-common/sonar/sonarEvents.js";
import Immutable from "immutable";
import { getLoggingContext } from "dremio-ui-common/contexts/LoggingContext.js";

const logger = getLoggingContext().createLogger(
  "sagas/performLoadDatasetNew.ts"
);

export function* listenToJobProgress(
  dataset: Immutable.Map<string, any>,
  datasetVersion: string,
  jobId: string,
  paginationUrl: string,
  navigateOptions: Record<string, any>,
  isRun: boolean,
  datasetPath: string,
  callback: any,
  curIndex: number,
  sessionId: string,
  viewId: string,
  tabId: string
): any {
  let resetViewState = true;
  let raceResult;

  // cancels any other data loads before beginning
  yield call(cancelDataLoad);

  // track all preview queries
  if (!isRun && datasetVersion) {
    sonarEvents.jobPreview();
  }

  try {
    // Tabs: Need to namespace this state as well, send tabId in
    yield put(setExploreJobIdInProgress(jobId, datasetVersion));
    yield spawn(jobUpdateWatchers, jobId, datasetVersion);

    yield put(
      updateViewState(
        EXPLORE_TABLE_ID,
        {
          isInProgress: true,
          isFailed: false,
          error: null,
        },
        { tabId }
      )
    );

    raceResult = yield race({
      dataLoaded: call(
        loadDatasetMetadata,
        dataset,
        datasetVersion,
        jobId,
        paginationUrl,
        navigateOptions,
        datasetPath,
        callback,
        curIndex,
        sessionId,
        viewId,
        tabId
      ),
      isLoadCanceled: take([CANCEL_TABLE_DATA_LOAD, TRANSFORM_PEEK_START]),
      locationChange: call(resetTableViewStateOnPageLeave),
    });
    logger.debug("loadDatasetMetadata raceResult: ", raceResult);
  } catch (e) {
    if (!(e instanceof DataLoadError)) {
      throw e;
    }

    resetViewState = false;
    const viewState = yield call(getViewStateFromAction, e.response);
    yield put(updateViewState(EXPLORE_TABLE_ID, viewState, { tabId }));
  } finally {
    if (resetViewState) {
      yield call(hideTableSpinner, tabId);
    }
  }

  return raceResult.dataLoaded ?? false;
}

export function* newLoadDataset(
  dataset: Immutable.Map<string, any>,
  viewId: string,
  sessionId: string
): any {
  const location = yield select(getLocation);
  const { tipVersion } = location.query || {};

  return yield call(
    newLoadExistingDataset,
    dataset,
    viewId,
    tipVersion,
    sessionId
    // Tabs: may need to send originalScriptId for meta
  );
}
