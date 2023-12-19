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

import { call, put, race, select, take } from "redux-saga/effects";
import {
  DataLoadError,
  explorePageChanged,
  genLoadJobDetails,
  watchUpdateHistoryOnJobProgress,
} from "@app/sagas/runDataset";
import { fetchJobFailureInfo } from "@app/sagas/performTransformNew";
import { fetchDatasetMetadata } from "@app/sagas/transformWatcherNew";
import { LOGOUT_USER_SUCCESS } from "@app/actions/account";
import { navigateToNextDataset } from "@app/actions/explore/dataset/common";
import {
  loadNextRows,
  startExplorePageListener,
  stopExplorePageListener,
  updateExploreJobProgress,
} from "@app/actions/explore/dataset/data";
import { loadNewDataset } from "@app/actions/explore/datasetNew/edit";
import { setQueryStatuses } from "@app/actions/explore/view";
import { updateHistoryWithJobState } from "@app/actions/explore/history";
import { getExploreState } from "@app/selectors/explore";
import socket, {
  // @ts-ignore
  WS_MESSAGE_JOB_PROGRESS,
  // @ts-ignore
  WS_MESSAGE_QV_JOB_PROGRESS,
  //  @ts-ignore
  WS_CONNECTION_OPEN,
} from "@inject/utils/socket";
import { cloneDeep } from "lodash";
import Immutable from "immutable";
import apiUtils from "@app/utils/apiUtils/apiUtils";
import { updateScriptContext } from "@app/sagas/scriptContext";

class JobFailedError {
  name: string;
  response: string;

  constructor(response: string) {
    this.name = "JobFailedError";
    this.response = response;
  }
}

export const getJobDoneActionFilter =
  (jobId: string) => (action: Record<any, any>) =>
    (action.type === WS_MESSAGE_JOB_PROGRESS ||
      action.type === WS_MESSAGE_QV_JOB_PROGRESS) &&
    action.payload.id.id === jobId &&
    action.payload.update.isComplete;

export function* loadDatasetMetadata(
  dataset: Immutable.Map<string, any>,
  datasetVersion: string,
  jobId: string,
  paginationUrl: string,
  navigateOptions: Record<string, any>,
  datasetPath: string,
  callback: any,
  curIndex: number,
  sessionId: string,
  viewId: string,
  tabId: string
): any {
  const { jobDone } = yield race({
    jobDone: call(
      handlePendingMetadataFetch,
      dataset,
      datasetVersion,
      jobId,
      paginationUrl,
      datasetPath,
      callback,
      curIndex,
      sessionId,
      viewId,
      tabId
    ),
    locationChange: call(explorePageChanged),
  });

  const willProceed = jobDone?.willProceed ?? false;
  const newResponse = jobDone?.newResponse;

  if (newResponse) {
    yield put(stopExplorePageListener());

    // Tabs: Skip navigation if activeScriptId has changed since original side-effect was started (user has changed tabs)
    if (!tabId) {
      yield call(updateScriptContext, sessionId);

      yield put(
        // @ts-ignore
        navigateToNextDataset(newResponse, {
          ...navigateOptions,
          newJobId: jobId,
        })
      );
    }
    yield put(startExplorePageListener(false));
  }

  if (callback && newResponse !== undefined) {
    const resultDataset = apiUtils.getEntityFromResponse(
      "datasetUI",
      newResponse
    );

    yield call(callback, true, resultDataset);
  }

  return willProceed;
}

export function* handlePendingMetadataFetch(
  dataset: Immutable.Map<string, any>,
  datasetVersion: string,
  jobId: string,
  paginationUrl: string,
  datasetPath: string,
  callback: any,
  curIndex: number,
  sessionId: string,
  viewId: string,
  tabId: string
): any {
  let willProceed = true;
  let newResponse;

  try {
    // @ts-ignore
    if (!socket.isOpen) {
      const raceResult = yield race({
        socketOpen: take(WS_CONNECTION_OPEN),
        stop: take(LOGOUT_USER_SUCCESS),
      });

      if (raceResult.stop) {
        return;
      }
    }

    // @ts-ignore
    yield call([socket, socket.startListenToJobProgress], jobId, true);

    const { jobDone } = yield race({
      jobProgress: call(watchUpdateHistoryOnJobProgress, datasetVersion, jobId),
      jobDone: take(getJobDoneActionFilter(jobId)),
      locationChange: call(explorePageChanged),
    });

    if (jobDone) {
      // if a job fails, throw an error to avoid calling the /preview endpoint
      const updatedJob = jobDone.payload?.update;
      const attempts = updatedJob?.attemptDetails || [];
      if (
        updatedJob.state === "FAILED" &&
        attempts.length &&
        attempts[attempts.length - 1].result === "FAILED"
      ) {
        const failureInfo = jobDone.payload.update.failureInfo;
        throw new JobFailedError(
          failureInfo?.errors?.[0]?.message || failureInfo?.message
        );
      }

      const apiAction = yield call(
        loadNewDataset,
        dataset,
        datasetPath,
        sessionId,
        datasetVersion,
        jobId,
        paginationUrl,
        viewId,
        tabId
      );

      if (apiAction === undefined) {
        throw new JobFailedError("Failed to fetch dataset.");
      }

      newResponse = yield call(fetchDatasetMetadata, apiAction, viewId);

      if (!callback) {
        const promise = yield put(
          // @ts-ignore
          loadNextRows(datasetVersion, paginationUrl, 0)
        );
        const response = yield promise;
        const exploreState = yield select(getExploreState);
        const queryStatuses = cloneDeep(
          exploreState?.view?.queryStatuses ?? []
        );

        if (response?.error) {
          if (queryStatuses.length) {
            const index = queryStatuses.findIndex(
              (query: any) => query.jobId === jobId
            );

            if (index > -1 && !queryStatuses[index].error) {
              const newStatuses = cloneDeep(queryStatuses);
              newStatuses[index].error = response;
              yield put(setQueryStatuses({ statuses: newStatuses }));
            }
          }
        }

        if (!response || response.error) {
          throw new DataLoadError(response);
        }

        yield put(
          updateHistoryWithJobState(
            datasetVersion,
            jobDone.payload.update.state
          )
        );
        yield put(updateExploreJobProgress(jobDone.payload.update));
        yield call(genLoadJobDetails, jobId);
      }
    }
  } catch (e) {
    // if a job fails, fetch the correct job failure info using the Jobs API
    willProceed = yield fetchJobFailureInfo(jobId, curIndex, callback);
  } finally {
    // @ts-ignore
    yield call([socket, socket.stopListenToJobProgress], jobId);
  }

  return { willProceed, newResponse };
}
