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

import { call, put, select } from "redux-saga/effects";
import {
  newTmpUntitledSql,
  newTmpUntitledSqlAndRun,
} from "@app/actions/explore/dataset/new";
import exploreUtils from "@app/utils/explore/exploreUtils";
import {
  PostNewQueryJobProps,
  GenerateRequestForNewDatasetProps,
  HandlePostNewQueryJobSuccessProps,
} from "@app/utils/performTransform/newTransform";
import { getNessieReferences } from "./nessie";
import { cancelDataLoad } from "./performLoadDataset";
import { initializeExploreJobProgress } from "@app/actions/explore/dataset/data";
import { fetchJobMetadata } from "./transformWatcher";
import { setQueryStatuses } from "@app/actions/explore/view";
import apiUtils from "@app/utils/apiUtils/apiUtils";
import { getExploreState } from "@app/selectors/explore";
import { cloneDeep } from "lodash";
import { loadJobDetails } from "@app/actions/jobs/jobs";
import { JOB_DETAILS_VIEW_ID } from "@app/actions/joblist/jobList";
import { showFailedJobDialog } from "./performTransform";
import { addNotification } from "@app/actions/notification";
import Immutable from "immutable";

export function* postNewQueryJob({
  currentSql,
  queryContext,
  viewId,
  isRun,
  runningSql,
  isSaveViewAs,
  sessionId,
  sqlStatement,
}: PostNewQueryJobProps): any {
  try {
    const { apiAction } = yield call(generateRequestForNewDataset, {
      sql: !isSaveViewAs ? sqlStatement : runningSql || currentSql,
      queryContext,
      viewId,
      isRun,
      sessionId,
      noUpdate: true,
    });

    let response;

    if (apiAction) {
      yield call(cancelDataLoad);
      yield put(initializeExploreJobProgress(isRun));

      response = yield call(fetchJobMetadata, apiAction, viewId);
    }

    return [response];
  } catch (e) {
    return [e];
  }
}

export function* generateRequestForNewDataset({
  sql,
  queryContext,
  viewId,
  isRun,
  sessionId,
  noUpdate,
}: GenerateRequestForNewDatasetProps): any {
  const references = yield getNessieReferences();
  const newVersion = exploreUtils.getNewDatasetVersion();
  let apiAction;

  if (isRun) {
    apiAction = yield call(
      newTmpUntitledSqlAndRun,
      sql,
      queryContext,
      viewId,
      references,
      sessionId,
      newVersion,
      noUpdate
    );
  } else {
    apiAction = yield call(
      newTmpUntitledSql,
      sql,
      queryContext?.toJS(),
      viewId,
      references,
      sessionId,
      newVersion,
      noUpdate
    );
  }

  return { apiAction };
}

export function* handlePostNewQueryJobSuccess({
  response,
  queryStatuses,
  curIndex,
  callback,
}: HandlePostNewQueryJobSuccessProps) {
  const { datasetPath, datasetVersion, jobId, paginationUrl, sessionId } =
    apiUtils.getFromNewQueryResponse(response);

  const mostRecentStatuses = queryStatuses;
  mostRecentStatuses[curIndex].jobId = jobId;
  mostRecentStatuses[curIndex].version = datasetVersion;

  if (mostRecentStatuses[curIndex].cancelled) {
    mostRecentStatuses[curIndex].cancelled = false;
  }

  // updated queryStatuses in Redux after job is submitted
  // and only if user is not trying to save a new view
  if (!callback) {
    yield put(setQueryStatuses({ statuses: mostRecentStatuses }));
  }

  return [datasetPath, datasetVersion, jobId, paginationUrl, sessionId];
}

export function* fetchJobFailureInfo(
  jobId: string,
  curIndex: number,
  callback: any
): any {
  const exploreState: any = yield select(getExploreState);
  const mostRecentStatuses = cloneDeep(exploreState?.view?.queryStatuses);
  const isLastQuery = mostRecentStatuses.length - 1 === curIndex;

  // @ts-ignore
  const jobDetails = yield put(loadJobDetails(jobId, JOB_DETAILS_VIEW_ID));
  const jobDetailsResponse = yield jobDetails;
  const error = jobDetailsResponse.payload.getIn([
    "entities",
    "jobDetails",
    jobId,
    "failureInfo",
    "errors",
    0,
  ]);

  const cancellationInfo = jobDetailsResponse.payload.getIn([
    "entities",
    "jobDetails",
    jobId,
    "cancellationInfo",
  ]);

  let willProceed = true;

  if (!callback && !isLastQuery) {
    // canceling a job throws a cancellation error, exit the multi-sql loop
    if (cancellationInfo) {
      willProceed = false;
    } else {
      const isParseError =
        error?.get("message") === "Failure parsing the query.";

      // if a job wasn't cancelled but still failed, show the dialog
      willProceed = yield call(
        showFailedJobDialog,
        curIndex,
        mostRecentStatuses[curIndex].sqlStatement,
        isParseError ? undefined : error?.get("message")
      );
    }
  } else if (callback) {
    willProceed = false;

    yield put(
      addNotification(
        apiUtils.getThrownErrorException(error ?? ""),
        "error",
        10
      )
    );
  }

  if (!callback) {
    const cancellationInfo = jobDetailsResponse.payload.getIn([
      "entities",
      "jobDetails",
      jobId,
      "cancellationInfo",
    ]);

    if (cancellationInfo) {
      mostRecentStatuses[curIndex].cancelled = true;
    } else {
      // @ts-ignore
      mostRecentStatuses[curIndex].error = new Immutable.Map(error);
    }

    yield put(setQueryStatuses({ statuses: mostRecentStatuses }));
  }

  return willProceed;
}
