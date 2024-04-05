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
} from "@app/actions/explore/datasetNew/new";
import {
  newRunDataset,
  newTransformAndRunDataset,
} from "@app/actions/explore/datasetNew/run";
import { newRunTableTransform } from "@app/actions/explore/datasetNew/transform";
import { newLoadDataset } from "@app/sagas/performLoadDatasetNew";
import {
  NewGetFetchDatasetMetaActionProps,
  NewPerformTransformSingleProps,
  HandlePostNewQueryJobSuccessProps,
} from "@app/types/performTransformNewTypes";
import { cancelDataLoad } from "@app/sagas/performLoadDataset";
import { initializeExploreJobProgress } from "@app/actions/explore/dataset/data";
import { submitTransformationJob } from "@app/sagas/transformWatcherNew";
import { setQueryStatuses } from "@app/actions/explore/view";
import { getExploreState } from "@app/selectors/explore";
import {
  fetchJobDetails,
  fetchJobSummary,
} from "@app/actions/explore/exploreJobs";
import { JobSummary } from "@app/exports/types/JobSummary.type";
import { showFailedJobDialog } from "@app/sagas/performTransform";
// @ts-ignore
import { updateTransformData } from "@inject/actions/explore/dataset/updateLocation";
import { EXPLORE_TABLE_ID } from "@app/reducers/explore/view";
import { resetViewState } from "@app/actions/resources";
import { addNotification } from "@app/actions/notification";
import { cloneDeep } from "lodash";
import Immutable from "immutable";
import apiUtils from "@app/utils/apiUtils/apiUtils";
import exploreUtils from "@app/utils/explore/exploreUtils";
import { getLoggingContext } from "dremio-ui-common/contexts/LoggingContext.js";

const logger = getLoggingContext().createLogger("sagas/performTransformNew.ts");

export function* newPerformTransformSingle({
  dataset,
  currentSql,
  queryContext,
  viewId,
  isRun,
  runningSql,
  isSaveViewAs,
  sessionId,
  sqlStatement,
  nextTable,
  finalTransformData,
  references,
}: NewPerformTransformSingleProps): any {
  try {
    const { apiAction, navigateOptions, newVersion } = yield call(
      newGetFetchDatasetMetaAction,
      {
        dataset,
        currentSql: !isSaveViewAs ? sqlStatement : runningSql || currentSql,
        queryContext,
        viewId,
        isRun,
        sessionId,
        nextTable,
        finalTransformData,
        references,
      },
    );

    let response;

    if (apiAction) {
      yield call(cancelDataLoad);
      yield put(initializeExploreJobProgress(isRun));
      response = yield call(submitTransformationJob, apiAction, viewId);
    }

    return [response, navigateOptions, newVersion];
  } catch (e) {
    return [e];
  }
}

export function* newGetFetchDatasetMetaAction({
  dataset,
  currentSql,
  queryContext,
  viewId,
  isRun,
  sessionId,
  nextTable,
  finalTransformData,
  references,
}: NewGetFetchDatasetMetaActionProps): any {
  const sql = currentSql || dataset.get("sql");
  const isNotDataset =
    !dataset.get("datasetVersion") ||
    (!dataset.get("datasetType") && !dataset.get("sql"));

  let apiAction;
  let navigateOptions;
  let newVersion = exploreUtils.getNewDatasetVersion();

  if (isRun) {
    if (isNotDataset) {
      apiAction = yield call(
        newTmpUntitledSqlAndRun,
        sql,
        queryContext,
        viewId,
        references,
        sessionId,
        newVersion,
      );

      navigateOptions = { changePathName: true };
    } else if (finalTransformData) {
      updateTransformData(finalTransformData);
      yield put(resetViewState(EXPLORE_TABLE_ID));

      apiAction = yield call(
        newTransformAndRunDataset,
        dataset,
        finalTransformData,
        viewId,
        sessionId,
        newVersion,
      );
    } else {
      apiAction = yield call(newRunDataset, dataset, viewId, sessionId);
      navigateOptions = { replaceNav: true, preserveTip: true };
      newVersion = dataset.get("datasetVersion");
    }
  } else {
    if (isNotDataset) {
      apiAction = yield call(
        newTmpUntitledSql,
        sql,
        queryContext?.toJS(),
        viewId,
        references,
        sessionId,
        newVersion,
      );

      navigateOptions = { changePathName: true };
    } else if (finalTransformData) {
      apiAction = yield call(
        newRunTableTransform,
        dataset,
        finalTransformData,
        viewId,
        nextTable,
        sessionId,
        newVersion,
      );
    } else {
      apiAction = yield call(newLoadDataset, dataset, viewId, sessionId);
      navigateOptions = { replaceNav: true, preserveTip: true };
      newVersion = dataset.get("datasetVersion");
    }
  }

  return { apiAction, navigateOptions, newVersion };
}

export function* handlePostNewQueryJobSuccess({
  response,
  newVersion,
  queryStatuses,
  curIndex,
  callback,
  tabId,
}: HandlePostNewQueryJobSuccessProps) {
  const {
    dataset,
    datasetPath,
    datasetVersion,
    jobId,
    paginationUrl,
    sessionId,
  } = apiUtils.getFromJSONResponse(response);

  const versionToUse = datasetVersion ?? newVersion;

  const mostRecentStatuses = queryStatuses;

  //Tabs: mostRecentStatuses[curIndex] is undefined when switching tabs sometimes
  if (mostRecentStatuses[curIndex]) {
    mostRecentStatuses[curIndex].jobId = jobId;
    mostRecentStatuses[curIndex].version = versionToUse;
    mostRecentStatuses[curIndex].paginationUrl = paginationUrl;

    if (mostRecentStatuses[curIndex].cancelled) {
      mostRecentStatuses[curIndex].cancelled = false;
    }
  } else {
    logger.debug(
      `handlePostNewQueryJobSuccess: Could not find curIndex '${curIndex}' in mostRecentStatuses. Nothing updated`,
      mostRecentStatuses,
    );
  }

  // updated queryStatuses in Redux after job is submitted
  // and only if user is not trying to save a new view
  if (!callback) {
    yield put(setQueryStatuses({ statuses: mostRecentStatuses, tabId }));
  }

  // fetch job details on job submissions for engine name and query type
  // @ts-expect-error return type of fetchJobDetails is not an action
  yield put(fetchJobDetails(jobId));

  return [dataset, datasetPath, versionToUse, jobId, paginationUrl, sessionId];
}

export function* fetchJobFailureInfo(
  jobId: string,
  curIndex: number,
  callback: any,
): any {
  const exploreState = yield select(getExploreState);
  const mostRecentStatuses = cloneDeep(exploreState?.view?.queryStatuses);
  const isLastQuery = mostRecentStatuses.length - 1 === curIndex;

  try {
    // need to fetch jobDetails on job failure to get the total job duration and attempt details
    // @ts-expect-error return type of fetchJobDetails is not an action
    yield put(fetchJobDetails(jobId));

    // @ts-expect-error return type of fetchJobSummary is not an action
    const summaryPromise = yield put(fetchJobSummary(jobId, 0));
    const { cancellationInfo, failureInfo }: JobSummary = yield summaryPromise;
    const error = failureInfo.errors.at(0); // used in case index doesn't exist

    let willProceed = true;

    if (!callback && !isLastQuery) {
      // canceling a job throws a cancellation error, exit the multi-sql loop
      if (!cancellationInfo) {
        const isParseError = failureInfo.type === "PARSE";

        // Tabs: mostRecentStatuses[curIndex] is sometimes undefined
        if (
          mostRecentStatuses[curIndex] &&
          //Prevent showing error dialog for cancelled queries (Switching tabs will cancel errored queries, see scriptUpdates)
          !mostRecentStatuses[curIndex].cancelled
        ) {
          // if a job wasn't cancelled but still failed, show the dialog
          willProceed = yield call(
            showFailedJobDialog,
            curIndex,
            mostRecentStatuses[curIndex].sqlStatement,
            isParseError ? undefined : error?.message ?? failureInfo.message,
          );
        }
      }
    } else if (callback) {
      willProceed = false;

      yield put(
        addNotification(
          apiUtils.getThrownErrorException(error ?? failureInfo),
          "error",
          10,
        ),
      );
    }

    if (!callback) {
      if (cancellationInfo) {
        mostRecentStatuses[curIndex].cancelled = true;
      } else {
        mostRecentStatuses[curIndex].error = Immutable.fromJS(
          error ?? failureInfo,
        );
      }

      yield put(setQueryStatuses({ statuses: mostRecentStatuses }));
    }

    return willProceed;
  } catch (err: any) {
    yield put(addNotification(err.responseBody.errorMessage, "error", 10));
    return false;
  }
}
