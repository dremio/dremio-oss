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
import { take, race, put, call, select, takeEvery } from "redux-saga/effects";
import Immutable from "immutable";
import invariant from "invariant";
import { cloneDeep } from "lodash";
import { getJobStateEvents } from "dremio-ui-common/sonar/JobStateEvents.js";

import {
  loadNextRows,
  EXPLORE_PAGE_EXIT,
  updateExploreJobProgress,
  updateJobRecordCount,
} from "actions/explore/dataset/data";
import { updateHistoryWithJobState } from "actions/explore/history";

import socket, {
  WS_MESSAGE_JOB_PROGRESS,
  WS_MESSAGE_QV_JOB_PROGRESS,
  WS_MESSAGE_JOB_RECORDS,
  WS_CONNECTION_OPEN,
} from "@inject/utils/socket";
import { getExplorePageLocationChangePredicate } from "@app/sagas/utils";
import {
  getTableDataRaw,
  getCurrentRouteParams,
  getExploreState,
} from "@app/selectors/explore";
import { getLocation } from "selectors/routing";
import { log } from "@app/utils/logger";
import { LOGOUT_USER_SUCCESS } from "@app/actions/account";
import {
  resetQueryState,
  setQueryContext,
  setQueryStatuses,
} from "@app/actions/explore/view";
import {
  fetchJobDetails,
  fetchJobSummary,
} from "@app/actions/explore/exploreJobs";
import { replaceScript } from "dremio-ui-common/sonar/scripts/endpoints/replaceScript.js";
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";
import { selectActiveScript } from "@app/components/SQLScripts/sqlScriptsUtils";
import { extractSelections } from "@app/utils/statements/statementParser";
import { setQuerySelectionsInStorage } from "@app/sagas/utils/querySelections";

const getJobDoneActionFilter = (jobId) => (action) =>
  (action.type === WS_MESSAGE_JOB_PROGRESS ||
    action.type === WS_MESSAGE_QV_JOB_PROGRESS) &&
  action.payload.id.id === jobId &&
  action.payload.update.isComplete;

const getJobProgressActionFilter = (jobId) => (action) =>
  (action.type === WS_MESSAGE_JOB_PROGRESS ||
    action.type === WS_MESSAGE_QV_JOB_PROGRESS) &&
  action.payload.id.id === jobId &&
  !action.payload.update.isComplete;

const getJobUpdateActionFilter = (jobId) => (action) =>
  (action.type === WS_MESSAGE_JOB_PROGRESS ||
    action.type === WS_MESSAGE_QV_JOB_PROGRESS) &&
  action.payload.id.id === jobId;

const getJobRecordsActionFilter = (jobId) => (action) =>
  action.type === WS_MESSAGE_JOB_RECORDS && action.payload.id.id === jobId;

/**
 * Load data for a dataset, if data is missing in redux store. Or forces data load if {@see forceReload}
 * set to true
 * @param {string!} datasetVersion - a dataset version
 * @param {string!} jobId - a job id for which data would be requested
 * @param {boolean!} forceReload
 * @param {string!} paginationUrl - put is a last parameter is there is plan to get rid of server
 * generated links
 * @yields {void} An exception may be thrown.
 * @throws DataLoadError
 */
export function* handleResumeRunDataset(
  datasetVersion,
  jobId,
  forceReload,
  paginationUrl,
  isRunOrPreview = true,
) {
  invariant(datasetVersion, "dataset version must be provided");
  invariant(jobId, "jobId must be provided");
  invariant(paginationUrl, "paginationUrl must be provided");

  // we always load data with column information inside, but rows may be missed in case if
  // we load data asynchronously
  const tableData = yield select(getTableDataRaw, datasetVersion);
  const rows = tableData ? tableData.get("rows") : null;
  log(`rows are present = ${!!rows}`);

  // if forceReload = true and data exists, we should not clear data here. As it would be replaced
  // by response in '/reducers/resources/entityReducers/table.js' reducer.
  if (forceReload || !rows) {
    yield race({
      jobDone: call(
        waitForRunToComplete,
        datasetVersion,
        paginationUrl,
        jobId,
        isRunOrPreview,
      ),
      locationChange: call(explorePageChanged),
    });
  }
}

export class DataLoadError {
  constructor(response) {
    this.name = "DataLoadError";
    this.response = response;
  }
}

//export for tests
/**
 * Registers a listener for a job progress and triggers data load when job is completed
 * @param {string} datasetVersion
 * @param {string} paginationUrl
 * @param {string} jobId
 * @yields {void}
 * @throws DataLoadError in case if data request returns an error
 */
export function* waitForRunToComplete(
  datasetVersion,
  paginationUrl,
  jobId,
  isRunOrPreview = true,
) {
  try {
    log("Check if socket is opened:", socket.isOpen);
    if (!socket.isOpen) {
      const raceResult = yield race({
        // When explore page is refreshed, we register 'pageChangeListener'
        // (see oss/dac/ui/src/sagas/performLoadDataset.js), which may call this saga
        // earlier, than application is booted ('APP_INIT' action) and socket is opened.
        // We must wait for WS_CONNECTION_OPEN before 'socket.startListenToJobProgress'
        socketOpen: take(WS_CONNECTION_OPEN),
        stop: take(LOGOUT_USER_SUCCESS),
      });
      log("wait for socket open result:", raceResult);
      if (raceResult.stop) {
        // if a user is logged out before socket is opened, terminate current saga
        return;
      }
    }
    yield call(
      [socket, socket.startListenToJobProgress],
      jobId,
      // force listen request to force a response from server.
      // There's no other way right now to know if job is already completed.
      true,
    );
    console.warn(`=+=+= socket listener registered for job id ${jobId}`);

    call(explorePageChanged);
    const { jobDone } = yield race({
      jobProgress: call(watchUpdateHistoryOnJobProgress, datasetVersion, jobId),
      jobDone: take(getJobDoneActionFilter(jobId)),
      locationChange: call(explorePageChanged),
    });

    // Only load table data when user executes run/preview
    if (jobDone && isRunOrPreview) {
      const promise = yield put(loadNextRows(datasetVersion, paginationUrl, 0));
      const response = yield promise;
      const exploreState = yield select(getExploreState);
      const queryStatuses = cloneDeep(exploreState?.view?.queryStatuses ?? []);

      if (response && response.error) {
        if (queryStatuses.length) {
          const index = queryStatuses.findIndex(
            (query) => query.jobId === jobId,
          );
          if (index > -1 && !queryStatuses[index].error) {
            const newStatuses = cloneDeep(queryStatuses);
            newStatuses[index].error = response;

            if (newStatuses[index].lazyLoad) {
              newStatuses[index].lazyLoad = false;
            }

            yield put(setQueryStatuses({ statuses: newStatuses }));
          }
        }
      }

      if (!response || response.error) {
        console.warn(`=+=+= socket returned error for job id ${jobId}`);
        throw new DataLoadError(response);
      }

      console.warn(`=+=+= socket returned payload for job id ${jobId}`);
      yield put(
        updateHistoryWithJobState(datasetVersion, jobDone.payload.update.state),
      );
      yield put(updateExploreJobProgress(jobDone.payload.update));
      yield call(genLoadJobSummary, jobId);

      // update script jobIds from the details wizard
      const location = yield select(getLocation);

      if (location?.state?.isTransform) {
        const activeScript = yield select(selectActiveScript) || {};

        if (activeScript.id) {
          yield replaceScript(activeScript.id, {
            ...activeScript,
            jobIds: queryStatuses.map((status) => status.jobId),
          });

          ScriptsResource.fetch();

          const newSelections = extractSelections(
            exploreState?.view?.currentSql ?? "",
          );

          setQuerySelectionsInStorage(activeScript.id, newSelections);
        }
      }
    }
  } finally {
    yield call([socket, socket.stopListenToJobProgress], jobId);
  }
}

/**
 * Returns a redux action that treated as explore page url change. The action could be one of the following cases:
 * 1) Navigation out of explore page has happen
 * 2) Current dataset or version of a dataset is changed
 * Note: navigation between data/wiki/graph tabs is not treated as page change
 * This saga has an issue where it will refresh on explore page leave, meaning it will cache
 * the old location and attempt to use it if the explore page is mounted again
 * @yields {object} an redux action
 */
export function* explorePageChanged() {
  const prevRouteParams = yield select(getCurrentRouteParams);

  let shouldReset;
  const promise = yield take([
    (action) => {
      const [result, shouldResetExploreViewState] =
        getExplorePageLocationChangePredicate(prevRouteParams, action);
      shouldReset = shouldResetExploreViewState;
      return result;
    },
    EXPLORE_PAGE_EXIT,
  ]);

  if (shouldReset) {
    const location = yield select(getLocation);
    const nextQueryContext = location.query?.context;

    yield put(resetQueryState());

    if (nextQueryContext) {
      // usually a context parameter in the URL will be surrounded by double-quotes
      // so we need to remove them before setting the editor context
      // context with special characters coming from URL need to be decoded
      yield put(
        setQueryContext({
          context: Immutable.fromJS([
            decodeURIComponent(nextQueryContext.replace(/^"|"$/g, "")),
          ]),
        }),
      );
    }
  }

  return promise;
}

/**
 * Endless job that monitors job progress with id {@see jobId} and updates job state in redux
 * store for particular {@see datasetVersion}
 * @param {string} datasetVersion
 * @param {string} jobId
 */
export function* watchUpdateHistoryOnJobProgress(datasetVersion, jobId) {
  function* updateHistoryOnJobProgress(action) {
    yield put(
      updateHistoryWithJobState(datasetVersion, action.payload.update.state),
    );
  }

  yield takeEvery(
    getJobProgressActionFilter(jobId),
    updateHistoryOnJobProgress,
  );
}

/**
 * handle job status and job running record watches
 */
export function* jobUpdateWatchers(jobId, datasetVersion) {
  yield race({
    recordWatcher: call(watchUpdateJobRecords, jobId, datasetVersion),
    statusWatcher: call(watchUpdateJobStatus, jobId),
    locationChange: call(explorePageChanged),
    jobDone: take(EXPLORE_JOB_STATUS_DONE),
  });
}

//export for testing
export const EXPLORE_JOB_STATUS_DONE = "EXPLORE_JOB_STATUS_DONE";

/**
 * monitor job status updates with jobId from the socket
 */
export function* watchUpdateJobStatus(jobId) {
  function* updateJobStatus(action) {
    yield put(updateExploreJobProgress(action.payload.update));
    if (action.payload.update.isComplete) {
      yield put({ type: EXPLORE_JOB_STATUS_DONE });
    }
  }

  yield takeEvery(getJobUpdateActionFilter(jobId), updateJobStatus);
}

/**
 * monitor job records updates with jobId from the socket
 */
export function* watchUpdateJobRecords(jobId, datasetVersion) {
  function* updateJobProgressWithRecordCount(action) {
    // This isn't sending datasetVersion which results in an undefined entry in tableData
    yield put(updateJobRecordCount(action.payload.recordCount, datasetVersion));
  }

  yield takeEvery(
    getJobRecordsActionFilter(jobId),
    updateJobProgressWithRecordCount,
  );
}

export function* genLoadJobSummary(jobId) {
  // need to fetch jobDetails on job success to get the total job duration and attempt details
  yield put(fetchJobDetails(jobId));

  const summaryPromise = yield put(fetchJobSummary(jobId, 0));
  const jobSummary = yield summaryPromise;

  getJobStateEvents().updateJobState(jobSummary.id, jobSummary);
}
