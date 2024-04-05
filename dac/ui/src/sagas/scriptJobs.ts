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

import Immutable from "immutable";
// @ts-expect-error ui-common type missing
import { getLoggingContext } from "dremio-ui-common/contexts/LoggingContext.js";
// @ts-expect-error ui-common type missing
import type { Script } from "dremio-ui-common/sonar/scripts/Script.type.js";
import {
  all,
  call,
  put,
  race,
  spawn,
  take,
  takeLatest,
} from "redux-saga/effects";
import {
  initializeExploreJobProgress,
  setExploreJobIdInProgress,
} from "@app/actions/explore/dataset/data";
import { TRANSFORM_PEEK_START } from "@app/actions/explore/dataset/peek";
import {
  clearExploreJobs,
  fetchJobDetails,
  fetchJobSummary,
} from "@app/actions/explore/exploreJobs";
import {
  focusSqlEditor,
  setQuerySelections,
  setQueryStatuses,
  setQueryTabNumber,
} from "@app/actions/explore/view";
import { updateViewState } from "@app/actions/resources";
import { LOAD_SCRIPT_JOBS } from "@app/actions/resources/scripts";
import { EXPLORE_TABLE_ID, EXPLORE_VIEW_ID } from "@app/reducers/explore/view";
import { getViewStateFromAction } from "@app/reducers/resources/view";
import {
  CANCEL_TABLE_DATA_LOAD,
  hideTableSpinner,
  loadDataset,
  resetTableViewStateOnPageLeave,
} from "@app/sagas/performLoadDataset";
import {
  DataLoadError,
  handleResumeRunDataset,
  jobUpdateWatchers,
} from "@app/sagas/runDataset";
import { transformThenNavigate } from "@app/sagas/transformWatcher";
import { getQuerySelectionsFromStorage } from "@app/sagas/utils/querySelections";
import { getDatasetPreview } from "@app/exports/endpoints/Datasets/getDatasetPreview";
import { JobDetails } from "@app/exports/types/JobDetails.type";
import apiUtils from "@app/utils/apiUtils/apiUtils";
import { QueryRange } from "@app/utils/statements/statement";

const logger = getLoggingContext().createLogger("sagas/scriptJobs");

export default function* scriptJobs() {
  yield takeLatest(LOAD_SCRIPT_JOBS, handleLoadScriptJobs);
}

function* handleLoadScriptJobs({
  script,
}: {
  type: typeof LOAD_SCRIPT_JOBS; // unused, but declared to prevent a ts error
  script: Script;
}): unknown {
  const {
    id,
    jobIds,
    jobResultUrls,
  }: { id: string; jobIds: string[]; jobResultUrls: string[] } = script;

  logger.debug("handleLoadScriptJobs checking jobIds", jobIds);

  if (
    !jobIds.length ||
    !jobResultUrls.length ||
    jobIds.length !== jobResultUrls.length
  ) {
    return;
  }

  // prevents jobs from appearing in other tabs when switching scripts
  yield put(clearExploreJobs());

  let selections: QueryRange[] = [];
  const allQuerySelections = getQuerySelectionsFromStorage();
  const curScriptSelections = allQuerySelections[id] ?? [];

  if (curScriptSelections.length === jobIds.length) {
    selections = curScriptSelections;
  } else {
    // use default selections if local storage is out of sync with script
    selections = jobIds.map(() => ({
      startLineNumber: 1,
      startColumn: 1,
      endLineNumber: 1,
      endColumn: 1,
    }));
  }

  yield put(
    setQuerySelections({
      selections,
      tabId: undefined,
    }),
  );

  if (jobIds.length > 1) {
    yield put(setQueryTabNumber({ tabNumber: 0 }));
  }

  try {
    // need to use job details instead of summaries because datasetPaths is missing from the latter
    const jobsPromises: Promise<JobDetails>[] = yield all(
      // @ts-expect-error return type of fetchJobDetails is not an action
      jobIds.map((jobId) => put(fetchJobDetails(jobId))),
    );

    const resolvedJobs: JobDetails[] = yield all(jobsPromises);

    const queryStatuses: Record<string, any>[] = [];

    resolvedJobs.forEach((resolvedJob) =>
      queryStatuses.push({
        sqlStatement: resolvedJob.description,
        cancelled: false,
      }),
    );

    // initialize query statuses to display the query tabs
    yield put(
      setQueryStatuses({
        statuses: [...queryStatuses],
      }),
    );

    for (const [index, jobId] of jobIds.entries()) {
      logger.debug("handleLoadScriptJobs loading job", jobId);

      const jobDetails = resolvedJobs[index];

      // need to handle job failure separately since the call to /preview fails if the job fails
      if (["FAILED", "CANCELED"].includes(jobDetails.jobStatus)) {
        // Need to fetch summaries for failed jobs to appear in jobs table
        // @ts-expect-error return type of fetchJobSummary is not an action
        yield put(fetchJobSummary(jobId, 0));

        const { cancellationInfo, failureInfo } = jobDetails;
        const error = failureInfo.errors.at(0);

        queryStatuses[index] = {
          cancelled: !!cancellationInfo,
          jobId,
          sqlStatement: jobDetails.description,
          version: jobDetails.datasetVersion,
          error: Immutable.fromJS(cancellationInfo ?? error ?? failureInfo),
        };

        yield put(
          setQueryStatuses({
            statuses: [...queryStatuses],
          }),
        );

        continue;
      }

      const viewMetadata = yield getDatasetPreview(
        jobDetails.datasetPaths,
        jobDetails.datasetVersion,
        false,
      );

      queryStatuses[index] = {
        cancelled: false,
        jobId,
        sqlStatement: viewMetadata.dataset.sql,
        version: viewMetadata.dataset.datasetVersion,
      };

      yield put(
        setQueryStatuses({
          statuses: [...queryStatuses],
        }),
      );

      // taken from handlePerformLoadDataset saga
      const apiAction = yield call(
        loadDataset,
        Immutable.fromJS({ ...viewMetadata.dataset, jobId }),
        EXPLORE_VIEW_ID,
        undefined /* forceDataLoad */,
        undefined /* sessionId */,
        true /* willLoadTable */,
      );

      const response = yield call(
        transformThenNavigate,
        apiAction,
        EXPLORE_VIEW_ID,
        {
          replaceNav: true,
          preserveTip: true,
        },
      );

      const nextFullDataset = apiUtils.getEntityFromResponse(
        "fullDataset",
        response,
      );

      yield call(focusSqlEditor);

      if (nextFullDataset) {
        if (nextFullDataset.get("error")) {
          yield put(
            updateViewState(EXPLORE_VIEW_ID, {
              isFailed: true,
              error: {
                message: nextFullDataset.getIn(["error", "errorMessage"]),
              },
            }),
          );
        } else {
          yield put(initializeExploreJobProgress(false)); // Sets if the initial dataset load is a `Run`
          yield call(
            loadScriptTableData,
            jobId,
            viewMetadata.dataset,
            jobResultUrls.find((jobResultUrl) => jobResultUrl.includes(jobId)),
          );
        }
      }
    }
  } catch (e: any) {
    logger.error("handleLoadScriptJobs error", e);
    logger.error("handleLoadScriptJobs apiError", e.responseBody?.errorMessage);
  }
}

// modified version of the loadTableData saga, specific to script jobs
function* loadScriptTableData(
  jobId: string,
  dataset: Record<string, any>,
  jobResultUrl?: string,
): unknown {
  logger.debug("loadScriptTableData loading table data for dataset", dataset);

  if (!jobResultUrl) {
    return;
  }

  let resetViewState = true;

  try {
    yield put(setExploreJobIdInProgress(jobId, dataset.datasetVersion));
    yield spawn(jobUpdateWatchers, jobId);
    yield put(
      updateViewState(EXPLORE_TABLE_ID, {
        isInProgress: true,
        isFailed: false,
        error: null,
      }),
    );

    yield race({
      dataLoaded: call(
        handleResumeRunDataset,
        dataset.datasetVersion,
        jobId,
        false /* forceReload */,
        jobResultUrl,
        true /* isRunOrPreview */,
      ),
      isLoadCanceled: take([CANCEL_TABLE_DATA_LOAD, TRANSFORM_PEEK_START]),
      locationChange: call(resetTableViewStateOnPageLeave),
    });
  } catch (e) {
    logger.error("loadScriptTableData error", e);

    if (!(e instanceof DataLoadError)) {
      throw e;
    }

    resetViewState = false; // to not hide an error

    const viewState = yield call(getViewStateFromAction, e.response);
    yield put(updateViewState(EXPLORE_TABLE_ID, viewState));
  } finally {
    if (resetViewState) {
      yield call(hideTableSpinner);
    }
  }
}
