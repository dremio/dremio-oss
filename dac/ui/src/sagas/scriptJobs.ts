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
import { cloneDeep } from "lodash";
import {
  all,
  call,
  put,
  race,
  select,
  spawn,
  take,
  takeEvery,
  throttle,
} from "redux-saga/effects";
// @ts-expect-error ui-common type missing
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { getLoggingContext } from "dremio-ui-common/contexts/LoggingContext.js";
import { NotFoundError } from "dremio-ui-common/errors/NotFoundError";
// @ts-expect-error ui-common type missing
import { replaceScript } from "dremio-ui-common/sonar/scripts/endpoints/replaceScript.js";
// @ts-expect-error ui-common type missing
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";
// @ts-expect-error ui-common type missing
import type { Script } from "dremio-ui-common/sonar/scripts/Script.type.js";

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
  setExploreTableLoading,
  setQuerySelections,
  setQueryStatuses,
  setQueryTabNumber,
} from "@app/actions/explore/view";
import { updateViewState } from "@app/actions/resources";
import {
  LOAD_JOB_RESULTS,
  LOAD_JOB_TABS,
} from "@app/actions/resources/scripts";
import { EXPLORE_TABLE_ID, EXPLORE_VIEW_ID } from "@app/reducers/explore/view";
import {
  CANCEL_TABLE_DATA_LOAD,
  hideTableSpinner,
  loadDataset,
  resetTableViewStateOnPageLeave,
} from "@app/sagas/performLoadDataset";
import {
  handleResumeRunDataset,
  jobUpdateWatchers,
} from "@app/sagas/runDataset";
import { transformThenNavigate } from "@app/sagas/transformWatcher";
import { getTabForActions } from "@app/sagas/utils";
import { getQuerySelectionsFromStorage } from "@app/sagas/utils/querySelections";
import { getTableDataRaw } from "@app/selectors/explore";
import { getAllJobDetails } from "@app/selectors/exploreJobs";
import { getQueryStatuses } from "@app/selectors/jobs";
import { getDatasetPreview } from "@app/exports/endpoints/Datasets/getDatasetPreview";
import { JobDetails } from "@app/exports/types/JobDetails.type";
import { JobSummary } from "@app/exports/types/JobSummary.type";
import apiUtils from "@app/utils/apiUtils/apiUtils";
import { QueryRange } from "@app/utils/statements/statement";

const logger = getLoggingContext().createLogger("sagas/scriptJobs");

export default function* scriptJobs() {
  yield throttle(1000, LOAD_JOB_TABS, handleLoadJobTabs);
  yield takeEvery(LOAD_JOB_RESULTS, handleLoadJobResults);
}

/**
 * @description Prepares query statuses and tabs for lazy-load when opening a script
 */
function* handleLoadJobTabs({
  script,
}: {
  type: typeof LOAD_JOB_TABS;
  script: Script;
}): unknown {
  const {
    id,
    jobIds,
    jobResultUrls,
  }: { id: string; jobIds: string[]; jobResultUrls: string[] } = script;

  // prevents the saga from executing twice on the same script
  const startingStatuses: unknown[] = yield select(getQueryStatuses);

  if (
    startingStatuses.length ||
    !jobIds.length ||
    !jobResultUrls.length ||
    jobIds.length !== jobResultUrls.length
  ) {
    logger.debug("handleLoadJobTabs cancelling tab load");
    return;
  }

  logger.debug("handleLoadJobTabs preparing query tabs");

  // prevents jobs from appearing in other tabs when switching scripts
  yield put(clearExploreJobs());

  yield put(setExploreTableLoading({ isLoading: true }));

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
      tabId: yield getTabForActions(id),
    }),
  );

  try {
    const summaryTabId: string = yield getTabForActions(id);

    const jobSummaries: JobSummary[] = yield all(
      yield all(
        jobIds.map((jobId) =>
          put(
            // @ts-expect-error return type of fetchJobSummary is not an action
            fetchJobSummary({
              jobId,
              maxSqlLength: 0,
              tabId: summaryTabId,
            }),
          ),
        ),
      ),
    );

    const successfulJobIds: string[] = [];
    jobSummaries.forEach(
      (summary) =>
        summary.state === "COMPLETED" && successfulJobIds.push(summary.id),
    );

    if (!successfulJobIds.length) {
      logger.debug("handleLoadJobTabs no successful jobs found");
      yield put(clearExploreJobs());
      return;
    }

    const areResultsAvailable = yield call(
      checkForExistingResults,
      jobResultUrls.filter((resultUrl) =>
        successfulJobIds.includes(resultUrl.split("/")[2]),
      ),
    );

    if (!areResultsAvailable) {
      yield put(clearExploreJobs());
      return;
    }

    const detailsTabId: string = yield getTabForActions(id);

    // needed to fill in the jobs table
    yield all(
      yield all(
        jobIds.map((jobId) =>
          put(
            // @ts-expect-error return type of fetchJobDetails is not an action
            fetchJobDetails({ jobId, tabId: detailsTabId }),
          ),
        ),
      ),
    );

    const queryStatuses: Record<string, unknown>[] = [];

    jobSummaries.forEach((jobSummary) => {
      const { id, description, datasetVersion } = jobSummary;

      if (["FAILED", "CANCELED"].includes(jobSummary.state)) {
        const { cancellationInfo, failureInfo } = jobSummary;
        const error = failureInfo.errors.at(0);

        queryStatuses.push({
          cancelled: !!cancellationInfo,
          jobId: id,
          sqlStatement: description,
          version: datasetVersion,
          error: Immutable.fromJS(cancellationInfo ?? error ?? failureInfo),
        });
      } else {
        queryStatuses.push({
          cancelled: false,
          jobId: id,
          sqlStatement: description,
          version: datasetVersion,
          lazyLoad: true,
        });
      }
    });

    // initialize query statuses to display query tabs
    yield put(
      setQueryTabNumber({ tabNumber: 0, tabId: yield getTabForActions(id) }),
    );
    yield put(
      setQueryStatuses({
        statuses: [...queryStatuses],
        tabId: yield getTabForActions(id),
      }),
    );

    logger.debug("handleLoadJobTabs done preparing tabs");
  } catch (e: any) {
    logger.error("handleLoadJobTabs", e.res);

    // if jobs no longer exists in the store, remove the jobIds from the script
    if (e instanceof NotFoundError) {
      try {
        yield replaceScript(script.id, {
          ...script,
          jobIds: [],
        });

        ScriptsResource.fetch();
      } catch {
        logger.debug("handleLoadJobTabs removing stale jobs");
      }
    }
  } finally {
    yield put(setExploreTableLoading({ isLoading: false }));
  }
}

/**
 * @description Samples given jobs and checks if the results are available
 */
function* checkForExistingResults(jobResultUrls: string[]) {
  try {
    for (const resultUrl of jobResultUrls) {
      const jobDataUrl = getApiContext().createSonarUrl(
        `${resultUrl.slice(1)}?offset=0&limit=100`,
      );

      // check if the results exist
      yield getApiContext()
        .fetch(jobDataUrl)
        .then((res: Response) => res.json());
    }

    return true;
  } catch (e: any) {
    // nothing should be shown to the user if jobs or results have expired
    // need to specify the error messages here since the API can return 400 for other reasons
    if (
      ["Please rerun the query", "output doesn't exist"].some((errMessage) =>
        e.responseBody?.errorMessage?.endsWith(errMessage),
      )
    ) {
      logger.debug(
        "checkForExistingResults results not found",
        e.responseBody.errorMessage,
      );
      return false;
    }

    return true;
  }
}

/**
 * @description Fetches a given job's results when clicking on a query tab
 */
function* handleLoadJobResults({
  script,
  jobStatus,
}: {
  type: typeof LOAD_JOB_RESULTS;
  script: Script;
  jobStatus: {
    cancelled: boolean;
    jobId: string;
    sqlStatement: string;
    version: string;
    error?: Immutable.Map<string, any>;
    lazyLoad?: boolean;
  };
}): unknown {
  const { jobResultUrls }: { jobResultUrls: string[] } = script;

  const tableRows: Immutable.List<any> | undefined = (yield select(
    getTableDataRaw,
    jobStatus.version,
  ))?.get("rows");

  if (tableRows || jobStatus.cancelled || jobStatus.error) {
    logger.debug("handleLoadJobResults skipping results load");
    return;
  }

  logger.debug("handleLoadJobResults loading job results for", jobStatus.jobId);

  const jobDetails: JobDetails = (yield select(getAllJobDetails))[
    jobStatus.jobId
  ];

  try {
    const viewMetadata = yield getDatasetPreview(
      jobDetails.datasetPaths,
      jobDetails.datasetVersion,
      false,
    );

    // taken from handlePerformLoadDataset saga
    const apiAction = yield call(
      loadDataset,
      Immutable.fromJS({ ...viewMetadata.dataset, jobId: jobStatus.jobId }),
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

    if (nextFullDataset?.get("error")) {
      yield put(
        updateViewState(EXPLORE_VIEW_ID, {
          isFailed: true,
          error: {
            message: nextFullDataset.getIn(["error", "errorMessage"]),
          },
        }),
      );
    } else {
      yield put(initializeExploreJobProgress(false)); // sets if the initial dataset load is a `Run`
      yield call(
        loadScriptTableData,
        jobStatus.jobId,
        viewMetadata.dataset,
        jobResultUrls.find((jobResultUrl) =>
          jobResultUrl.includes(jobStatus.jobId),
        ),
      );
    }

    // deep copy needed to avoid modifying Redux by reference
    const statuses: any[] = cloneDeep(yield select(getQueryStatuses));
    const currentStatusIndex = statuses.findIndex(
      (status) => status.jobId === jobStatus.jobId,
    );
    statuses[currentStatusIndex].lazyLoad = false;
    yield put(setQueryStatuses({ statuses }));

    logger.debug("handleLoadJobResults done loading job results");
  } catch (e: any) {
    logger.error("handleLoadJobResults", e.res);

    // if dataset doesn't exist save the error in querystatuses
    if (e instanceof NotFoundError) {
      const statuses: any[] = cloneDeep(yield select(getQueryStatuses));
      const currentStatusIndex = statuses.findIndex(
        (status) => status.jobId === jobStatus.jobId,
      );
      statuses[currentStatusIndex].error = Immutable.fromJS({
        message: e.responseBody.errorMessage,
      });
      statuses[currentStatusIndex].lazyLoad = false;
      yield put(setQueryStatuses({ statuses }));
    }
  }
}

// modified version of the loadTableData saga, specific to script jobs
function* loadScriptTableData(
  jobId: string,
  dataset: Record<string, any>,
  jobResultUrl?: string,
): unknown {
  logger.debug("loadScriptTableData loading table data");

  if (!jobResultUrl) {
    return;
  }

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
    logger.error("loadScriptTableData", e);

    // DataLoadError is handled in handleResumeRunDataset, can just throw here
    throw e;
  } finally {
    yield call(hideTableSpinner);
  }
}
