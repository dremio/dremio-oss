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
import {
  all,
  put,
  call,
  takeEvery,
  select,
  take,
  fork,
} from "redux-saga/effects";
import invariant from "invariant";
import { cloneDeep } from "lodash";
import { intl } from "@app/utils/intl";
import {
  newUntitledSql,
  newUntitledSqlAndRun,
} from "actions/explore/dataset/new";
import {
  PERFORM_TRANSFORM,
  runTableTransform,
} from "actions/explore/dataset/transform";
import { resetViewState } from "actions/resources";
import { initializeExploreJobProgress } from "@app/actions/explore/dataset/data";
import { SQLEditor } from "@app/components/SQLEditor";
import { addNotification } from "@app/actions/notification";
import {
  PERFORM_TRANSFORM_AND_RUN,
  runDataset,
  transformAndRunDataset,
  RUN_DATASET_SQL,
} from "actions/explore/dataset/run";

import { EXPLORE_TABLE_ID } from "reducers/explore/view";

import {
  loadTableData,
  cancelDataLoad,
  loadDataset,
} from "@app/sagas/performLoadDataset";
import { listenToJobProgress } from "@app/sagas/performLoadDatasetNew";
import { transformHistoryCheck } from "sagas/transformHistoryCheck";
import { getExploreState, getExplorePageDataset } from "selectors/explore";
import { getExploreViewState } from "selectors/resources";
import { getLocation } from "@app/selectors/routing";
import { updateTransformData } from "@inject/actions/explore/dataset/updateLocation";
import {
  setQuerySelections,
  setQueryStatuses,
  setPreviousMultiSql,
  setSelectedSql,
  setIsMultiQueryRunning,
  setPreviousAndCurrentSql,
  setActionState,
} from "@app/actions/explore/view";

import apiUtils from "utils/apiUtils/apiUtils";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import { needsTransform } from "sagas/utils";
import {
  extractSelections,
  extractStatements,
} from "@app/utils/statements/statementParser";
import { showConfirmationDialog } from "actions/confirmation";

import { getReferenceListForTransform } from "@app/utils/nessieUtils";
import { hasReferencesChanged } from "@app/utils/datasetUtils";
import exploreUtils from "@app/utils/explore/exploreUtils";
import {
  transformThenNavigate,
  TransformFailedError,
  TransformCanceledError,
  TransformCanceledByLocationChangeError,
} from "./transformWatcher";
import { getNessieReferences } from "./nessie";
import { extractSql, toQueryRange } from "@app/utils/statements/statement";
import {
  handlePostNewQueryJobSuccess,
  newPerformTransformSingle,
} from "./performTransformNew";
import { PHYSICAL_DATASET_TYPES } from "@app/constants/datasetTypes";
import { SQL_DARK_THEME, SQL_LIGHT_THEME } from "@app/utils/sql-editor";
import Immutable from "immutable";
import { getLoggingContext } from "dremio-ui-common/contexts/LoggingContext.js";
import { EXPLORE_VIEW_ID } from "@app/reducers/explore/view";
import {
  clearExploreJobs,
  fetchJobDetails,
  fetchJobSummary,
  removeExploreJob,
} from "@app/actions/explore/exploreJobs";
import { replaceScript } from "dremio-ui-common/sonar/scripts/endpoints/replaceScript.js";
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";
import { selectActiveScript } from "@app/components/SQLScripts/sqlScriptsUtils";
import {
  deleteQuerySelectionsFromStorage,
  setQuerySelectionsInStorage,
} from "@app/sagas/utils/querySelections";

const logger = getLoggingContext().createLogger(
  "oss/sagas/performTransform.ts",
);

export default function* watchPerformTransform() {
  yield all([
    takeEvery(PERFORM_TRANSFORM, handlePerformTransform),
    takeEvery(PERFORM_TRANSFORM_AND_RUN, handlePerformTransformAndRun),
    fork(processRunDatasetSql),
  ]);
}

function* processRunDatasetSql() {
  while (true) {
    // eslint-disable-line no-constant-condition
    const action = yield take(RUN_DATASET_SQL);
    yield call(handleRunDatasetSql, action);
  }
}

export function* handlePerformTransformAndRun({ payload }) {
  yield* performTransform({ ...payload, isRun: true });
}

export function* handlePerformTransform({ payload }) {
  yield* performTransform(payload);
}

function* getActiveScriptId() {
  return (yield select(getExploreState))?.view?.activeScript?.id;
}

// Checks the passed in script ID agains the current activeScriptId in the explorePage.view.activeScript
// Returns the passed in tab (for the actions when originally dispatched) if they differ
function* getTabForActions(activeScriptId) {
  const id = yield getActiveScriptId();

  if (id !== activeScriptId) return activeScriptId;
  else return "";
}

// wrapper for multiple queries
export function* performTransform(payload) {
  try {
    const {
      dataset,
      runningSql,
      currentSql,
      selectedRange,
      callback,
      indexToModify,
      isSaveViewAs,
      isRun,
      viewId,
      forceDataLoad,
      queryContext,
      transformData,
      useOptimizedJobFlow,
      activeScriptId,
    } = payload;

    //
    yield put(
      setIsMultiQueryRunning({
        running: true,
        tabId: yield getTabForActions(activeScriptId),
      }),
    );

    let queryStatuses = [];
    const [rawQueries, selections] = yield call(getParsedSql, {
      dataset,
      currentSql,
      runningSql,
      selectedRange,
    });

    // we can trim end of queries without any issues because they will never contribute to invalid sql and will not mess with query ranges
    // we can't, however, trim query beginnings because that will desync server and client line & column info.
    const queries = rawQueries.map((q) => q.trimEnd());

    queries.forEach((query) =>
      queryStatuses.push({
        sqlStatement: query,
        cancelled: false /*!!curTabId*/,
      }),
    );

    // callback is passed in when clicking on actions
    if (!callback && indexToModify == undefined) {
      const curTabId = yield getTabForActions(activeScriptId);
      yield put(clearExploreJobs());
      yield put(
        setQueryStatuses({
          statuses: queryStatuses,
          tabId: curTabId,
        }),
      );
      yield put(setQuerySelections({ selections }));
      yield put(setPreviousMultiSql({ sql: currentSql }));

      const activeScript = yield select(selectActiveScript) || {};

      if (activeScript.id) {
        yield replaceScript(activeScript.id, {
          ...activeScript,
          jobIds: [], // reset the script jobs when submitting new jobs
        });

        ScriptsResource.fetch();

        deleteQuerySelectionsFromStorage(activeScript.id);
      }
    }

    let sessionId = "";
    let shouldBreak;
    for (let i = 0; i < queries.length; i++) {
      let exploreState = yield select(getExploreState);
      if (!exploreState || (isSaveViewAs && i)) {
        shouldBreak = true;
        break;
      }

      //Tabs: May need to remove this for background jobs
      const curTabId = yield getTabForActions(activeScriptId);
      const hasChangedTabsSinceRun = !!curTabId;
      if (hasChangedTabsSinceRun) {
        const newStatuses = (
          curTabId ? exploreState.tabViews[curTabId].queryStatuses : []
        ).map((status) => ({
          ...status,
          cancelled: true,
        }));
        const action = setQueryStatuses({
          statuses: newStatuses,
          tabId: curTabId,
        });
        logger.debug(
          `${activeScriptId}: Cancelling previous queryStatuses`,
          action,
        );
        yield put(action);
        break;
      }

      let curTab = yield getTabForActions(activeScriptId);
      const qStatuses = curTab
        ? exploreState?.tabViews?.[activeScriptId]?.queryStatuses
        : exploreState?.view?.queryStatuses;
      const preUpdatedQueryStatuses = exploreState ? qStatuses : queryStatuses;
      if (
        preUpdatedQueryStatuses?.length &&
        preUpdatedQueryStatuses[i].cancelled
      ) {
        continue;
      }

      const isLastQuery = i === queryStatuses.length - 1;

      let willProceed = true;

      const datasetType = dataset.get("datasetType");

      const isNotDataset =
        !dataset.get("datasetVersion") ||
        (!dataset.get("datasetType") && !dataset.get("sql"));

      const references = yield getNessieReferences();

      const sql = !isSaveViewAs
        ? queryStatuses[i].sqlStatement
        : runningSql || currentSql;

      const finalTransformData = yield call(
        getTransformData,
        dataset,
        sql || dataset.get("sql"),
        queryContext,
        transformData,
        references,
      );

      const location = yield select(getLocation);
      const { jobId } = location.query || {};

      // forces a job to be submitted to generate the required tmp dataset for saving
      // typically for cases where an unsubmitted query is being saved as a view
      const shouldTriggerJobForSaving =
        isNotDataset || // new query that hasn't been ran/previewed
        finalTransformData || // query that was executed and then modified
        (PHYSICAL_DATASET_TYPES.has(datasetType) && !jobId); // table that hasn't been ran/previewed

      // The process for running/previewing a new query is now handled differently.
      // submit job request -> listen to job progress -> fetch dataset data,
      // if job fails -> call summary API to fetch error details.
      // useOptimizedQueryFlow guarantees that this logic is only followed when clicking on run/preview,
      // trying to save a new or modified query that hasn't been ran/previewed yet, or creating
      // a new view from a PDS
      if (useOptimizedJobFlow || (isSaveViewAs && shouldTriggerJobForSaving)) {
        const [response, navigateOptions, newVersion] = yield call(
          newPerformTransformSingle,
          {
            ...payload,
            sessionId,
            sqlStatement: queryStatuses[i].sqlStatement,
            finalTransformData,
            references,
          },
        );

        exploreState = yield select(getExploreState);

        if (!exploreState) {
          shouldBreak = true;
          break;
        }

        // fetch queryStatuses from Redux
        curTab = yield getTabForActions(activeScriptId);
        const updatedQueryStatuses = cloneDeep(
          curTab
            ? exploreState?.tabViews?.[activeScriptId]?.queryStatuses
            : exploreState?.view?.queryStatuses,
        );

        // if queryStatuses does not exist, use the manually generated statuses
        const mostRecentStatuses = updatedQueryStatuses?.length
          ? updatedQueryStatuses
          : cloneDeep(queryStatuses);

        // handle successful job submission
        if (response?.payload) {
          let newDataset = undefined;
          let datasetPath = "";
          let datasetVersion = "";
          let jobId = "";
          let paginationUrl = "";

          // destructure response and update the queryStatuses object in Redux
          [
            newDataset,
            datasetPath,
            datasetVersion,
            jobId,
            paginationUrl,
            sessionId,
          ] = yield call(handlePostNewQueryJobSuccess, {
            response,
            newVersion,
            queryStatuses: mostRecentStatuses,
            curIndex: i,
            indexToModify,
            callback,
            tabId: yield getTabForActions(activeScriptId),
          });

          // start the job listener and track job progress in Redux
          willProceed = yield call(
            listenToJobProgress,
            newDataset,
            datasetVersion,
            jobId,
            paginationUrl,
            navigateOptions,
            isRun,
            datasetPath,
            callback,
            i,
            sessionId,
            viewId,
            yield getTabForActions(activeScriptId),
            selections[i],
          );
        }

        if (!callback && (!willProceed || (isLastQuery && !response.payload))) {
          exploreState = yield select(getExploreState);

          // fetch queryStatuses from Redux
          curTab = yield getTabForActions(activeScriptId);
          const updatedQueryStatuses = cloneDeep(
            curTab
              ? exploreState?.tabViews?.[activeScriptId]?.queryStatuses
              : exploreState?.view?.queryStatuses,
          );

          // if queryStatuses does not exist, use the manually generated statuses
          const mostRecentStatuses = updatedQueryStatuses?.length
            ? updatedQueryStatuses
            : cloneDeep(queryStatuses);

          for (let idx = i + 1; idx < mostRecentStatuses.length; idx++) {
            mostRecentStatuses[idx].cancelled = true;
          }

          yield put(
            setQueryStatuses({
              statuses: mostRecentStatuses,
              tabId: yield getTabForActions(activeScriptId),
            }),
          );
          break;
        }

        continue;
      }

      const isSavingPDS =
        isSaveViewAs && PHYSICAL_DATASET_TYPES.has(datasetType);

      // need to call the /preview endpoint when trying to save a PDS that wasn't ran/previewed first
      const [response, newVersion] = yield call(
        performTransformSingle,
        {
          ...payload,
          sessionId,
          forceDataLoad:
            isSavingPDS && preUpdatedQueryStatuses?.length === 0
              ? true
              : forceDataLoad,
        },
        queryStatuses[i],
      );

      exploreState = yield select(getExploreState);
      if (!exploreState) {
        shouldBreak = true;
        break;
      }
      curTab = yield getTabForActions(activeScriptId);
      const updatedQueryStatuses = cloneDeep(
        curTab
          ? exploreState?.tabViews?.[activeScriptId]?.queryStatuses
          : exploreState?.view?.queryStatuses,
      );
      const mostRecentStatuses = updatedQueryStatuses?.length
        ? updatedQueryStatuses
        : cloneDeep(queryStatuses);

      // handle success
      if (response && response.payload) {
        curTab = yield getTabForActions(activeScriptId);
        [sessionId] = yield call(handlePerformTransformSuccess, {
          response,
          queryStatuses: mostRecentStatuses,
          curIndex: i,
          indexToModify,
          callback,
          // Skips redirection
          tabId: curTab,
        });

        const resultDataset = apiUtils.getEntityFromResponse(
          "datasetUI",
          response,
        );

        curTab = yield getTabForActions(activeScriptId);

        //Try just breaking here;
        if (curTab) {
          shouldBreak = true;
          break;
        }

        if (!isSaveViewAs) {
          // we successfully loaded a dataset metadata. We need load table data for it
          yield call(loadTableData, resultDataset.get("datasetVersion"), isRun);
        }
      }

      // Skip if user has switched tabs
      curTab = yield getTabForActions(activeScriptId);
      if (curTab) {
        return;
      }

      // handle failure
      if (response && (!response.payload || response.error)) {
        willProceed = yield call(handlePerformTransformFailure, {
          response,
          queryStatuses: mostRecentStatuses,
          curIndex: i,
          newVersion,
          isSaveViewAs,
        });
      }

      if (shouldBreak) {
        return;
      }

      // if user cancels the other jobs, or its the last time
      if (!callback && (!willProceed || (isLastQuery && !response.payload))) {
        for (let idx = i + 1; idx < mostRecentStatuses.length; idx++) {
          mostRecentStatuses[idx].cancelled = true;
        }
        yield put(
          setQueryStatuses({
            statuses: mostRecentStatuses,
            tabId: yield getTabForActions(activeScriptId),
          }),
        );
        break;
      }
      // eslint-disable-next-line require-atomic-updates
      queryStatuses = mostRecentStatuses;
    }
  } catch (e) {
    // do nothing for api errors. view state handles them.
    if (handlePerformTransformError(e)) {
      throw e;
    }
  } finally {
    yield put(
      setIsMultiQueryRunning({
        running: false,
        tabId: yield getTabForActions(payload.activeScriptId),
      }),
    );
    yield put(setActionState({ actionState: null })); // tabId
  }
}

// handle the performTransform logic for a single query
export function* performTransformSingle(payload, query) {
  try {
    const {
      dataset,
      currentSql,
      queryContext,
      viewId,
      nextTable,
      isRun,
      transformData,
      runningSql,
      callback, // function(didTransform, dataset)
      forceDataLoad, // a boolean flag that forces a preview reload, when nothing is changed
      isSaveViewAs, // if saving a query, a multi-sql statement should not be parsed
      sessionId,
    } = payload;
    const { apiAction, navigateOptions, newVersion } = yield call(
      getFetchDatasetMetaAction,
      {
        dataset,
        currentSql: !isSaveViewAs
          ? query.sqlStatement
          : runningSql || currentSql,
        queryContext,
        viewId,
        nextTable,
        isRun,
        transformData,
        forceDataLoad,
        sessionId,
        noUpdate: true,
      },
    );

    let resultDataset = dataset;
    const didTransform = !!apiAction;
    let response;
    if (apiAction) {
      yield call(cancelDataLoad);
      yield put(
        initializeExploreJobProgress(
          isRun,
          resultDataset.get("datasetVersion"),
        ),
      );
      // response will be not empty. See transformThenNavigate
      response = yield call(
        transformThenNavigate,
        apiAction,
        viewId,
        navigateOptions,
      );
      if (!response || response.error) {
        throw new Error(
          "transformThenNavigate must return not empty response without error",
        );
      }

      resultDataset = apiUtils.getEntityFromResponse("datasetUI", response);
    }

    if (callback) {
      yield call(callback, didTransform, resultDataset);
    }

    return [response, newVersion];
  } catch (e) {
    return [e];
  }
}

export function* handlePerformTransformSuccess({
  response,
  queryStatuses,
  curIndex,
  indexToModify,
  callback,
  tabId,
}) {
  const mostRecentStatuses = queryStatuses;
  // Session ID in here
  const [sqlStatement, jobId, sessionId, version] =
    apiUtils.getFromResponse(response);

  const index = indexToModify != null ? indexToModify : curIndex;
  const statusToReplace = { ...mostRecentStatuses[index] }; // used for no-code flows
  mostRecentStatuses[index].jobId = jobId;
  mostRecentStatuses[index].version = version;
  mostRecentStatuses[index].sqlStatement = sqlStatement;

  queryStatuses = mostRecentStatuses;
  if (queryStatuses[curIndex].cancelled) {
    queryStatuses[curIndex].cancelled = false;
  }

  // store queryStatuses in redux store after a job succeeds
  // this results in the job query also having the jobId
  if (!callback) {
    yield put(setQueryStatuses({ statuses: queryStatuses, tabId }));

    if (indexToModify != null) {
      let newSql = "";

      for (const status of queryStatuses) {
        newSql += status.sqlStatement + ";\n";
      }

      const newSelections = extractSelections(newSql);

      // this gets the current script before it's updated by the below actions
      const activeScriptId = yield getActiveScriptId();
      const scriptBeforeRefresh =
        ScriptsResource.getResource().value?.find(
          (script) => script.id === activeScriptId,
        ) || {};

      yield put(removeExploreJob(statusToReplace.jobId));
      yield put(setPreviousAndCurrentSql({ sql: newSql, tabId }));
      yield put(setQuerySelections({ selections: newSelections, tabId }));

      if (scriptBeforeRefresh.id) {
        // wait for script refresh from setPreviousAndCurrentSql
        yield take("SCRIPT_SYNC_COMPLETED");

        // need to use the post-update script as the template
        const updatedScript = yield select(selectActiveScript) || {};

        const jobIds = [...scriptBeforeRefresh.jobIds];
        jobIds[indexToModify] = jobId;

        yield replaceScript(scriptBeforeRefresh.id, {
          ...updatedScript,
          jobIds,
        });

        ScriptsResource.fetch();

        setQuerySelectionsInStorage(scriptBeforeRefresh.id, newSelections);
      }
    }
  }

  return [sessionId];
}

const PARSE_FAILURE = "Failure parsing the query.";

export function* handlePerformTransformFailure({
  response,
  queryStatuses,
  curIndex,
  newVersion,
  isSaveViewAs,
}) {
  const mostRecentStatuses = queryStatuses;

  const error = response?.response?.payload?.response ?? {};
  const statusCode = response?.response?.payload?.status;
  const isGenericFailure = statusCode === 400;
  const isLastQuery = mostRecentStatuses.length - 1 === curIndex;
  let willProceed = true;
  if (isGenericFailure && !isSaveViewAs && !isLastQuery) {
    const isParseError = error?.errorMessage === PARSE_FAILURE;
    willProceed = yield call(
      showFailedJobDialog,
      curIndex,
      mostRecentStatuses[curIndex].sqlStatement,
      isParseError ? undefined : error?.errorMessage,
    );
  } else if (handlePerformTransformError(response) || isSaveViewAs) {
    willProceed = false;
    yield put(
      addNotification(apiUtils.getThrownErrorException(response), "error", 10),
    );
  }

  if (!isSaveViewAs) {
    mostRecentStatuses[curIndex].error = new Immutable.Map(response);
    const jobIdObj = error?.details?.jobId ?? {};
    if (error.code === "INVALID_QUERY" || !jobIdObj.id) {
      mostRecentStatuses[curIndex].cancelled = true;
    }

    const entity =
      response.response &&
      response.response.meta &&
      response.response.meta.entity;
    const errorVersion =
      entity && entity.get("tipVersion")
        ? entity.get("tipVersion")
        : newVersion;
    mostRecentStatuses[curIndex].jobId = jobIdObj.id;
    mostRecentStatuses[curIndex].version = errorVersion;
    yield put(setQueryStatuses({ statuses: mostRecentStatuses }));

    if (jobIdObj.id) {
      yield put(fetchJobDetails(jobIdObj.id));
      yield put(fetchJobSummary(jobIdObj.id, 0));
    }
  }

  return willProceed;
}

function handlePerformTransformError(e) {
  return (
    !apiUtils.isApiError(e) &&
    !(e instanceof TransformFailedError) &&
    !(e instanceof TransformCanceledError) &&
    !(e instanceof TransformCanceledByLocationChangeError)
  );
}

export function* handleRunDatasetSql({
  isPreview,
  selectedSql,
  useOptimizedJobFlow,
}) {
  const dataset = yield select(getExplorePageDataset);
  const exploreViewState = yield select(getExploreViewState);
  const exploreState = yield select(getExploreState);
  const viewId = exploreViewState.get("viewId");
  const currentSql = exploreState.view.currentSql;
  const runningSql = selectedSql.sql ? selectedSql.sql : currentSql;
  const queryContext = exploreState.view.queryContext;

  if (yield call(proceedWithDataLoad, dataset, queryContext, runningSql)) {
    const performTransformParam = {
      dataset,
      currentSql,
      runningSql,
      selectedRange: selectedSql.sql ? selectedSql.range : undefined,
      queryContext,
      viewId,
      useOptimizedJobFlow,
      activeScriptId: exploreState?.view?.activeScript?.id,
      // Session id should be passed in from script session state
      ...(isPreview ? { forceDataLoad: true } : { isRun: true }),
    };

    yield put(
      setSelectedSql({ sql: selectedSql.sql ? selectedSql.sql : undefined }),
    );

    yield call(performTransform, performTransformParam);
  }
}

/*
 * Helpers
 */

// do we need so many different endpoints? We should refactor our api to reduce the number
// Returns an action that should be triggered for case of Run/Preview
// export for tests
export function* getFetchDatasetMetaAction(props) {
  const {
    dataset,
    currentSql,
    queryContext,
    viewId,
    nextTable,
    isRun,
    transformData,
    forceDataLoad,
    sessionId = "",
    noUpdate = false,
  } = props;

  const references = yield getNessieReferences();
  const sql = currentSql || dataset.get("sql");
  const isNotDataset =
    !dataset.get("datasetVersion") ||
    (!dataset.get("datasetType") && !dataset.get("sql"));
  invariant(
    !queryContext || queryContext instanceof Immutable.List,
    "queryContext must be Immutable.List",
  );
  const finalTransformData = yield call(
    getTransformData,
    dataset,
    sql,
    queryContext,
    transformData,
    references,
  );
  let apiAction;
  let navigateOptions;
  let newVersion;

  if (isRun) {
    if (isNotDataset) {
      // dataset is not created. Create with sql and run.
      newVersion = exploreUtils.getNewDatasetVersion();
      apiAction = yield call(
        newUntitledSqlAndRun,
        sql,
        queryContext,
        viewId,
        references,
        sessionId,
        newVersion,
        noUpdate,
      );
      navigateOptions = { changePathname: true }; //changePathname to navigate to newUntitled
    } else if (finalTransformData) {
      updateTransformData(finalTransformData);

      // transform is requested. Transform and run.
      yield put(resetViewState(EXPLORE_TABLE_ID)); // Clear error from previous query run
      apiAction = yield call(
        transformAndRunDataset,
        dataset,
        finalTransformData,
        viewId,
        sessionId,
      );
    } else {
      // just run
      apiAction = yield call(runDataset, dataset, viewId, sessionId);
      navigateOptions = { replaceNav: true, preserveTip: true };
    }
  } else {
    // VBesschetnov original code has the following flow:
    // ----------------------------------------------------------------------------
    // if (!dataset.get('datasetVersion')) { ... }
    // if (finalTransformData) { ... }
    // ----------------------------------------------------------------------------
    // I could not imagine a case when dataset without version could be transformed. It looks more
    // like creation of a new VDS. So I would refactor the code with an assumption that we will
    // never face got a situation when !dataset.get('datasetVersion') && !!finalTransformData
    // The one possible case that come in my mind is when we navigate to existent VDS by direct link
    // and a user will try to alter a query, until we receive a current version of VDS. But it is
    // unlikely as we block the UI until VDS info would be loaded.
    // I will throw an exception in case if the assumption would be invalid
    if (!dataset.get("datasetVersion") && finalTransformData) {
      throw new Error(
        "this case is not supported. Code is built in assumption, that this case will never happen. We need investigate this case",
      );
    }
    // ----------------------------------------------------------------------------

    if (isNotDataset) {
      newVersion = exploreUtils.getNewDatasetVersion();
      apiAction = yield call(
        newUntitledSql,
        sql,
        queryContext && queryContext.toJS(),
        viewId,
        references,
        sessionId,
        newVersion,
        noUpdate,
      );
      navigateOptions = { changePathname: true }; //changePathname to navigate to newUntitled
    } else if (finalTransformData) {
      apiAction = yield call(
        runTableTransform,
        dataset,
        finalTransformData,
        viewId,
        nextTable,
        sessionId,
      );
    } else {
      // preview existing dataset
      if (forceDataLoad) {
        apiAction = yield call(
          loadDataset,
          dataset,
          viewId,
          forceDataLoad,
          sessionId,
          true,
        );
      }
      navigateOptions = { replaceNav: true, preserveTip: true };
    }
  }

  // api action could be empty only for this case, when there is an existent dataset without changes
  // and data reload was not enforced
  if (
    !apiAction &&
    !(dataset.get("datasetVersion") && !finalTransformData && !forceDataLoad)
  ) {
    throw new Error("we should not appear here");
  }
  return {
    apiAction,
    navigateOptions,
    newVersion,
  };
}

/**
 * Returns updateSQL transforms if there isn't already transformData, and dataset is not new.
 */
export const getTransformData = (
  dataset,
  sql,
  queryContext,
  transformData,
  references,
) => {
  if (!dataset) return;

  if (dataset.get("isNewQuery") || transformData) {
    return transformData;
  }

  const savedSql = dataset.get("sql") || "";
  const savedContext = dataset.get("context") || Immutable.List();
  const savedReferences = (dataset.get("references") || Immutable.Map()).toJS();
  if (
    (sql !== null && savedSql !== sql) ||
    !savedContext.equals(queryContext || Immutable.List()) ||
    hasReferencesChanged(references, savedReferences)
  ) {
    return {
      type: "updateSQL",
      sql,
      sqlContextList: queryContext && queryContext.toJS(),
      referencesList: getReferenceListForTransform(references),
    };
  }
};

/**
 * Checks if data load should be done despite the possible data loses
 *
 *  - When a user changes a sql or query that will result with a new dataset version.
 * But if a user changed some previous version, he could loose some future history versions.
 * In that case we show a confirmation to the user and he could cancel the transformation.
 * We return {@see false} form this saga in that case, otherwise {@see true}.
 *
 *  - If user does change sql or context, he may want to just load data for a current
 * version of a dataset. In that case the method returns {@see true}.
 *
 * @exports only for tests
 * @param {Immutable.Map} dataset
 * @param {Immutable.List} queryContext
 * @param {string} currentSql
 * @returns {boolean} true if we should proceed with transformation
 */
export function* proceedWithDataLoad(dataset, queryContext, currentSql) {
  // check that we have a transform case, i.e. sql or context is changed
  const sqlOrContextChanged = needsTransform(dataset, queryContext, currentSql);

  if (sqlOrContextChanged) {
    return yield call(transformHistoryCheck, dataset);
  }
  // nothing is changed. We should allow to load data
  return true;
}

export function* showFailedJobDialog(i, sql, errorMessage) {
  const options = {
    selectOnLineNumbers: false,
    disableLayerHinting: true,
    wordWrap: "on",
    overviewRulerBorder: false,
    lineNumbers: "on",
    readOnly: true,
    minimap: {
      enabled: false,
    },
  };

  // monaco-editor does not support having multiple editors on the same page with different themes
  // current solution is to apply the same theme to the popup as the main editor
  // refer to: https://github.com/microsoft/monaco-editor/issues/338
  let action;
  const isContrast = localStorageUtils.getSqlThemeContrast();

  const mainMessage = errorMessage ? (
    <>
      {errorMessage} -{" "}
      <b>{`${intl.formatMessage({
        id: "NewQuery.LowercaseQuery",
      })} ${i + 1}`}</b>
    </>
  ) : (
    intl.formatMessage(
      { id: "NewQuery.FailedMessageState" },
      {
        queryIndex: (
          <b>{`${intl.formatMessage({
            id: "NewQuery.LowercaseQuery",
          })} ${i + 1}`}</b>
        ),
      },
    )
  );

  const confirmPromise = new Promise((resolve) => {
    action = showConfirmationDialog({
      title: intl.formatMessage({ id: "NewQuery.FailedTitle" }),
      confirmText: intl.formatMessage({ id: "Common.Proceed" }),
      cancelText: intl.formatMessage({ id: "NewQuery.StopExecution" }),
      text: (
        <div className="failedJobDialog__body">
          <div className="failedJobDialog__message">{mainMessage}</div>
          <div className="failedJobDialog__editor">
            <SQLEditor
              readOnly
              value={sql.trim()}
              fitHeightToContent
              maxHeight={190}
              contextMenu={false}
              customTheme
              theme={isContrast ? SQL_DARK_THEME : SQL_LIGHT_THEME}
              background={isContrast ? "#333333" : "#FFFFFF"}
              selectionBackground={isContrast ? "#304D6D" : "#B5D5FB"}
              inactiveSelectionBackground={isContrast ? "#505862" : "#c6e9ef"}
              customOptions={{ ...options }}
            />
          </div>
          <div className="failedJobDialog__message">
            {intl.formatMessage({ id: "NewQuery.FailedMessageConfirm" })}
          </div>
        </div>
      ),
      size: "small",
      confirm: () => resolve(true),
      cancel: () => resolve(false),
      closeButtonType: "XBig",
      className: "failedJobDialog --newModalStyles",
      headerIcon: (
        <dremio-icon
          name="interface/warning"
          alt="Warning"
          class="failedJobDialog__icon"
        />
      ),
    });
  });

  yield put(action);
  return yield confirmPromise;
}

export function getParsedSql({
  dataset,
  currentSql,
  runningSql,
  selectedRange, // will be undefined if nothing is highlighted
}) {
  const datasetSql = dataset.get("sql");
  const sql = runningSql || currentSql || datasetSql;
  const statements = extractStatements(sql, {
    column: selectedRange?.startColumn,
    line: selectedRange?.startLineNumber,
  });

  return [
    statements.map((s) => extractSql(sql, s)),
    statements.map(toQueryRange),
  ];
}

export function* doJobFetch({
  queryStatus,
  activeScriptId,
  index,
  sessionId = "",
}) {
  try {
    let newDataset = undefined;
    let datasetPath = ["tmp", "UNTITLED"];
    let datasetVersion = queryStatus.version;
    let jobId = queryStatus.jobId;
    let paginationUrl = queryStatus.paginationUrl;

    yield put(
      setIsMultiQueryRunning({
        running: true,
        tabId: yield getTabForActions(activeScriptId),
      }),
    );

    yield call(
      listenToJobProgress,
      newDataset,
      datasetVersion,
      jobId,
      paginationUrl,
      {}, // navigateOptions
      true, // isRun, Only used for tracking event, move the tracking event somewhere else
      datasetPath,
      undefined, // callback, used for save as view
      index,
      sessionId,
      EXPLORE_VIEW_ID,
      yield getTabForActions(activeScriptId), //Send appropriate tabId if user has switched tabs again
    );
  } finally {
    yield put(
      setIsMultiQueryRunning({
        running: false,
        tabId: yield getTabForActions(activeScriptId),
      }),
    );
  }
}
