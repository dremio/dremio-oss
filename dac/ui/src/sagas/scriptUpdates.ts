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

import { all, fork, select, takeLatest, put, take } from "redux-saga/effects";
import {
  POLL_SCRIPT_JOBS,
  REFRESH_SCRIPTS_RESOURCE,
  REPLACE_SCRIPT_CONTENTS,
} from "@app/actions/resources/scripts";
import { replaceScript } from "dremio-ui-common/sonar/scripts/endpoints/replaceScript.js";
import { store } from "@app/store/store";
import { getImmutableJobList, getQueryStatuses } from "@app/selectors/jobs";
import { getLoggingContext } from "dremio-ui-common/contexts/LoggingContext.js";
import { doJobFetch } from "./performTransform";
import { setQueryStatuses } from "@app/actions/explore/view";
import { resetViewState } from "@app/actions/resources";
import { EXPLORE_VIEW_ID } from "@app/reducers/explore/view";
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";
import { isEqual } from "lodash";
import { NotFoundError } from "dremio-ui-common/errors/NotFoundError";
import { EXPLORE_PAGE_LOCATION_CHANGED } from "@app/actions/explore/dataset/data";
import { getImmutableTable } from "@app/selectors/explore";

const logger = getLoggingContext().createLogger("sagas/scriptUpdates");

const syncUpdatedScript = async (action: any) => {
  const originalScriptContents = ScriptsResource.getResource().value?.find(
    (script) => script.id === action.scriptId
  );
  if (
    !originalScriptContents ||
    !originalScriptContents.permissions.includes("MODIFY")
  ) {
    return;
  }
  if (
    originalScriptContents &&
    originalScriptContents.content === action.script.content &&
    isEqual(originalScriptContents.context, action.script.context)
  ) {
    return;
  }

  logger.info("Syncing script changes");
  store.dispatch({ type: "SCRIPT_SYNC_STARTED", id: action.scriptId });
  try {
    const result = await replaceScript(action.scriptId, action.script);
    ScriptsResource.fetch();
    store.dispatch({ type: "SCRIPT_SYNC_COMPLETED", id: action.scriptId });
    return result;
  } catch (e) {
    const is404 = e instanceof NotFoundError;
    if (is404) {
      window.sqlUtils.handleDeletedScript();
    }
  }
};

function* pollIncompleteJobs(action: any): any {
  logger.debug("pollIncompleteJobs  : ", action);
  // Resets SQL runner state when switching tabs (i.e. if error on query then switching tab shouldnt have sql runner greyed out)
  yield put(resetViewState(EXPLORE_VIEW_ID));

  if (!action.queryStatuses) {
    logger.debug("pollIncompleteJobs waiting for locationChange");
    const locationChanged = yield take(EXPLORE_PAGE_LOCATION_CHANGED); // Wait for location change
    logger.debug("pollIncompleteJobs locationChanged: ", locationChanged);
  }

  let curQueryStatuses = yield select(getQueryStatuses);
  if (action.queryStatuses) {
    yield put(setQueryStatuses({ statuses: action.queryStatuses }));
    curQueryStatuses = action.queryStatuses;
  }

  if (curQueryStatuses.length === 0) {
    logger.debug("pollIncompleteJobs no query statuses, nothing to do.");
    return;
  }

  const storeState = store.getState();
  const jobsList = getImmutableJobList(storeState);
  const statuses = curQueryStatuses.filter((status: any) => {
    const tableData = getImmutableTable(storeState, status.version);
    const currentJob = jobsList.find(
      (job: any) => status.jobId && job.get("id") === status.jobId
    );
    const hasRows = status.version && !!tableData?.get("rows");
    if (!hasRows) {
      logger.debug("hasRows: ", { status, hasRows, tableData });
    }
    // May later remove manually cancelled jobs, note: switching tabs sets cancelled=true right now (performTransform)
    return (
      !!status.jobId &&
      (!currentJob || !currentJob.get("isComplete") || !hasRows)
    );
  });

  //Clear any jobs without a jobId
  const updateAction = setQueryStatuses({
    statuses: curQueryStatuses.map((status: any) => {
      return {
        ...status,
        cancelled:
          status.cancelled ||
          !status.jobId ||
          // This will prevent the error/failed query dialog from showing up when switching tabs
          status.error,
      };
    }),
  });
  logger.debug(
    "Switched tabs, clearing unsubmitted queryStatuses: ",
    updateAction
  );
  yield put(updateAction);

  if (statuses.length > 0) {
    logger.debug("Switched tabs, restoring job listeners for jobs:", statuses);
  } else {
    logger.debug("Switched tabs, no jobs to listen for.");
  }

  yield all(
    statuses.map((queryStatus: any, index: number) => {
      return fork(doJobFetch, {
        queryStatus,
        activeScriptId: action.script.id,
        // Tabs: Need to store sessionId in the script
        sessionId: action.script.sessionId || "",
        index,
      });
    })
  );
}

function refreshScriptsResource() {
  ScriptsResource.fetch();
}

export default function* scriptUpdates() {
  yield takeLatest(REPLACE_SCRIPT_CONTENTS, syncUpdatedScript);
  yield takeLatest(POLL_SCRIPT_JOBS, pollIncompleteJobs);
  yield takeLatest(REFRESH_SCRIPTS_RESOURCE, refreshScriptsResource);
}
