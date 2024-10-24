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
import { put, select } from "redux-saga/effects";
import { updateContext } from "dremio-ui-common/sonar/scripts/endpoints/updateContext.js";
import { setQueryContext } from "#oss/actions/explore/view";
import { setRefsFromScript } from "#oss/actions/nessie/nessie";
import { addNotification } from "#oss/actions/notification";
import { getExploreState } from "#oss/selectors/explore";
import { MSG_CLEAR_DELAY_SEC } from "#oss/constants/Constants";

/**
 * @description Send a successful job's sessionId to /update_context to
 * update a script’s context and referencesList from job result
 */
export function* updateScriptContext(sessionId?: string): any {
  const activeScript =
    (yield select(getExploreState))?.view?.activeScript || {};

  try {
    if (!activeScript.permissions?.includes("MODIFY")) {
      return;
    }
    // sessionId can be null if the dataset was fetched using the "/preview" endpoint
    // e.g. run a new query -> click preview without changing anything
    if (activeScript.id && sessionId) {
      const updatedScript = yield updateContext(activeScript.id, sessionId);

      if (
        activeScript.context?.toString() !== updatedScript.context.toString()
      ) {
        yield put(
          setQueryContext({
            context: Immutable.fromJS(updatedScript.context),
          }),
        );
      }

      yield put(setRefsFromScript(updatedScript.referencesList));
    }
  } catch (e: any) {
    yield put(
      addNotification(
        e.responseBody.errorMessage,
        "error",
        MSG_CLEAR_DELAY_SEC,
      ),
    );
  }
}
