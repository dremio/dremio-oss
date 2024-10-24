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

import { takeEvery, takeLatest } from "redux-saga/effects";
import {
  MODIFY_CURRENT_SQL,
  SET_PREVIOUS_AND_CURRENT_SQL,
  SET_QUERY_CONTEXT,
} from "#oss/actions/explore/view";
import { store } from "#oss/store/store";
import { selectActiveScript } from "#oss/components/SQLScripts/sqlScriptsUtils";
import { getSupportFlag } from "#oss/exports/endpoints/SupportFlags/getSupportFlag";
import { SQLRUNNER_TABS_UI } from "#oss/exports/endpoints/SupportFlags/supportFlagConstants";
import { throttle, isEqual } from "lodash";

export const scriptReplaceSideEffect = async (script) => {
  if (!(await getSupportFlag(SQLRUNNER_TABS_UI)).value) {
    return;
  }

  store.dispatch({
    type: "REPLACE_SCRIPT_CONTENTS",
    scriptId: script.id,
    script,
  });
};

const throttledScriptReplace = throttle(scriptReplaceSideEffect, 2000, {
  leading: false,
  trailing: true,
});

export default function* currentSqlSideEffects() {
  /**
   * Whenever the current SQL is modified and the multi-tab feature is enabled,
   * immediately sync the updated script contents to the store so that the user
   * doesn't have to explicitly save the script changes anymore.
   */
  yield takeEvery(MODIFY_CURRENT_SQL, (action) => {
    const activeScript = selectActiveScript(store.getState());

    if (!activeScript?.id) {
      return;
    }

    if (activeScript.content === action.sql) {
      return;
    }

    return throttledScriptReplace({ ...activeScript, content: action.sql });
  });
  yield takeLatest(SET_PREVIOUS_AND_CURRENT_SQL, (action) => {
    const activeScript = selectActiveScript(store.getState());

    if (!activeScript?.id) {
      return;
    }

    if (activeScript.content === action.sql) {
      return;
    }

    return scriptReplaceSideEffect({ ...activeScript, content: action.sql });
  });
  yield takeEvery(SET_QUERY_CONTEXT, (action) => {
    const activeScript = selectActiveScript(store.getState());

    if (!activeScript?.id) {
      return;
    }
    if (isEqual(activeScript.context, action.context.toJS())) {
      return;
    }
    return scriptReplaceSideEffect({
      ...activeScript,
      context: action.context,
    });
  });
}
