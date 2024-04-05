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
import { getSqlRunnerSession } from "./getSqlRunnerSession";
import { replaceSqlRunnerSession } from "./replaceSqlRunnerSession";

// temporarily copied from SqlRunnerSessionResource until moved to backend
const updateCurrentScript = (state: any): any => {
  // Don't change selected tab if it's still in the open tabs list
  if (state.scriptIds.includes(state.currentScriptId)) {
    return state;
  }

  // Return the last scriptId in the list
  return {
    ...state,
    currentScriptId: state.scriptIds.at(-1)!,
  };
};

export const closeSqlRunnerSessionTabs = async (
  scriptIds: string[],
): Promise<void> => {
  const session = await getSqlRunnerSession();

  const updatedSession = updateCurrentScript({
    ...session,
    scriptIds: session.scriptIds.filter(
      (scriptId) => !scriptIds.includes(scriptId),
    ),
  });

  return replaceSqlRunnerSession(updatedSession).then(() => undefined);
};
