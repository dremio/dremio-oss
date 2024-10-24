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
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";
import sessionStorageUtils from "@inject/utils/storageUtils/sessionStorageUtils";
import { store } from "#oss/store/store";
// @ts-ignore
import { resetPrivilegesState } from "@inject/actions/privileges";
import { isNotSoftware } from "dyn-load/utils/versionUtils";
import { resetAllSourcesView } from "#oss/actions/resources/sources";
import { SonarContentsResource } from "#oss/exports/resources/SonarContentsResource";
import { resetHomeContents } from "#oss/actions/home";
import { $SqlRunnerSession } from "dremio-ui-common/sonar/SqlRunnerSession/resources/SqlRunnerSessionResource.js";
import { ScriptsResource } from "dremio-ui-common/sonar/scripts/resources/ScriptsResource.js";

export const handleSonarProjectChange = (
  project: any,
  pushToProject?: () => any,
) => {
  (sessionStorageUtils as any)?.setProjectContext?.(project);
  (localStorageUtils as any)?.clearCurrentEngine?.();
  if (isNotSoftware()) {
    // reset redux state related to projects
    store.dispatch(resetPrivilegesState(["engineMapping"]));
  }

  // Reset the views, important when switching projects with ARS enabled to select the correct primary catalog
  store.dispatch(resetAllSourcesView());
  store.dispatch(resetHomeContents());
  SonarContentsResource.reset(); // Reset folder list when switching projects

  // Reset scripts resources when switching projects
  $SqlRunnerSession.resource.fetch();
  ScriptsResource.fetch();

  if (pushToProject) setImmediate(pushToProject); // Schedule callback/redirect
};
