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
import * as commonPaths from "../paths/common";
import { getSonarContext } from "../contexts/SonarContext";

export function getProjectBase(projectId?: string) {
  return commonPaths.projectBase.link({
    projectId: projectId
      ? projectId
      : getSonarContext()?.getSelectedProjectId?.(),
  });
}

/**
 *
 * @param {string} url - URL to remove the sonar projectId prefix from
 * @param {Object} opts Options
 * @param {string} opts.projectId - Optional projectId, if not provided then the current sonarContext projectId will be used
 * @returns {string} URL without the sonar projectId prefix, or the same URL if the projectBase is "/"
 */
export const rmProjectBase = (
  urlArg: string | null | undefined,
  opts?: { projectId?: string },
) => {
  const url = urlArg || "";
  const projBase = getProjectBase(opts?.projectId);
  if (projBase !== "/" && url.startsWith(projBase)) {
    return url.replace(projBase, "");
  } else {
    return url;
  }
};

/**
 *
 * @param {string} url - URL to add the sonar projectId prefix to
 * @param {Object} opts Options
 * @param {string} opts.projectId - Optional projectId, if not provided then the current sonarContext projectId will be used
 * @returns {string} URL without the sonar projectId prefix, or the same URL if the projectBase is "/"
 */
export const addProjectBase = (
  urlArg: string | null | undefined,
  opts?: { projectId?: string },
) => {
  if (urlArg == null) return "";
  const url = urlArg || "";
  const projBase = getProjectBase(opts?.projectId);
  if (projBase !== "/") {
    return projBase + url;
  } else {
    return url;
  }
};
