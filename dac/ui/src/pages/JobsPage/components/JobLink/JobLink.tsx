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

import { Link } from "react-router";
// @ts-ignore
import * as jobPaths from "dremio-ui-common/paths/jobs.js";
// @ts-ignore
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

export const JobLink = ({
  job,
  reflectionId,
  newTab,
}: {
  job: any;
  reflectionId?: string;
  newTab?: boolean;
}) => {
  const jobAttempts = job?.totalAttempts;
  return (
    <Link
      target={newTab ? "blank" : undefined}
      id={job.id}
      to={`${jobPaths.job.link({
        // @ts-ignore
        projectId: getSonarContext().getSelectedProjectId?.(),
        jobId: job.id,
      })}${jobAttempts ? `?attempts=${jobAttempts || 1}` : ""}${
        reflectionId ? `#${reflectionId}` : ""
      }`}
    >
      {job.id}
    </Link>
  );
};
