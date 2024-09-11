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

import { Problem } from "@dremio/dremio-js/community";
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";

export type GetDownloadStatusParams = {
  jobId: string;
  downloadId: string;
};

const getDownloadStatusUrl = (params: GetDownloadStatusParams) =>
  getApiContext().createSonarUrl(
    `job/${params.jobId}/download/status?downloadJobId=${params.downloadId}`,
  );

export const getDownloadStatus = (
  params: GetDownloadStatusParams,
): Promise<{
  status: "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED";
  errors: Problem[];
}> =>
  getApiContext()
    .fetch(getDownloadStatusUrl(params))
    .then((res: Response) => res.json());
