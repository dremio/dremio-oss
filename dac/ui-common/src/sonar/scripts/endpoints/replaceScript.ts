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
import type { paths, components } from "../../../../apiTypes/scripts";
import { getApiContext } from "../../../contexts/ApiContext";
import { scriptsLogger } from "../scriptsLogger";

export const replaceScriptUrl = (scriptId: string) =>
  getApiContext().createSonarUrl(`scripts/${scriptId}`);

type path = paths["/scripts/{id}"]["patch"];

export type UpdatedScript = path["requestBody"]["content"]["application/json"];
export type Script = components["schemas"]["Script"];

export const replaceScript = async (
  scriptId: string,
  body: UpdatedScript,
): Promise<path["responses"]["200"]["content"]["application/json"]> => {
  scriptsLogger.debug(
    `Syncing updating script ${body.name} with contents: ${body.content}`,
  );
  return getApiContext()
    .fetch(replaceScriptUrl(scriptId), {
      method: "put",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
    .then((res) => res.json());
};
