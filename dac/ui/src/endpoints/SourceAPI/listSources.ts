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

//@ts-ignore
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";
import { APIV2Call } from "@app/core/APICall";

export const getSourcesUrl = (include: boolean) =>
  new APIV2Call()
    .paths(`sources`)
    .param({ includeDatasetCount: include })
    .toString();

export const getResourceTree = (include: boolean): Promise<any> =>
  getApiContext()
    .fetch(getSourcesUrl(include))
    .then((res: any) => res.json());
