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

import { RSAA } from "redux-api-middleware";
import { APIV2Call } from "@app/core/APICall";
import {
  RUN_TABLE_TRANSFORM_FAILURE,
  RUN_TABLE_TRANSFORM_START,
  RUN_TABLE_TRANSFORM_SUCCESS,
} from "@app/actions/explore/dataset/common";
import Immutable from "immutable";
import apiUtils from "@app/utils/apiUtils/apiUtils";
import readResponseAsJSON from "@app/utils/apiUtils/responseUtils";

export const newPostDatasetOperation = (
  href: string,
  dataset: Immutable.Map<string, any>,
  viewId: string,
  nextTable: any,
  body: any,
  sessionId: string
) => {
  const meta = {
    viewId,
    dataset,
    entity: dataset,
    nextTable,
    href,
  };

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        { type: RUN_TABLE_TRANSFORM_START, meta },
        readResponseAsJSON(RUN_TABLE_TRANSFORM_SUCCESS, meta),
        { type: RUN_TABLE_TRANSFORM_FAILURE, meta },
      ],
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...apiUtils.getJobDataNumbersAsStringsHeader(),
      },
      body: body && JSON.stringify(sessionId ? { ...body, sessionId } : body),
      endpoint: apiCall,
    },
  };
};
