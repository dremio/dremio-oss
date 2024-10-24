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
import apiUtils from "#oss/utils/apiUtils/apiUtils";
import { APIV2Call } from "#oss/core/APICall";
import { clearCachedSupportFlag } from "#oss/exports/endpoints/SupportFlags/getSupportFlag";

// SUPPORT FLAG CONSTANTS
export const SUPPORT_FLAG_START = "SUPPORT_FLAG_START";
export const SUPPORT_FLAG_SUCCESS = "SUPPORT_FLAG_SUCCESS";
export const SUPPORT_FLAG_FAILURE = "SUPPORT_FLAG_FAILURE";
export const SAVE_SUPPORT_FLAG_START = "SAVE_SUPPORT_FLAG_START";
export const SAVE_SUPPORT_FLAG_SUCCESS = "SAVE_SUPPORT_FLAG_SUCCESS";
export const SAVE_SUPPORT_FLAG_FAILURE = "SAVE_SUPPORT_FLAG_FAILURE";

export const fetchSupportFlagsSuccess = (payload, meta) => ({
  type: SUPPORT_FLAG_SUCCESS,
  payload,
  meta,
});

const fetchSupportFlagsFailure = (payload) => ({
  type: SUPPORT_FLAG_FAILURE,
  payload,
  meta: {
    notification: false,
  },
  error: true,
});

export const fetchSupportFlags = (supportKey) => (dispatch) => {
  const fetchOptions = {
    customOptions: {
      includeProjectId: true,
    },
  };
  return apiUtils
    .fetch(`/settings/${supportKey}`, fetchOptions, 2)
    .then((response) => response.json())
    .then((response) => dispatch(fetchSupportFlagsSuccess(response)))
    .catch((error) => dispatch(fetchSupportFlagsFailure(error)));
};

export const saveSupportFlag = (supportKey, values) => {
  const apiCall = new APIV2Call().fullpath(`/settings/${supportKey}`);
  const meta = { notification: true };

  //Clear any support flag cached in the getSupportFlag.ts file (new way of fetching support keys)
  clearCachedSupportFlag(supportKey);

  return {
    [RSAA]: {
      types: [
        SAVE_SUPPORT_FLAG_START,
        SAVE_SUPPORT_FLAG_SUCCESS,
        { type: SAVE_SUPPORT_FLAG_FAILURE, meta },
      ],
      method: "PUT",
      body: JSON.stringify(values),
      headers: { "Content-Type": "application/json" },
      endpoint: apiCall,
    },
  };
};
