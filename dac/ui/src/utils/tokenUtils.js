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
import { APIV2Call } from "@app/core/APICall";
import { API_V2, API_V3 } from "@app/constants/Api";
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";

class TokenUtils {
  getTempToken({ params, requestApiVersion }) {
    const { request } = params;
    const version = requestApiVersion === 2 ? API_V2 : API_V3;
    let cleanedRequest = request;
    if (request.indexOf("/") === 0) {
      cleanedRequest = request.substring(1);
    }
    const updatedRequest = `/${version}/${cleanedRequest}`;
    const updatedParams = {
      ...params,
      request: updatedRequest,
    };
    const tempApiCall = new APIV2Call()
      .path("temp-token")
      .params(updatedParams);
    return fetch(tempApiCall.toString(), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: localStorageUtils.getAuthToken(),
      },
    }).then((res) => res.json());
  }
}

export default new TokenUtils();
