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
import ApiUtils from "./apiUtils";

/**
 * Poll the server api
 * @param apiParams {endpoint, options, version}
 * @param handleFailure (takes caught error or {error: msg}; should return boolean isStopPollingNeeded)
 * @param handleSuccess (takes response; should return boolean isStopPollingNeeded)
 * @param intervalSec
 * @param timeoutSec
 * @constructor
 */
export default function ApiPolling({
  endpoint,
  options,
  version,
  handleFailure,
  handleSuccess,
  intervalSec = 5,
  timeoutSec = 6,
  apiCallFunc,
}) {
  let pollingHandle;

  const makeCall = async () => {
    return await callApi(handleOk, handleError);
  };

  const callApi = (handleOk, handleError) => {
    if (apiCallFunc) {
      return apiCallFunc().then(handleOk).catch(handleError);
    }
    return ApiUtils.fetch(endpoint, options, version)
      .then(handleOk)
      .catch(handleError);
  };

  const handleOk = (response) => {
    if (handleSuccess(response)) {
      // handleSuccess returns boolean isStopPollingNeeded
      clearTimeout(timeoutHandle);
    } else {
      scheduleApiCall();
    }
  };

  const handleError = (error) => {
    if (handleFailure(error)) {
      // handleFailure returns boolean isStopPollingNeeded
      clearTimeout(timeoutHandle);
    } else {
      scheduleApiCall();
    }
  };

  const scheduleApiCall = () => {
    pollingHandle = setTimeout(makeCall, intervalSec * 1000);
  };

  const handleTimeout = () => {
    clearTimeout(pollingHandle);
    handleFailure({ error: "Exceeded wait time." });
  };

  // start polling and set timeout
  const timeoutHandle = setTimeout(handleTimeout, timeoutSec * 1000);
  return makeCall();
}
