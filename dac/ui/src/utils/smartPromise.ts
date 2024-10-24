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
import Immutable from "immutable";
import { StatusCondition } from "react-smart-promise/dist-types/StatusCondition.type";

export function isReqLoading(status: StatusCondition) {
  return ["INITIAL", "PENDING"].includes(status);
}

export function getViewStateFromReq(
  error: any,
  status: StatusCondition,
  defaultError = "Failed to fetch",
) {
  if (["PENDING", "INITIAL"].includes(status)) {
    return Immutable.Map({ isInProgress: true });
  } else if (error || status === "ERROR") {
    return Immutable.Map({
      isInProgress: false,
      isFailed: true,
      error: Immutable.Map({ message: error?.statusText || defaultError }),
    });
  }
  return;
}
