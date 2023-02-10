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
import { push, replace } from "react-router-redux";
import { performNextAction } from "actions/explore/nextAction";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";

export function navigateAfterReapply(response, replaceNav, nextAction) {
  return (dispatch) => {
    const nextDataset = response.payload.getIn([
      "entities",
      "datasetUI",
      response.payload.get("result"),
    ]);
    const link = wrapBackendLink(nextDataset.getIn(["links", "edit"]));

    const action = replaceNav ? replace : push;
    const result = dispatch(action(link));
    if (nextAction) {
      return result.then((nextResponse) => {
        if (!nextResponse.error) {
          return dispatch(performNextAction(nextDataset, nextAction));
        }
        return nextResponse;
      });
    }
    return result;
  };
}
