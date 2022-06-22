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

//Ref: https://medium.com/stashaway-engineering/react-redux-tips-better-way-to-handle-loading-flags-in-your-reducers-afda42a804c6
function matchAction(type: string) {
  return /(.*)_(REQUEST|SUCCESS|FAILURE)/.exec(type);
}

//Store loading flags on a per action basis
// E.g. { FETCH_DEFAULT_BRANCH: true/false }
function createLoadingReducer(actionList: string[]) {
  return function (state: any = {}, action: any) {
    if (!actionList.includes(action.type)) return state;
    const match = matchAction(action.type);
    if (!match) return state;
    const [, reqName, requestState] = match;
    const isReq = requestState === "REQUEST";
    return { ...state, [isReq ? action.type : reqName]: isReq };
  };
}

export default createLoadingReducer;
