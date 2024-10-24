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
import { get as lodashGet } from "lodash/object";
import {
  NEW_UNTITLED_FAILURE,
  NEW_UNTITLED_SUCCESS,
} from "#oss/actions/explore/dataset/new";
import {
  LOAD_EXPLORE_ENTITIES_FAILURE,
  LOAD_EXPLORE_ENTITIES_SUCCESS,
} from "#oss/actions/explore/dataset/get";

export default function data(state, action) {
  switch (action.type) {
    case NEW_UNTITLED_SUCCESS:
    case LOAD_EXPLORE_ENTITIES_SUCCESS:
      return state.set("datasetSummary", null);
    case NEW_UNTITLED_FAILURE: //read data set summary here
    case LOAD_EXPLORE_ENTITIES_FAILURE: {
      const datasetSummary =
        lodashGet(action, "payload.response.details.datasetSummary") || null;
      return state.set("datasetSummary", datasetSummary);
    }
    default:
      return state;
  }
}
