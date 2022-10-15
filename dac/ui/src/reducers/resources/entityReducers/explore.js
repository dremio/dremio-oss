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
import {
  LOAD_DEPENDENT_DATASETS_SUCCESS,
  LOAD_PARENTS_SUCCESS,
  LOAD_PARENTS_FAILURE,
} from "actions/resources/spaceDetails";

export default function data(state, action) {
  switch (action.type) {
    case LOAD_PARENTS_SUCCESS:
      return state.setIn(["datasetUI", "parentList"], action.payload);
    case LOAD_DEPENDENT_DATASETS_SUCCESS:
      return state.setIn(["datasetUI", "descendantsList"], action.payload);
    case LOAD_PARENTS_FAILURE:
      return state.setIn(["datasetUI", "parentList"], []);
    default:
      return state;
  }
}
