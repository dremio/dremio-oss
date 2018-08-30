/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { NEW_UNTITLED_FAILURE, NEW_UNTITLED_SUCCESS } from '@app/actions/explore/dataset/new';
import { LOAD_EXPLORE_ENTITIES_FAILURE, LOAD_EXPLORE_ENTITIES_SUCCESS } from '@app/actions/explore/dataset/get';

export default function data(state, action) {
  switch (action.type) {
  case NEW_UNTITLED_SUCCESS:
  case LOAD_EXPLORE_ENTITIES_SUCCESS:
    return state.set('datasetSummary', null);
  case NEW_UNTITLED_FAILURE: //read data set summary here
  case LOAD_EXPLORE_ENTITIES_FAILURE:
    if (action.payload.response.details && action.payload.response.details.datasetSummary) {
      return state.set('datasetSummary', action.payload.response.details.datasetSummary);
    }
    return state.set('datasetSummary', null);
  default:
    return state;
  }
}
