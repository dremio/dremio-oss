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
import { UPDATE_HISTORY_WITH_JOB_STATE } from 'actions/explore/history';

export default function data(state, action) {
  switch (action.type) {
  case UPDATE_HISTORY_WITH_JOB_STATE: {
    const { datasetVersion, jobState } = action.meta;
    const historyItem = state.getIn(['historyItem', datasetVersion]);
    if (historyItem) {
      return state.setIn(['historyItem', datasetVersion, 'state'], jobState);
    }
    return state;
  }
  default:
    return state;
  }
}
