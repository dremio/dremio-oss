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
import { REAPPLY_DATASET_SUCCESS } from 'actions/explore/dataset/reapply';
import {
  LOAD_DEPENDENT_DATASETS_SUCCESS,
  LOAD_PARENTS_SUCCESS,
  LOAD_PARENTS_FAILURE
} from 'actions/resources/spaceDetails';

export default function data(state, action) {
  switch (action.type) {
  case LOAD_PARENTS_SUCCESS:
    return state.setIn(['datasetUI', 'parentList'], action.payload);
  case LOAD_DEPENDENT_DATASETS_SUCCESS:
    return state.setIn(['datasetUI', 'descendantsList'], action.payload);
  case LOAD_PARENTS_FAILURE:
    return state.setIn(['datasetUI', 'parentList'], []);
  case REAPPLY_DATASET_SUCCESS: {
    // seems like this block does not work, as previousId has wrong format.
    // Example: previousId: "77dbab5e-a924-4cae-bdc9-eed4da63216d"
    // But table data is stored by dataset version.
    const newDataId = action.payload.getIn(['entities', 'datasetUI', action.payload.get('result'), 'id']);
    const oldData = state.getIn(['tableData', action.meta.previousId]);
    if (oldData) {
      return state.setIn(['tableData', newDataId], oldData);
    }
    return state;
  }
  default:
    return state;
  }
}
