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

import {CONVERT_DATASET_TO_FOLDER_SUCCESS} from 'actions/home';

export default function folder(state, action) {
  switch (action.type) {
  case CONVERT_DATASET_TO_FOLDER_SUCCESS:
    if (state.getIn(['folder', action.meta.folderId])) {
      return state.setIn(['folder', action.meta.folderId, 'queryable'], false);
    }
    return state;
  default:
    return state;
  }
}

