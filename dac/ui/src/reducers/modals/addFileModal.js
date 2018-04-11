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
import Immutable  from 'immutable';

import * as ActionTypes from 'actions/modals/addFileModal';
import StateUtils from 'utils/stateUtils';
import AddFileModalMapper from 'utils/mappers/addFileModalMapper';
import gridTableMapper from 'utils/mappers/gridTableMapper';

const initialState = Immutable.fromJS({
  filePath: '',
  fileFormat: {},
  preview: {}
});

export default function jobCreating(state = initialState, action) {

  switch (action.type) {

  case ActionTypes.UPLOAD_FILE_REQUEST:
    return StateUtils.request(state, ['filePath']);

  case ActionTypes.UPLOAD_FILE_SUCCESS:
    return StateUtils.success(state, ['filePath'], action.payload, AddFileModalMapper.mapFilePayload);

  case ActionTypes.UPLOAD_FILE_FAILURE:
    return state.set('isInProgress', false).set('isFailed', true);

  case ActionTypes.FILE_FORMAT_PREVIEW_REQUEST:
    return StateUtils.request(state, ['preview']);

  case ActionTypes.FILE_FORMAT_PREVIEW_SUCCESS:
    return StateUtils.success(
      state,
      ['preview'],
      gridTableMapper.mapJson({}, action.payload, 1),
      AddFileModalMapper.mapFileFormatPayload
    );

  case ActionTypes.FILE_FORMAT_PREVIEW_FAILURE:
    return state.set('isInProgress', false).set('isFailed', true);

  case ActionTypes.RESET_FILE_FORMAT_PREVIEW:
    return state.set('preview', Immutable.Map());

  default:
    return state;
  }
}
