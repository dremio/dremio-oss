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
import { combineReducers } from "redux";

import {
  USER_GET_START,
  USER_GET_FAILURE,
  USER_GET_SUCCESS,
  EDIT_USER_SUCCESS,
} from "#oss/actions/modals/editUserModal";
import { isLoading } from "#oss/reducers/reducerFactories";

export const moduleKey = "EDIT_USER_DETAILS";

//state/payload has a form of UserInfo java class
const userDetails = (state = null, { type, payload }) => {
  switch (type) {
    case USER_GET_START:
    case USER_GET_FAILURE:
      return null;
    case USER_GET_SUCCESS:
    case EDIT_USER_SUCCESS:
      return payload;
    default:
      return state;
  }
};

const userLoadActions = {
  start: USER_GET_START,
  success: USER_GET_SUCCESS,
  failure: USER_GET_FAILURE,
};

export default combineReducers({
  userDetails,
  isLoading: isLoading(userLoadActions),
});

export const getUserInfo = (state) => state.userDetails;
