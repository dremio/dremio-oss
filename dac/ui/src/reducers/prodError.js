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
import * as ActionTypes from 'actions/prodError';

const initialState = {
  isOpen: false,
  error: null,
  errorId: null
};

export default function prodError(state = initialState, action) {
  switch (action.type) {
  case ActionTypes.SHOW_PROD_ERROR: {
    // once we've shown one error, the app is unstable - don't bother showing more
    // also prevents render feedback loop from re-triggering the error because we re-render
    if (state.error) {
      return state;
    }

    const { error, errorId } = action;
    return { ...state, error, errorId };
  }
  case ActionTypes.HIDE_PROD_ERROR:
    return { ...state, error: null, errorId: null };

  default:
    return state;
  }
}

export const getError = state => state.error;
export const getErrorId = state => state.errorId;
