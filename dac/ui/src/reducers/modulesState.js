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
import { get } from 'lodash/object';
import { ADD_MODULE_STATE, RESET_MODULE_STATE } from '@app/actions/modulesState';

// dynamic state storage. Primary usage is storing of page data. We need to store data only for
// current page and remove state for other pages
const modulesStateReducer = (state = {}, action) => {
  const {
    type,
    reducer,
    moduleKey
  } = action;

  switch (type) {
  case ADD_MODULE_STATE: {
    const module = state[moduleKey];
    // Safety check. We should not use the same module key for different data structures
    // But it is ok, if we would like to reset state to default using ADD_MODULE action
    if (module && module.reducer !== reducer) {
      throw new Error(`Invalid module usage. Most likely module key '${moduleKey}' is used for different components`);
    }
    return {
      ...state,
      [moduleKey]: {
        reducer,
        data: reducer(undefined, action)
      }
    };
  }
  case RESET_MODULE_STATE: {
    const {
      [moduleKey]: removedModule, // remove state
      ...rest
    } = state;

    return rest;
  }
  default:
    return Object.keys(state).reduce((nextState, currentModuleKey) => {
      const moduleState = state[currentModuleKey];
      nextState[currentModuleKey] = {
        ...moduleState,
        data: moduleState.reducer(moduleState.data, action)
      };
      return nextState;
    }, {});
  }
};

export default modulesStateReducer;

// selectors
export const isInitialized = (state, moduleKey) => !!state[moduleKey];
export const getData = (state, moduleKey) => get(state[moduleKey], 'data', null);
