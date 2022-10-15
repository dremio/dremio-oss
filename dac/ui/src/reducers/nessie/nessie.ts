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
  DEFAULT_REF_REQUEST_SUCCESS,
  INIT_REFS,
  NessieActionTypes,
  NessieRootActionTypes,
  SET_REF,
  SET_REFS,
} from "@app/actions/nessie/nessie";
import { NessieRootState, NessieState } from "@app/types/nessie";
import nessieErrorReducer from "./nessieErrorReducer";
import nessieLoadingReducer from "./nessieLoadingReducer";
import { initializeDatasetRefs, initializeRefState } from "./utils";
//@ts-ignore

export const initialState: NessieState = {
  defaultReference: null,
  reference: null,
  hash: null,
  date: null,
  loading: {},
  errors: {},
};

function nessieReducer(
  oldState = initialState,
  action: NessieActionTypes
): NessieState {
  let state = oldState;
  const loadingState = nessieLoadingReducer(state.loading, action); //Automatically handles loading flags for req,success,failure
  const errorState = nessieErrorReducer(state.errors, action); //Automatically handles storing error payload
  switch (action.type) {
    case SET_REF: {
      state = {
        ...state,
        ...action.payload,
        hash: action.payload.hash || null,
        date: action.payload.date || null,
      };
      break;
    }
    case DEFAULT_REF_REQUEST_SUCCESS:
      state = {
        ...state,
        defaultReference: action.payload,
        ...(state.reference == null && {
          reference: action.payload,
        }),
      };
      break;
    default:
      break;
  }
  if (loadingState !== state.loading) {
    state = { ...state, loading: loadingState };
  }
  if (errorState !== state.errors) {
    state = { ...state, errors: errorState };
  }
  return state;
}

//Map: sourceId -> NessieState
function nessieRootReducer(
  state = {} as NessieRootState,
  action: NessieActionTypes | NessieRootActionTypes
): NessieRootState {
  const { type } = action;
  if (!type.startsWith("NESSIE_")) {
    return state;
  }
  if (action.type === INIT_REFS) {
    return { ...state, ...initializeRefState(state) };
  } else if (action.type === SET_REFS) {
    return { ...state, ...initializeDatasetRefs(state, action.payload) };
  } else {
    const { source } = action;
    return {
      ...state,
      [source]: nessieReducer(state[source], action),
    };
  }
}

export default nessieRootReducer;
