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
  NESSIE_RESET_STATE,
  REMOVE_ENTRY,
  RESET_REFS,
  SET_REFERENCES_LIST,
} from "@app/actions/nessie/nessie";
import { ARCTIC_STATE_PREFIX, NESSIE_REF_PREFIX } from "@app/constants/nessie";
import { NessieRootState, NessieState } from "@app/types/nessie";
import nessieErrorReducer from "./nessieErrorReducer";
import nessieLoadingReducer from "./nessieLoadingReducer";
import { initializeDatasetRefs, initializeRefState } from "./utils";
import {
  convertReferencesListToRootState,
  getStateRefsOmitted,
} from "@app/utils/nessieUtils";

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
    return initializeDatasetRefs(state, action.payload);
  } else if (action.type === RESET_REFS) {
    return initializeDatasetRefs(getStateRefsOmitted(state), action.payload);
  } else if (action.type === NESSIE_RESET_STATE) {
    return {};
  } else if (action.type === REMOVE_ENTRY) {
    //Remove `CatalogName` and `ref/CatalogName` and `__ARCTIC/CatalogName` entries from state
    const {
      [action.payload]: omit,
      [`${NESSIE_REF_PREFIX}${action.payload}`]: refOmit,
      [`${ARCTIC_STATE_PREFIX}${action.payload}`]: arcticOmit,
      ...newState
    } = state;
    if (omit || refOmit || arcticOmit) {
      return { ...newState };
    } else {
      return state;
    }
  } else if (action.type === SET_REFERENCES_LIST) {
    return convertReferencesListToRootState(state, action.payload);
  } else {
    const { source } = action;
    return {
      ...state,
      [source]: nessieReducer(state[source], action),
    };
  }
}

export default nessieRootReducer;
