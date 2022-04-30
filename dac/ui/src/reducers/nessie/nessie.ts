
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
  NessieActionTypes,
  SET_REF
} from '@app/actions/nessie/nessie';
import { Reference } from '@app/services/nessie/client';
import nessieErrorReducer from './nessieErrorReducer';
import nessieLoadingReducer from './nessieLoadingReducer';
//@ts-ignore

// Map - sourceId: NessieState
export type NessieRootState = {
  [key: string]: NessieState;
};


export type NessieState = {
    defaultReference: Reference | null;
    reference: Reference | null;
    hash: string | null;
    date: Date | null; // When user selects a commit before certain time in branch picker
    loading: { [key: string]: boolean };
    errors: { [key: string]: any };
}

export const initialState: NessieState = {
  defaultReference: null,
  reference: null,
  hash: null,
  date: null,
  loading: {},
  errors: {}
};

function nessieReducer(state = initialState, action: NessieActionTypes): NessieState {
  state.loading = nessieLoadingReducer(state.loading, action); //Automatically handles loading flags for req,success,failure
  state.errors = nessieErrorReducer(state.errors, action); //Automatically handles storing error payload
  switch (action.type) {
  case SET_REF: {
    return {
      ...state,
      ...action.payload,
      hash: action.payload.hash || null,
      date: action.payload.date || null
    };
  }
  case DEFAULT_REF_REQUEST_SUCCESS:
    return {
      ...state,
      defaultReference: action.payload,
      ...(state.reference == null && {
        reference: action.payload
      })
    };
  default:
    return state;
  }
}

//Map: sourceId -> NessieState
function nessieRootReducer(
  state = {} as NessieRootState,
  action: NessieActionTypes
): NessieRootState {
  const { type } = action;
  if (!type.startsWith('NESSIE_') || !action.source) {
    return state;
  } else {
    const { source } = action;
    return {
      ...state,
      [source]: nessieReducer(state[source], action)
    };
  }
}

export default nessieRootReducer;
