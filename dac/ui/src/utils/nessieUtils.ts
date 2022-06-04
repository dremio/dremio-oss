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
import { NessieRootState, NessieState } from '@app/reducers/nessie/nessie';

export function getShortHash(hash: string) {
  return hash.length > 6 ? hash.substring(0, 6) : hash;
}

export function getIconByType(refType: string, hash?: string | null) {
  if (hash) return 'NessieCommit.svg';
  else if (refType === 'TAG') return 'NessieTag.svg';
  else return 'GitBranch.svg';
}

export function getFullPathByType(refName: string, hash?: string | null) {
  let path = refName;
  if (hash) path = path + ` @ ${getShortHash(hash)}`;
  return path;
}

export function getTypeAndValue(state?: NessieState | null) {
  let value = null;
  if (!state) return null;
  if (state.hash) {
    value = {
      type: 'COMMIT',
      value: state.hash
    };
  } else if (state.reference) {
    value = {
      type: state.reference.type,
      value: state.reference.name
    };
  }
  return value;
}

function getNessieState(nessie: NessieRootState, stateKey: string) {
  if (!nessie || !stateKey) return null;
  const state = nessie[stateKey]; //Send reference if it exists in Nessie State
  if (!state || !state.reference) return null;
  return state;
}

//Ref parameter object that is sent to endpoints
export function getRefParams(nessie: NessieRootState, stateKey: string) {
  const state = getNessieState(nessie, stateKey);
  const value = getTypeAndValue(state);
  if (!value) {
    return {};
  } else {
    return { ref: value };
  }
}

//Ref parameter object that is sent to endpoints
export function getRefQueryParams(nessie: NessieRootState, stateKey: string) {
  const state = getNessieState(nessie, stateKey);
  const value = getTypeAndValue(state);
  if (!value) {
    return {};
  } else {
    return {
      refType: value.type,
      refValue: value.value
    };
  }
}
