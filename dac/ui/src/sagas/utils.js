/*
 * Copyright (C) 2017 Dremio Corporation
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
import { CALL_API, isRSAA } from 'redux-api-middleware';
import invariant from 'invariant';
import deepEqual from 'deep-equal';

export const LOCATION_CHANGE = '@@router/LOCATION_CHANGE';

// This check is to ignore location change to the current location
export function getLocationChangePredicate(oldLocation) {
  return (action) => {
    const { payload } = action;
    if (!payload) {
      return false;
    }
    return action.type === LOCATION_CHANGE &&
      (
        oldLocation.pathname !== payload.pathname ||
        !deepEqual(oldLocation.query, payload.query) ||
        !deepEqual(oldLocation.state, payload.state)
      );
  };
}


export function getActionPredicate(actionType, entity) {
  return (action) => {
    const actionTypeList = actionType instanceof String ? [actionType] : actionType;
    return actionTypeList.indexOf(action.type) !== -1 && (!entity || (action.meta && action.meta.entity) === entity);
  };
}

export function getApiCallCompletePredicate(apiAction) {
  const actionTypes = getApiActionTypes(apiAction);
  const entity = getApiActionEntity(apiAction);
  return (action) => {
    const actionEntity = action.meta && action.meta.entity;
    if (actionTypes.indexOf(action.type) !== -1 && action.error) {
      return (!entity || actionEntity === entity);
    }
    return getActionPredicate(actionTypes.slice(1), entity)(action);
  };
}

export function unwrapAction(wrappedAction) {
  let result = wrappedAction;
  while (typeof result === 'function') {
    result = result((action) => action);
  }
  return result;
}

export function getApiActionTypes(apiAction) {
  const callApiAction = unwrapAction(apiAction);
  invariant(isRSAA(callApiAction), 'Not a valid api action');
  return callApiAction[CALL_API].types.map(actionType => typeof actionType === 'string' ? actionType : actionType.type);
}

export function getApiActionEntity(apiAction) {
  const callApiAction = unwrapAction(apiAction);
  invariant(isRSAA(callApiAction), 'Not a valid api action');
  const actionTypes = callApiAction[CALL_API].types;
  const successType = actionTypes && actionTypes[1];
  return successType && successType.meta && successType.meta.entity;
}
