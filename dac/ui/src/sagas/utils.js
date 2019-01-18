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
import { call, select } from 'redux-saga/effects';
import { CALL_API, isRSAA } from 'redux-api-middleware';
import invariant from 'invariant';
import deepEqual from 'deep-equal';
import { excludePageType } from '@app/pages/ExplorePage/pageTypeUtils';
import { getLocation } from '@app/selectors/routing';


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


/**
 * Returns a predicate, that returns true if explore page url was changed due to:
 * 1) Navigation out of explore page
 * 2) Current dataset or version of a dataset is changed
 * Note: navigation between data/wiki/graph tabs is not treated as page change
 * @yields {func} that monitors for a location change event
 */
export function* getExplorePageLocationChangePredicate() {
  const location = yield select(getLocation);
  return yield call(getExplorePageLocationChangePredicateImpl, location);
}

//export for tests
// is used for data load cancelation purposes
export function getExplorePageLocationChangePredicateImpl(oldLocation) {
  return (action) => {
    const { payload } = action;
    if (!payload) {
      return false;
    }
    return action.type === LOCATION_CHANGE &&
    (
      excludePageType(oldLocation.pathname) !== excludePageType(payload.pathname) ||
      !deepEqual(oldLocation.query, payload.query)
      // do not check state here as in getLocationChangePredicate above. For case of 'save as' state is changed to show a modal,
      // but we should not cancel data loading
      // state would look like:
      // { modal: "SaveAsDatasetModal" }
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
