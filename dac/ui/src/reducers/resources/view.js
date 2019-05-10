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
import Immutable from 'immutable';
import uuid from 'uuid';

import { RequestError, ApiError, InternalError } from 'redux-api-middleware';
import { RESET_VIEW_STATE, UPDATE_VIEW_STATE, DISMISS_VIEW_STATE_ERROR } from 'actions/resources';
import { CANCEL_TRANSFORM } from 'actions/explore/dataset/transform';
import { RESET_NEW_QUERY } from 'actions/explore/view';
import { CLEAR_ENTITIES } from 'actions/resources/entities';

export const NO_INTERNET_MESSAGE = 'Could not connect to the Dremio server.'; // todo: loc


function isSuccessAction(action) {
  return !action.error && (action.payload !== undefined || action.meta.success);
}

function isStartAction(action) {
  return !action.error && action.payload === undefined;
}

function isAutoPeekError(action) {
  return action.error && action.meta.submitType === 'autoPeek';
}

export function getErrorMessage(action) {
  // allow overriding the error message in action creator.
  if (action.meta && action.meta.errorMessage) {
    return { errorMessage: action.meta.errorMessage };
  }

  const {payload} = action;
  if (payload) {
    if (payload instanceof RequestError) {
      return { errorMessage: NO_INTERNET_MESSAGE };
    }
    if (payload instanceof ApiError) {
      if (payload.response && payload.response.errorMessage) {
        return payload.response;
      }
      return { errorMessage: payload.message };
    }
    if (payload instanceof InternalError) {
      console.error('InternalError', action);
      return { errorMessage: `${payload.name}: ${payload.message}.` };
    }
  }
  return { errorMessage: 'Unknown error: ' + payload };
}

export function getErrorDetails(action) {
  const {payload} = action;
  if (payload) {
    if (payload instanceof ApiError) {
      if (payload.response && payload.response.details) {
        return payload.response.details;
      }
    }
  }
}

function invalidateViewIds(state, action) {
  const viewIds = action.meta.invalidateViewIds;
  if (!viewIds || !isSuccessAction(action)) {
    return state;
  }
  return state.mergeDeep(viewIds.reduce(
    (previous, current) => previous.set(current, Immutable.Map({invalidated: true})),
    Immutable.Map())
  );
}

export const getDefaultViewConfig = viewId => ({
  viewId, isInProgress: false, isFailed: false, isWarning: false, invalidated: false, error: null
});

function getInitialViewState(viewId) {
  return Immutable.Map(getDefaultViewConfig(viewId));
}

export const getViewStateFromAction = (action) => {

  // todo: these duck-type sniffers are quite brittle
  // e.g. they used to think a DELETE "success" with a viewId was a "start"
  // Fixed by making sure isSuccessAction checks first and crudFactory sets meta.success.
  // (But should be replaced with something better.)

  if (isSuccessAction(action)) {
    return {
      isInProgress: false,
      isFailed: false,
      isWarning: false,
      isAutoPeekFailed: false,
      error: null
    };
  }
  if (isStartAction(action)) {
    return {
      isInProgress: true,
      isFailed: false,
      isWarning: false,
      invalidated: false,
      isAutoPeekFailed: false,
      error: null
    };
  }
  if (isAutoPeekError(action)) {
    return {
      isInProgress: false,
      isFailed: false,
      isWarning: false,
      isAutoPeekFailed: true,
      error: null
    };
  }

  // FAILURE
  return {
    isInProgress: false,
    isFailed: true,
    isWarning: false,
    isAutoPeekFailed: false,
    error: {
      message: getErrorMessage(action),
      details: getErrorDetails(action),
      id: uuid.v4(),
      dismissed: false
    }
  };
};

function updateLoadingViewId(state, action) {
  const {viewId, invalidateViewIds: viewIds} = action.meta;
  if (!viewId) {
    return state;
  }

  let newViewState = getViewStateFromAction(action);
  if (isSuccessAction(action)) {
    newViewState = {
      ...newViewState,
      isInProgress: !viewIds || !viewIds.length ? false : state.getIn([...viewId, 'isInProgress'])
    };
  }
  return state.mergeIn([viewId], {
    ...newViewState,
    viewId
  });
}

export default function view(state = Immutable.Map(), action) {
  const { meta } = action;
  if (action.type === RESET_VIEW_STATE) {
    return state.set(meta.viewId, getInitialViewState(meta.viewId));
  }

  if (action.type === UPDATE_VIEW_STATE) {
    return state.mergeIn([meta.viewId], {viewId: meta.viewId, ...meta.viewState});
  }

  if (action.type === DISMISS_VIEW_STATE_ERROR) {
    return state.mergeIn([meta.viewId, 'error'], { dismissed: true });
  }

  if (action.type === CLEAR_ENTITIES) {
    return Immutable.Map();
  }

  if (!meta) {
    if (action.type === CANCEL_TRANSFORM) {
      return state.mergeIn([action.viewId], {isInProgress: false, isFailed: false, invalidated: false});
    }
    if (action.type === RESET_NEW_QUERY) {
      return state.set(action.viewId, getInitialViewState(action.viewId));
    }
    return state;
  }

  return updateLoadingViewId(
    invalidateViewIds(state, action),
    action
  );
}
