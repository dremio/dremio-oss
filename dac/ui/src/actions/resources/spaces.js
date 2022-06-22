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
import { RSAA } from "redux-api-middleware";
import { arrayOf } from "normalizr";

import spaceSchema from "dyn-load/schemas/space";

import APICall from "@app/core/APICall";
import schemaUtils from "utils/apiUtils/schemaUtils";
import actionUtils from "utils/actionUtils/actionUtils";
import {
  getParamsForSpacesUrl,
  updateSpacePermissions,
} from "dyn-load/actions/resources/spacesMixin";
import FormUtils from "dyn-load/utils/FormUtils/FormUtils";

export const SPACES_LIST_LOAD_START = "SPACES_LIST_LOAD_START";
export const SPACES_LIST_LOAD_SUCCESS = "SPACES_LIST_LOAD_SUCCESS";
export const SPACES_LIST_LOAD_FAILURE = "SPACES_LIST_LOAD_FAILURE";

export const ALL_SPACES_VIEW_ID = "AllSpaces";
function fetchSpaceListData(includeDatasetCount = false) {
  const meta = { viewId: ALL_SPACES_VIEW_ID, replaceEntities: true };

  const apiCall = new APICall()
    .path("catalog")
    .params({ include: getParamsForSpacesUrl(includeDatasetCount) })
    .uncachable();

  return {
    [RSAA]: {
      types: [
        { type: SPACES_LIST_LOAD_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(
          SPACES_LIST_LOAD_SUCCESS,
          { data: arrayOf(spaceSchema) },
          meta
        ),
        { type: SPACES_LIST_LOAD_FAILURE, meta },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
}

export function loadSpaceListData() {
  return (dispatch) => {
    return dispatch(fetchSpaceListData()).then(
      dispatch(fetchSpaceListData(true))
    );
  };
}

export const SAVE_SPACE_START = "SAVE_SPACE_START";
export const SAVE_SPACE_SUCCESS = "SAVE_SPACE_SUCCESS";
export const SAVE_SPACE_FAILURE = "SAVE_SPACE_FAILURE";

function saveSpace(values, isCreate) {
  const meta = {
    invalidateViewIds: [ALL_SPACES_VIEW_ID], // cause data reload. See SpacesLoader
    mergeEntities: true,
    notification: {
      message: isCreate
        ? la("Successfully created.")
        : la("Successfully updated."),
      level: "success",
    },
  };
  // AccessControlsListSection mutates submit values to include "userControls" and
  // "groupControls" instead of "users" and "groups", expected in V3 /catalog/ API.
  const space = FormUtils.makeSpaceFromFormValues(values);

  const apiCall = new APICall();
  apiCall.path("catalog");

  if (!isCreate) {
    apiCall.path(space.id);
  }

  return {
    [RSAA]: {
      types: [
        SAVE_SPACE_START,
        schemaUtils.getSuccessActionTypeWithSchema(
          SAVE_SPACE_SUCCESS,
          spaceSchema,
          meta
        ),
        SAVE_SPACE_FAILURE,
      ],
      method: isCreate ? "POST" : "PUT",
      body: JSON.stringify(space),
      endpoint: apiCall,
    },
  };
}

export function createNewSpace(values) {
  return saveSpace(values, true);
}

export function updateSpace(values) {
  return saveSpace(values, false);
}

export function updateSpacePrivileges(values) {
  return updateSpacePermissions(values);
}

export const REMOVE_SPACE_START = "REMOVE_SPACE_START";
export const REMOVE_SPACE_SUCCESS = "REMOVE_SPACE_SUCCESS";
export const REMOVE_SPACE_FAILURE = "REMOVE_SPACE_FAILURE";

export function removeSpace(spaceId, spaceVersion) {
  const meta = {
    id: spaceId,
    invalidateViewIds: [ALL_SPACES_VIEW_ID], // cause data reload. See SpacesLoader
  };
  const errorMessage = la("There was an error removing the space.");

  const apiCall = new APICall()
    .path("catalog")
    .path(spaceId)
    .params({ tag: spaceVersion });

  return {
    [RSAA]: {
      types: [
        {
          type: REMOVE_SPACE_START,
          meta,
        },
        {
          type: REMOVE_SPACE_SUCCESS,
          meta: { ...meta, success: true },
        },
        {
          type: REMOVE_SPACE_FAILURE,
          meta: {
            ...meta,
            notification: actionUtils.humanizeNotificationMessage(errorMessage),
          },
        },
      ],
      method: "DELETE",
      endpoint: apiCall,
    },
  };
}
