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

import schemaUtils from "utils/apiUtils/schemaUtils";
import datasetAccelerationSettingsSchema from "schemas/datasetAccelerationSettings";
import { constructFullPath } from "utils/pathUtils";
import { APIV2Call } from "#oss/core/APICall";

export const LOAD_DATASET_ACCELERATION_SETTINGS_START =
  "LOAD_DATASET_ACCELERATION_SETTINGS_START";
export const LOAD_DATASET_ACCELERATION_SETTINGS_SUCCESS =
  "LOAD_DATASET_ACCELERATION_SETTINGS_SUCCESS";
export const LOAD_DATASET_ACCELERATION_SETTINGS_FAILURE =
  "LOAD_DATASET_ACCELERATION_SETTINGS_FAILURE";

export function loadDatasetAccelerationSettings(
  fullPathList,
  viewId,
  versionContext,
) {
  const meta = { viewId };

  const params = versionContext
    ? {
        versionType: versionContext.type,
        versionValue: versionContext.value,
      }
    : {};

  const apiCall = new APIV2Call()
    .path("dataset")
    .path(constructFullPath(fullPathList))
    .paths("acceleration/settings")
    .params(params);

  // TODO: this is a workaround for accelerationSettings not having its own id
  return {
    [RSAA]: {
      types: [
        { type: LOAD_DATASET_ACCELERATION_SETTINGS_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(
          LOAD_DATASET_ACCELERATION_SETTINGS_SUCCESS,
          datasetAccelerationSettingsSchema,
          meta,
          "datasetResourcePath",
          constructFullPath(fullPathList),
        ),
        { type: LOAD_DATASET_ACCELERATION_SETTINGS_FAILURE, meta },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
}

export const UPDATE_DATASET_ACCELERATION_SETTINGS_START =
  "UPDATE_DATASET_ACCELERATION_SETTINGS_START";
export const UPDATE_DATASET_ACCELERATION_SETTINGS_SUCCESS =
  "UPDATE_DATASET_ACCELERATION_SETTINGS_SUCCESS";
export const UPDATE_DATASET_ACCELERATION_SETTINGS_FAILURE =
  "UPDATE_DATASET_ACCELERATION_SETTINGS_FAILURE";

export function updateDatasetAccelerationSettings(
  fullPathList,
  form,
  versionContext,
) {
  const params = versionContext
    ? {
        versionType: versionContext.type,
        versionValue: versionContext.value,
      }
    : {};

  const apiCall = new APIV2Call()
    .path("dataset")
    .path(constructFullPath(fullPathList))
    .paths("acceleration/settings")
    .params(params);

  return {
    [RSAA]: {
      types: [
        UPDATE_DATASET_ACCELERATION_SETTINGS_START,
        UPDATE_DATASET_ACCELERATION_SETTINGS_SUCCESS,
        UPDATE_DATASET_ACCELERATION_SETTINGS_FAILURE,
      ],
      method: "PUT",
      body: JSON.stringify(form),
      headers: { "Content-Type": "application/json" },
      endpoint: apiCall,
    },
  };
}

export const CLEAR_DATASET_ACCELERATION_SETTINGS =
  "CLEAR_DATASET_ACCELERATION_SETTINGS";

export function clearDataSetAccelerationSettings(fullPathList) {
  return function (dispatch) {
    dispatch({
      type: CLEAR_DATASET_ACCELERATION_SETTINGS,
      payload: constructFullPath(fullPathList),
    });
  };
}
