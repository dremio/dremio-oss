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

import folderSchema from "schemas/folder";
import datasetSchema from "schemas/dataset";
import schemaUtils from "utils/apiUtils/schemaUtils";
import actionUtils from "utils/actionUtils/actionUtils";

import { constructFullPathAndEncode } from "utils/pathUtils";

import { VIEW_ID as HOME_CONTENTS_VIEW_ID } from "pages/HomePage/subpages/HomeContents";
import { getEntityType, getNormalizedEntityPath } from "@app/selectors/home";
import { ENTITY_TYPES } from "@app/constants/Constants";
import { APIV2Call } from "@app/core/APICall";
import { ALL_SPACES_VIEW_ID } from "./spaces";
import {
  getRefQueryParams,
  getRefQueryParamsFromDataset,
} from "@app/utils/nessieUtils";

export const ADD_FOLDER_START = "ADD_FOLDER_START";
export const ADD_FOLDER_SUCCESS = "ADD_FOLDER_SUCCESS";
export const ADD_FOLDER_FAILURE = "ADD_FOLDER_FAILURE";

export const addNewFolderForSpace = (name) => (dispatch, getState) => {
  const state = getState();
  const parentType = getEntityType(state);
  const parentPath = getNormalizedEntityPath(state);

  const sourceName = parentPath.replace("/source/", "");
  const params = getRefQueryParams(state.nessie, sourceName);

  const resourcePath =
    parentType === ENTITY_TYPES.folder
      ? `${parentPath}`
      : `${parentPath}/folder/`;
  const meta = { resourcePath, invalidateViewIds: [HOME_CONTENTS_VIEW_ID] };

  const apiCall = new APIV2Call()
    .paths(resourcePath)
    .params(params)
    .uncachable();

  return dispatch({
    [RSAA]: {
      types: [
        {
          type: ADD_FOLDER_START,
          meta,
        },
        schemaUtils.getSuccessActionTypeWithSchema(
          ADD_FOLDER_SUCCESS,
          folderSchema,
          meta
        ),
        {
          type: ADD_FOLDER_FAILURE,
          meta,
        },
      ],
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name,
      }),
      endpoint: apiCall,
    },
  });
};

export const REMOVE_SPACE_FOLDER_START = "REMOVE_SPACE_FOLDER_START";
export const REMOVE_SPACE_FOLDER_SUCCESS = "REMOVE_SPACE_FOLDER_SUCCESS";
export const REMOVE_SPACE_FOLDER_FAILURE = "REMOVE_SPACE_FOLDER_FAILURE";

function fetchRemoveFolder(folder) {
  const resourcePath = folder.getIn(["links", "self"]);
  const meta = {
    resourcePath,
    invalidateViewIds: [HOME_CONTENTS_VIEW_ID],
  };

  const apiCall = new APIV2Call()
    .paths(resourcePath)
    .params({ version: folder.get("version") });

  return {
    [RSAA]: {
      types: [
        {
          type: REMOVE_SPACE_FOLDER_START,
          meta,
        },
        {
          type: REMOVE_SPACE_FOLDER_SUCCESS,
          meta: { ...meta, success: true },
        },
        {
          type: REMOVE_SPACE_FOLDER_FAILURE,
          meta: {
            ...meta,
            notification: {
              message: la("There was an error removing the folder."),
              level: "error",
            },
          },
        },
      ],
      method: "DELETE",
      endpoint: apiCall,
    },
  };
}

export function removeSpaceFolder(folder) {
  return (dispatch) => {
    return dispatch(fetchRemoveFolder(folder));
  };
}

export const REMOVE_FILE_START = "REMOVE_FILE_START";
export const REMOVE_FILE_SUCCESS = "REMOVE_FILE_SUCCESS";
export const REMOVE_FILE_FAILURE = "REMOVE_FILE_FAILURE";

function fetchRemoveFile(file) {
  const resourcePath = file.getIn(["links", "self"]);
  const meta = {
    resourcePath,
    invalidateViewIds: [HOME_CONTENTS_VIEW_ID],
  };
  const errorMessage = la("There was an error removing the file.");

  const apiCall = new APIV2Call()
    .paths(resourcePath)
    .params({ version: file.getIn(["fileFormat", "version"]) });

  return {
    [RSAA]: {
      types: [
        {
          type: REMOVE_FILE_START,
          meta,
        },
        {
          type: REMOVE_FILE_SUCCESS,
          meta: { ...meta, success: true },
        },
        {
          type: REMOVE_FILE_FAILURE,
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

export function removeFile(file) {
  return (dispatch) => {
    return dispatch(fetchRemoveFile(file));
  };
}

export const REMOVE_FILE_FORMAT_START = "REMOVE_FILE_FORMAT_START";
export const REMOVE_FILE_FORMAT_SUCCESS = "REMOVE_FILE_FORMAT_SUCCESS";
export const REMOVE_FILE_FORMAT_FAILURE = "REMOVE_FILE_FORMAT_FAILURE";

function fetchRemoveFileFormat(file) {
  const meta = {
    invalidateViewIds: [HOME_CONTENTS_VIEW_ID],
  };
  const errorMessage = la(
    "There was an error removing the format for the file."
  );
  const entityRemovePaths = [["fileFormat", file.getIn(["fileFormat", "id"])]];

  const apiCall = new APIV2Call().fullpath(
    file.getIn(["links", "delete_format"])
  );

  return {
    [RSAA]: {
      types: [
        { type: REMOVE_FILE_FORMAT_START, meta },
        {
          type: REMOVE_FILE_FORMAT_SUCCESS,
          meta: { ...meta, success: true, entityRemovePaths },
        },
        {
          type: REMOVE_FILE_FORMAT_FAILURE,
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

export function removeFileFormat(file) {
  return (dispatch) => {
    return dispatch(fetchRemoveFileFormat(file));
  };
}

export const RENAME_SPACE_DATASET_START = "RENAME_SPACE_DATASET_START";
export const RENAME_SPACE_DATASET_SUCCESS = "RENAME_SPACE_DATASET_SUCCESS";
export const RENAME_SPACE_DATASET_FAILURE = "RENAME_SPACE_DATASET_FAILURE";

function fetchRenameDataset(dataset, newName) {
  const href = constructFullPathAndEncode(dataset.get("fullPathList"));
  const meta = { newName, invalidateViewIds: [HOME_CONTENTS_VIEW_ID] };

  const apiCall = new APIV2Call()
    .paths(`dataset/${href}/rename`)
    .params({ renameTo: newName });

  return {
    [RSAA]: {
      types: [
        {
          type: RENAME_SPACE_DATASET_START,
          meta,
        },
        schemaUtils.getSuccessActionTypeWithSchema(
          RENAME_SPACE_DATASET_SUCCESS,
          datasetSchema,
          meta
        ),
        {
          type: RENAME_SPACE_DATASET_FAILURE,
          meta,
        },
      ],
      method: "POST",
      endpoint: apiCall,
    },
  };
}

export function renameSpaceDataset(dataset, newName) {
  return (dispatch) => {
    return dispatch(fetchRenameDataset(dataset, newName));
  };
}

export const REMOVE_DATASET_START = "REMOVE_DATASET_START";
export const REMOVE_DATASET_SUCCESS = "REMOVE_DATASET_SUCCESS";
export const REMOVE_DATASET_FAILURE = "REMOVE_DATASET_FAILURE";

function fetchRemoveDataset(dataset) {
  const href = dataset.get("resourcePath");
  const meta = {
    name: dataset.get("name"),
    invalidateViewIds: [ALL_SPACES_VIEW_ID, HOME_CONTENTS_VIEW_ID],
  };
  const notification = {
    message: la("Successfully removed."),
    level: "success",
  };
  const errorMessage = la("There was an error removing the dataset.");

  const apiCall = new APIV2Call().fullpath(href);

  const savedTag = dataset.getIn(["datasetConfig", "savedTag"]);
  if (savedTag) {
    apiCall.params({ savedTag });
  }

  apiCall.params(getRefQueryParamsFromDataset(dataset.get("fullPathList")));

  return {
    [RSAA]: {
      types: [
        {
          type: REMOVE_DATASET_START,
          meta,
        },
        {
          type: REMOVE_DATASET_SUCCESS,
          meta: { ...meta, success: true, notification },
        },
        {
          type: REMOVE_DATASET_FAILURE,
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

export function removeDataset(dataset) {
  return (dispatch) => {
    return dispatch(fetchRemoveDataset(dataset));
  };
}

export const RENAME_FOLDER_START = "RENAME_FOLDER_START";
export const RENAME_FOLDER_SUCCESS = "RENAME_FOLDER_SUCCESS";
export const RENAME_FOLDER_FAILURE = "RENAME_FOLDER_FAILURE";

function fetchRenameFolder(folder, newName) {
  const meta = { invalidateViewIds: [HOME_CONTENTS_VIEW_ID] };

  const apiCall = new APIV2Call()
    .paths(folder.getIn(["links", "rename"]))
    .params({ renameTo: newName });

  return {
    [RSAA]: {
      types: [
        {
          type: RENAME_FOLDER_START,
          meta,
        },
        {
          type: RENAME_FOLDER_SUCCESS,
          meta,
        },
        {
          type: RENAME_FOLDER_FAILURE,
          meta,
        },
      ],
      method: "POST",
      endpoint: apiCall,
    },
  };
}

export function renameFolder(folder, oldName, newName) {
  return (dispatch) => {
    return dispatch(fetchRenameFolder(folder, oldName, newName));
  };
}

export const LOAD_DEPENDENT_DATASETS_STARTED =
  "LOAD_DEPENDENT_DATASETS_STARTED";
export const LOAD_DEPENDENT_DATASETS_SUCCESS =
  "LOAD_DEPENDENT_DATASETS_SUCCESS";
export const LOAD_DEPENDENT_DATASETS_FAILURE =
  "LOAD_DEPENDENT_DATASETS_FAILURE";

function fetchDependentDatasets(fullPath) {
  const href = constructFullPathAndEncode(fullPath);

  const apiCall = new APIV2Call().paths(`dataset/${href}/descendants`);

  return {
    [RSAA]: {
      types: [
        LOAD_DEPENDENT_DATASETS_STARTED,
        LOAD_DEPENDENT_DATASETS_SUCCESS,
        LOAD_DEPENDENT_DATASETS_FAILURE,
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
}

export function loadDependentDatasets(fullPath) {
  return (dispatch) => {
    return dispatch(fetchDependentDatasets(fullPath));
  };
}

export const LOAD_PARENTS_START = "LOAD_PARENTS_START";
export const LOAD_PARENTS_SUCCESS = "LOAD_PARENTS_SUCCESS";
export const LOAD_PARENTS_FAILURE = "LOAD_PARENTS_FAILURE";

function fetchParents(fullPath, version, viewId) {
  const href = constructFullPathAndEncode(fullPath);
  const meta = { viewId };

  const apiCall = new APIV2Call()
    .paths(`dataset/${href}/version`)
    .path(version)
    .path("parents");

  return {
    [RSAA]: {
      types: [
        {
          type: LOAD_PARENTS_START,
          meta,
        },
        {
          type: LOAD_PARENTS_SUCCESS,
          meta,
        },
        {
          type: LOAD_PARENTS_FAILURE,
          meta,
        },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
}

export function loadParents() {
  return (dispatch) => dispatch(fetchParents(...arguments));
}
