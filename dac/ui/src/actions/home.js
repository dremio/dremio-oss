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

import ApiUtils from "utils/apiUtils/apiUtils";

import folderSchema from "schemas/folder";
import schemaUtils from "utils/apiUtils/schemaUtils";
import actionUtils from "@app/utils/actionUtils/actionUtils";
import { sidebarMinWidth } from "@app/pages/HomePage/components/Columns.less";
import * as schemas from "@app/schemas";
import { APIV2Call } from "@app/core/APICall";
import { getSonarContentsResponse } from "@app/exports/endpoints/SonarContents/getSonarContents";
import { getLoggingContext } from "dremio-ui-common/contexts/LoggingContext.js";
import { ApiError } from "redux-api-middleware";

export const logger = getLoggingContext().createLogger("actions/home");

export const CONVERT_FOLDER_TO_DATASET_START =
  "CONVERT_FOLDER_TO_DATASET_START";
export const CONVERT_FOLDER_TO_DATASET_SUCCESS =
  "CONVERT_FOLDER_TO_DATASET_SUCCESS";
export const CONVERT_FOLDER_TO_DATASET_FAILURE =
  "CONVERT_FOLDER_TO_DATASET_FAILURE";

function fetchConvertFolder({ folder, values, viewId }) {
  const meta = { viewId, invalidateViewIds: ["HomeContents"] };

  const apiCall = new APIV2Call().fullpath(
    `${folder.getIn(["links", "format"])}`
  );

  return {
    [RSAA]: {
      types: [
        CONVERT_FOLDER_TO_DATASET_START,
        schemaUtils.getSuccessActionTypeWithSchema(
          CONVERT_FOLDER_TO_DATASET_SUCCESS,
          folderSchema,
          meta
        ),
        { type: CONVERT_FOLDER_TO_DATASET_FAILURE, meta },
      ],
      method: "PUT",
      body: JSON.stringify(values),
      headers: { "Content-Type": "application/json" },
      endpoint: apiCall,
    },
  };
}

export function convertFolderToDataset({ folder, values, viewId }) {
  return (dispatch) => {
    return dispatch(fetchConvertFolder({ folder, values, viewId }));
  };
}

export const CONVERT_DATASET_TO_FOLDER_START =
  "CONVERT_DATASET_TO_FOLDER_START";
export const CONVERT_DATASET_TO_FOLDER_SUCCESS =
  "CONVERT_DATASET_TO_FOLDER_SUCCESS";
export const CONVERT_DATASET_TO_FOLDER_FAILURE =
  "CONVERT_DATASET_TO_FOLDER_FAILURE";

function fetchConvertDataset(entity, viewId) {
  // DX-8102: invalidating home view id so that # of jobs of the folder updates
  const meta = {
    viewId,
    folderId: entity.get("id"),
    invalidateViewIds: ["HomeContents"],
  };
  const successMeta = { ...meta, success: true }; // doesn't invalidateViewIds without `success: true`
  const errorMessage = laDeprecated(
    "There was an error removing the format for the folder."
  );

  const apiCall = new APIV2Call().fullpath(
    entity.getIn(["links", "delete_format"])
  );

  return {
    [RSAA]: {
      types: [
        { type: CONVERT_DATASET_TO_FOLDER_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(
          CONVERT_DATASET_TO_FOLDER_SUCCESS,
          folderSchema,
          successMeta
        ),
        {
          type: CONVERT_DATASET_TO_FOLDER_FAILURE,
          meta: {
            ...meta,
            notification: actionUtils.humanizeNotificationMessage(errorMessage),
          },
        },
      ],
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      endpoint: apiCall,
    },
  };
}

export function convertDatasetToFolder(entity, viewId) {
  return (dispatch) => {
    return dispatch(fetchConvertDataset(entity, viewId));
  };
}

export const wikiActions = actionUtils.generateRequestActions("WIKI");

const wikiSuccess = (dispatch, resolvePromise, wikiData, actionDetails) => {
  const data = {
    //default values
    text: "",
    version: null,
    //--------------
    ...wikiData,
  };
  dispatch({
    type: wikiActions.success,
    ...data,
    ...actionDetails,
  });
  resolvePromise(data);
};

export const loadWiki = (dispatch) => (entityId) => {
  if (!entityId) return;
  const commonActionProps = { entityId };
  dispatch({
    type: wikiActions.start,
    ...commonActionProps,
  });

  return new Promise((resolve) => {
    ApiUtils.fetch(`catalog/${entityId}/collaboration/wiki`)
      .then(
        async (response) => {
          if (response) {
            const wikiData = await response.json();
            wikiSuccess(dispatch, resolve, wikiData, commonActionProps);
          }
          return null;
        },
        async (response) => {
          // no error message needed on 404 when wiki is not present for given id
          if (response.status === 404) {
            wikiSuccess(dispatch, resolve, {}, commonActionProps);
            return;
          }
          const errorInfo = {
            errorMessage: await ApiUtils.getErrorMessage(
              laDeprecated("Wiki API returned an error"),
              response
            ),
            errorId: "" + Math.random(),
          };
          dispatch({
            type: wikiActions.failure,
            ...errorInfo,
            ...commonActionProps,
          });
        }
      )
      .catch((error) => error);
  });
};

export const WIKI_SAVED = "WIKI_SAVED";
export const wikiSaved = (entityId, text, version) => ({
  type: WIKI_SAVED,
  entityId,
  text,
  version,
});

export const MIN_SIDEBAR_WIDTH = parseInt(sidebarMinWidth, 10);
export const SET_SIDEBAR_SIZE = "SET_SIDEBAR_SIZE";
export const setSidebarSize = (size) => ({
  type: SET_SIDEBAR_SIZE,
  size: Math.max(MIN_SIDEBAR_WIDTH, size),
});

export const contentLoadActions =
  actionUtils.generateRequestActions("HOME_CONTENT_LOAD");
export const RESET_HOME_CONTENTS = "RESET_HOME_CONTENTS";

export function resetHomeContents() {
  return {
    type: RESET_HOME_CONTENTS,
    meta: {
      viewId: "HomeContents",
      invalidateViewIds: ["HomeContents"],
    },
  };
}
const isAborted = (json) => json === "The user aborted a request.";

export const loadHomeContent = (
  getDataUrl,
  entityType,
  viewId,
  params,
  abortController
) => {
  const entitySchema = schemas[entityType];
  const meta = { viewId };
  return {
    [RSAA]: {
      types: [
        { type: contentLoadActions.start, meta },
        {
          type: contentLoadActions.success,
          meta: async (action, state, res) => {
            try {
              const json = await res.clone().json();
              if (isAborted(json)) {
                return { ...meta }; //Skip setting content
              } else {
                return {
                  entitySchema,
                  ...meta,
                };
              }
            } catch (e) {
              //
            }
          },
          payload: (action, state, res) => {
            return res
              .clone()
              .json()
              .then((json) => {
                if (isAborted(json)) throw json;
                else return json;
              })
              .catch(() => new ApiError(res.status, "CANCELED", res));
          },
        },
        { type: contentLoadActions.failure, meta },
      ],
      method: "GET",
      endpoint: "", // Overridden by fetch
      fetch: () => {
        const reqInit = abortController
          ? { signal: abortController.signal }
          : undefined;
        return getSonarContentsResponse(
          {
            path: getDataUrl,
            params,
            entityType,
          },
          reqInit
        )
          .then((res) => res.clone())
          .catch((e) => {
            if (e instanceof DOMException) {
              logger.debug("Aborting previous request");
              return new Response(JSON.stringify(e.message), {
                ...e,
              });
            }
            return new Response(JSON.stringify(e.responseBody), {
              status: 400,
              headers: {
                "Content-Type": "application/json",
              },
            });
          });
      },
    },
  };
};

export const loadHomeContentWithAbort = (
  getDataUrl,
  entityType,
  viewId,
  params
) => {
  const controller = new AbortController();
  const rsaa = loadHomeContent(
    getDataUrl,
    entityType,
    viewId,
    params,
    controller
  );

  return [controller, rsaa];
};
