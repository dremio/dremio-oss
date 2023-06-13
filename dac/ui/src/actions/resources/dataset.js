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
import summaryDatasetSchema from "schemas/v2/summaryDataset";
import schemaUtils from "utils/apiUtils/schemaUtils";
import { Schema } from "normalizr";
import APICall, { APIV2Call } from "@app/core/APICall";
import { store } from "@app/store/store";
import { getRefQueryParamsFromPath } from "@app/utils/nessieUtils";

export const LOAD_SUMMARY_DATASET_START = "LOAD_SUMMARY_DATASET_START";
export const LOAD_SUMMARY_DATASET_SUCCESS = "LOAD_SUMMARY_DATASET_SUCCESS";
export const LOAD_SUMMARY_DATASET_FAILURE = "LOAD_SUMMARY_DATASET_FAILURE";
export const LOADING_ITEMS = "LoadingItems";

// todo: can we nix this DS shape variation? (handle its needs with one of the other "DS" shapes)
function fetchSummaryDataset(
  fullPath,
  viewId,
  storageName,
  nodeExpanded,
  currNode,
  versionContext
) {
  const meta = {
    viewId,
    fullPath,
    errorMessage: la("Cannot provide more information about this dataset."),
    isSummaryDatasetResponse: storageName ? true : false,
    nodeExpanded,
    currNode,
  };

  const params = versionContext
    ? {
        refType: versionContext.type,
        refValue: versionContext.value,
      }
    : getRefQueryParamsFromPath(fullPath, store.getState().nessie, "/");
  const apiCall = new APIV2Call()
    .paths("datasets/summary")
    .params(params)
    .paths(fullPath);

  return {
    [RSAA]: {
      types: [
        {
          type: `${
            storageName ? `${storageName}_START` : LOAD_SUMMARY_DATASET_START
          }`,
          meta,
        },
        schemaUtils.getSuccessActionTypeWithSchema(
          `${
            storageName
              ? `${storageName}_SUCCESS`
              : LOAD_SUMMARY_DATASET_SUCCESS
          }`,
          summaryDatasetSchema,
          meta
        ),
        {
          type: `${
            storageName
              ? `${storageName}_FAILURE`
              : LOAD_SUMMARY_DATASET_FAILURE
          }`,
          meta,
        },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
}

export const loadSummaryDataset =
  (fullPath, viewId, storageName, nodeExpanded, currNode, versionContext) =>
  (dispatch) =>
    dispatch(
      fetchSummaryDataset(
        fullPath,
        viewId,
        storageName,
        nodeExpanded,
        currNode,
        versionContext
      )
    );

export const LOAD_DATASET_START = "LOAD_DATASET_START";
export const LOAD_DATASET_SUCCESS = "LOAD_DATASET_SUCCESS";
export const LOAD_DATASET_FAILURE = "LOAD_DATASET_FAILURE";

const datasetSchema = new Schema("dataset", {
  // id and datasetVersion used to be the same,
  // so there is a lot of code that has a DS version and uses it to look up
  // the datasetUI object - so can't us #id, even though it has one
  idAttribute: "id",
});

function fetchDataset(id, viewId) {
  const meta = {
    viewId,
    id,
    errorMessage: la("Cannot provide more information about this dataset."),
  };

  const apiCall = new APICall().path("catalog").path(id);

  return {
    [RSAA]: {
      types: [
        { type: LOAD_DATASET_START, meta },
        schemaUtils.getSuccessActionTypeWithSchema(
          LOAD_DATASET_SUCCESS,
          datasetSchema,
          meta
        ),
        { type: LOAD_DATASET_FAILURE, meta },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
}

export const loadDataset = (id, viewId) => (dispatch) =>
  dispatch(fetchDataset(id, viewId));
