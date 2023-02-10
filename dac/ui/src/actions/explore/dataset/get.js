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
import { APIV2Call } from "@app/core/APICall";

export const LOAD_EXPLORE_ENTITIES_STARTED = "LOAD_EXPLORE_ENTITIES_STARTED";
export const LOAD_EXPLORE_ENTITIES_SUCCESS = "LOAD_EXPLORE_ENTITIES_SUCCESS";
export const LOAD_EXPLORE_ENTITIES_FAILURE = "LOAD_EXPLORE_ENTITIES_FAILURE";

function fetchEntities({
  href,
  schema,
  viewId,
  uiPropsForEntity,
  invalidateViewIds,
}) {
  const meta = { viewId, invalidateViewIds, href };

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        { type: LOAD_EXPLORE_ENTITIES_STARTED, meta },
        schemaUtils.getSuccessActionTypeWithSchema(
          LOAD_EXPLORE_ENTITIES_SUCCESS,
          schema,
          meta,
          uiPropsForEntity
        ),
        { type: LOAD_EXPLORE_ENTITIES_FAILURE, meta },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
}

function newFetchEntities({
  href,
  schema,
  viewId,
  datasetVersion,
  jobId,
  paginationUrl,
}) {
  const meta = { viewId, href };

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        { type: LOAD_EXPLORE_ENTITIES_STARTED, meta },
        schemaUtils.newGetSuccessActionTypeWithSchema(
          LOAD_EXPLORE_ENTITIES_SUCCESS,
          schema,
          meta,
          datasetVersion,
          jobId,
          paginationUrl
        ),
        {
          type: LOAD_EXPLORE_ENTITIES_FAILURE,
          meta: { ...meta, noUpdate: true },
        },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
}

export const loadExploreEntities =
  ({ href, schema, viewId, uiPropsForEntity, invalidateViewIds }) =>
  (dispatch) =>
    dispatch(
      fetchEntities({
        href,
        schema,
        viewId,
        uiPropsForEntity,
        invalidateViewIds,
      })
    );

export const newLoadExploreEntities =
  ({ href, schema, viewId, datasetVersion, jobId, paginationUrl }) =>
  (dispatch) =>
    dispatch(
      newFetchEntities({
        href,
        schema,
        viewId,
        datasetVersion,
        jobId,
        paginationUrl,
      })
    );

export const PERFORM_LOAD_DATASET = "PERFORM_LOAD_DATASET";

export const performLoadDataset = (dataset, viewId, willLoadTable) => {
  return {
    type: PERFORM_LOAD_DATASET,
    meta: { dataset, viewId, willLoadTable },
  };
};

export const CLEAN_DATA_VIEW_ID = "CLEAN_DATA_VIEW_ID";

export const LOAD_CLEAN_DATA_START = "LOAD_CLEAN_DATA_START";
export const LOAD_CLEAN_DATA_SUCCESS = "LOAD_CLEAN_DATA_SUCCESS";
export const LOAD_CLEAN_DATA_FAILURE = "LOAD_CLEAN_DATA_FAILURE";

export function loadCleanData(colName, dataset) {
  return (dispatch) => {
    return dispatch(loadCleanDataFetch(colName, dataset));
  };
}

function loadCleanDataFetch(colName, dataset) {
  const data = { colName };
  const meta = { viewId: CLEAN_DATA_VIEW_ID };

  const apiCall = new APIV2Call().paths(
    `${dataset.getIn(["apiLinks", "self"])}/clean`
  );

  return {
    [RSAA]: {
      types: [
        { type: LOAD_CLEAN_DATA_START, meta },
        { type: LOAD_CLEAN_DATA_SUCCESS, meta },
        { type: LOAD_CLEAN_DATA_FAILURE, meta },
      ],
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
      endpoint: apiCall,
    },
  };
}
