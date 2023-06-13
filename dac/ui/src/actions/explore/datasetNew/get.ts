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
import { APIV2Call } from "@app/core/APICall";
import {
  LOAD_EXPLORE_ENTITIES_FAILURE,
  LOAD_EXPLORE_ENTITIES_STARTED,
  LOAD_EXPLORE_ENTITIES_SUCCESS,
} from "@app/actions/explore/dataset/get";
import { datasetWithoutData } from "@app/schemas/v2/fullDataset";
import schemaUtils from "@app/utils/apiUtils/schemaUtils";
import readResponseAsJSON from "@app/utils/apiUtils/responseUtils";

// used to call /preview/ to fetch dataset metadata after job completes
const fetchEntities = (
  href: string,
  datasetVersion: string,
  jobId: string,
  paginationUrl: string,
  viewId: string
) => {
  const meta = { viewId, href };

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        { type: LOAD_EXPLORE_ENTITIES_STARTED, meta },
        schemaUtils.newGetSuccessActionTypeWithSchema(
          LOAD_EXPLORE_ENTITIES_SUCCESS,
          datasetWithoutData,
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
};

// used to return an apiAction when previewing an existing temporary dataset
const newFetchEntities = (href: string, viewId: string) => {
  const meta = { viewId, href };

  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        { type: LOAD_EXPLORE_ENTITIES_STARTED, meta },
        readResponseAsJSON(LOAD_EXPLORE_ENTITIES_SUCCESS, meta),
        { type: LOAD_EXPLORE_ENTITIES_FAILURE, meta },
      ],
      method: "GET",
      endpoint: apiCall,
    },
  };
};

export const loadExploreEntities = (
  href: string,
  datasetVersion: string,
  jobId: string,
  paginationUrl: string,
  viewId: string
) => {
  return (dispatch: any) => {
    return dispatch(
      fetchEntities(href, datasetVersion, jobId, paginationUrl, viewId)
    );
  };
};

export const newLoadExploreEntities = (href: string, viewId: string) => {
  return (dispatch: any) => {
    return dispatch(newFetchEntities(href, viewId));
  };
};
