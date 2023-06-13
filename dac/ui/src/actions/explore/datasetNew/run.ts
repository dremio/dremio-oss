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
// @ts-ignore
import { updateParams } from "@inject/actions/explore/dataset/updateLocation";
import {
  RUN_DATASET_FAILURE,
  RUN_DATASET_START,
  RUN_DATASET_SUCCESS,
  TRANSFORM_AND_RUN_DATASET_FAILURE,
  TRANSFORM_AND_RUN_DATASET_START,
  TRANSFORM_AND_RUN_DATASET_SUCCESS,
} from "@app/actions/explore/dataset/run";
import Immutable from "immutable";
import readResponseAsJSON from "@app/utils/apiUtils/responseUtils";

const newFetchRunDataset = (
  dataset: Immutable.Map<string, any>,
  viewId: string,
  sessionId: string
) => {
  const tipVersion = dataset.get("tipVersion");

  const apiCall = new APIV2Call().paths(
    `${dataset.getIn(["apiLinks", "self"])}/run`
  );

  if (tipVersion) {
    apiCall.params({ tipVersion });
  }

  if (sessionId) {
    apiCall.params({ sessionId });
  }

  updateParams(apiCall);

  const meta = { dataset, viewId };

  return {
    [RSAA]: {
      types: [
        { type: RUN_DATASET_START, meta },
        readResponseAsJSON(RUN_DATASET_SUCCESS, meta),
        { type: RUN_DATASET_FAILURE, meta },
      ],
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
      endpoint: apiCall,
    },
  };
};

export const newRunDataset = (
  dataset: Immutable.Map<string, any>,
  viewId: string,
  sessionId: string
) => {
  return (dispatch: any) => {
    return dispatch(newFetchRunDataset(dataset, viewId, sessionId));
  };
};

const newFetchTransformAndRun = (
  dataset: Immutable.Map<string, any>,
  transformData: any,
  viewId: string,
  sessionId: string,
  newVersion: string
) => {
  const apiCall = new APIV2Call()
    .paths(`${dataset.getIn(["apiLinks", "self"])}/transform_and_run`)
    .params({ newVersion });

  const meta = { viewId, entity: dataset };

  return {
    [RSAA]: {
      types: [
        { type: TRANSFORM_AND_RUN_DATASET_START, meta },
        readResponseAsJSON(TRANSFORM_AND_RUN_DATASET_SUCCESS, meta),
        { type: TRANSFORM_AND_RUN_DATASET_FAILURE, meta },
      ],
      method: "POST",
      body: JSON.stringify(
        !sessionId ? transformData : { ...transformData, sessionId }
      ),
      headers: {
        "Content-Type": "application/json",
      },
      endpoint: apiCall,
    },
  };
};

export const newTransformAndRunDataset = (
  dataset: Immutable.Map<string, any>,
  transformData: any,
  viewId: string,
  sessionId: string,
  newVersion: string
) => {
  return (dispatch: any) => {
    return dispatch(
      newFetchTransformAndRun(
        dataset,
        transformData,
        viewId,
        sessionId,
        newVersion
      )
    );
  };
};
