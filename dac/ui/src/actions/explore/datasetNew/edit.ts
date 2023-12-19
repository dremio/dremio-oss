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

import {
  loadExploreEntities,
  newLoadExploreEntities,
} from "@app/actions/explore/datasetNew/get";
import Immutable from "immutable";
import exploreUtils from "@app/utils/explore/exploreUtils";

export const newLoadExistingDataset = (
  dataset: Immutable.Map<string, any>,
  viewId: string,
  tipVersion: string,
  sessionId: string
) => {
  return (dispatch: any) => {
    const href = exploreUtils.getPreviewLink(dataset, tipVersion, sessionId);

    return dispatch(newLoadExploreEntities(href, viewId));
  };
};

export const loadNewDataset = (
  dataset: Immutable.Map<string, any>,
  datasetPath: string,
  sessionId: string,
  datasetVersion: string,
  jobId: string,
  paginationUrl: string,
  viewId: string,
  tabId: string
) => {
  return (dispatch: any) => {
    const href = exploreUtils.getDatasetMetadataLink(
      dataset,
      datasetPath,
      sessionId,
      datasetVersion
    );

    if (!href) {
      return undefined;
    }

    return dispatch(
      loadExploreEntities(
        href,
        datasetVersion,
        jobId,
        paginationUrl,
        viewId,
        tabId
      )
    );
  };
};
