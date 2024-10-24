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

import { newPostDatasetOperation } from "#oss/actions/explore/datasetNew/common";
import Immutable from "immutable";
import exploreUtils from "#oss/utils/explore/exploreUtils";

export const newRunTableTransform = (
  dataset: Immutable.Map<string, any>,
  transformData: any,
  viewId: string,
  tableData: any,
  sessionId: string,
  newVersion: string,
) => {
  return (dispatch: any) => {
    const href = exploreUtils.getNewPreviewTransformationLink(
      dataset,
      newVersion,
    );

    const nextTable = tableData?.set("version", newVersion);

    return dispatch(
      newPostDatasetOperation(
        href,
        dataset,
        viewId,
        nextTable,
        transformData,
        sessionId,
      ),
    );
  };
};
