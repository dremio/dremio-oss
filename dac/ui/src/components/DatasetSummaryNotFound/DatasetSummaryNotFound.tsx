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

import SummaryNotFoundItemLabel from "./components/SummaryNotFoundItemLabel/SummaryNotFoundItemLabel";
import SummarySubHeader from "../DatasetSummary/components/SummarySubHeader/SummarySubHeader";
import { getIconType } from "../DatasetSummary/datasetSummaryUtils";
import { intl } from "@app/utils/intl";

import * as classes from "./DatasetSummaryNotFound.module.less";

type DatasetSummaryNotFoundProps = {
  fullPath: string;
  datasetType?: string;
  title?: string;
};
export const DatasetSummaryNotFound = ({
  fullPath,
  datasetType,
  title,
}: DatasetSummaryNotFoundProps) => {
  const { formatMessage } = intl;
  const iconName = datasetType && getIconType(datasetType);
  return (
    <div
      onClick={(e) => e.stopPropagation()}
      className={classes["dataset-not-found-summary-container"]}
    >
      <SummaryNotFoundItemLabel iconName={iconName} title={title} />
      <SummarySubHeader subTitle={fullPath} />
      <div className={classes["dataset-not-found-p-container"]}>
        <p>{formatMessage({ id: "Overlay.NotFoundDataset" })}</p>
      </div>
    </div>
  );
};

export default DatasetSummaryNotFound;
