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

import SummaryItemLabelError from "./components/SummaryItemLabelError";
import SummarySubHeader from "../components/SummarySubHeader/SummarySubHeader";
import { VersionContextType } from "dremio-ui-common/components/VersionContext.js";
import Message from "@app/components/Message";
import { getIconType } from "../datasetSummaryUtils";
import { intl } from "@app/utils/intl";
import clsx from "clsx";

import * as classes from "./DatasetSummaryError.module.less";

type DatasetSummaryErrorProps = {
  is403: boolean;
  fullPath: string;
  versionContext?: VersionContextType;
  datasetType?: string;
  title?: string;
  isOverlay?: boolean;
};
export const DatasetSummaryError = ({
  is403,
  fullPath,
  versionContext,
  datasetType,
  title,
  isOverlay,
}: DatasetSummaryErrorProps) => {
  const { formatMessage } = intl;
  const iconName = datasetType && getIconType(datasetType, !!versionContext);

  return (
    <div
      onClick={(e) => e.stopPropagation()}
      className={clsx(
        classes["dataset-not-found-summary-container"],
        isOverlay && classes["dataset-not-found-summary-container__overlay"]
      )}
    >
      <SummaryItemLabelError iconName={iconName} title={title} />
      {!is403 ? (
        <>
          <SummarySubHeader subTitle={fullPath} />
          <div className={classes["dataset-not-found-p-container"]}>
            <p>{formatMessage({ id: "Overlay.NotFoundDataset" })}</p>
          </div>
        </>
      ) : (
        <Message
          message={formatMessage({ id: "Overlay.NoAccessDataset" })}
          messageType="error"
          isDismissable={false}
          className={classes["dataset-not-found-error-message"]}
        />
      )}
    </div>
  );
};

export default DatasetSummaryError;
