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

import Immutable from "immutable";
import { useSelector } from "react-redux";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import {
  IconButton,
  Popover,
  Spinner,
  Tooltip,
} from "dremio-ui-lib/components";
import DownloadMenu from "#oss/components/Menus/ExplorePage/DownloadMenu";
import { useAsyncDownload } from "#oss/exports/providers/useAsyncDownload";
import { getAllJobDetails } from "#oss/selectors/exploreJobs";

export type DownloadButtonProps = {
  dataset: Immutable.Map<string, any>;
  datasetColumns: string[];
  isDownloading: boolean;
  disableOptions?: boolean;
  setIsDownloading: (isDownloading: boolean) => void;
};

const { t } = getIntlContext();

function DownloadButton({
  dataset,
  datasetColumns,
  isDownloading,
  disableOptions,
  setIsDownloading,
}: DownloadButtonProps) {
  const jobDetails = useSelector(
    (state) => getAllJobDetails(state)[dataset.get("jobId")],
  );

  const handleDownload = useAsyncDownload({
    jobId: dataset.get("jobId"),
    setIsDownloading,
  });

  return isDownloading ? (
    <Tooltip
      content={t("Sonar.SqlRunner.Download.Preparing")}
      placement="bottom-start"
      portal
    >
      <Spinner className="download-spinner" />
    </Tooltip>
  ) : (
    <Popover
      content={(opts) => (
        <DownloadMenu
          datasetColumns={datasetColumns}
          disableOptions={disableOptions}
          jobDetails={jobDetails}
          onDownload={handleDownload}
          {...opts}
        />
      )}
      mode="click"
      role="menu"
      placement="bottom-start"
      dismissable
    >
      <IconButton
        tooltip={t("Common.Download")}
        className="download-dataset"
        disabled={!dataset.get("datasetType")}
      >
        <dremio-icon name="sql-editor/download" alt="download dataset" />
        <dremio-icon
          name="interface/down-chevron"
          alt=""
          style={{ width: 12 }}
        />
      </IconButton>
    </Popover>
  );
}

export default DownloadButton;
