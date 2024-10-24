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

import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { Tooltip } from "dremio-ui-lib/components";
import { LIST, MAP, MIXED, STRUCT } from "#oss/constants/DataTypes";
import { StartDatasetDownloadParams } from "#oss/exports/endpoints/Datasets/startDatasetDownload";
import { JobDetails } from "#oss/exports/types/JobDetails.type";
import clsx from "clsx";
import config from "dyn-load/utils/config";

const { t } = getIntlContext();

const UNSUPPORTED_TYPE_COLUMNS: Partial<{
  [key in StartDatasetDownloadParams["format"]]: Set<string>;
}> = {
  CSV: new Set([MAP, LIST, MIXED, STRUCT]),
};

function DownloadMenu({
  datasetColumns,
  disableOptions,
  jobDetails,
  close,
  onDownload,
}: {
  datasetColumns: string[];
  disableOptions?: boolean;
  jobDetails: JobDetails | undefined;
  close: () => void;
  onDownload: (format: StartDatasetDownloadParams["format"]) => void;
}) {
  const configDisabled = !config.downloadRecordsLimit;

  const isRun = jobDetails?.queryType === "UI_RUN";
  const isOutputLimited = jobDetails?.isOutputLimited;

  const menuHeader = (
    <>
      {!isRun
        ? t("Sonar.SqlRunner.Download.Menu.Sample")
        : isOutputLimited
          ? t("Sonar.SqlRunner.Download.Menu.Limited")
          : t("Sonar.SqlRunner.Download.Menu.Label")}
      {(!isRun || isOutputLimited) && (
        <Tooltip
          content={
            !isRun
              ? t("Sonar.SqlRunner.Download.Menu.SampleHelper")
              : t("Sonar.SqlRunner.Download.Menu.LimitedHelper", {
                  rows: jobDetails.outputRecords.toLocaleString(),
                })
          }
          placement="top"
          portal
        >
          <dremio-icon name="interface/information" />
        </Tooltip>
      )}
    </>
  );

  return (
    <div className="flex flex-col gap-05 py-05 drop-shadow-lg bg-popover rounded">
      <span className="flex items-center gap-05 h-4 px-1 text-semibold">
        {menuHeader}
      </span>
      <div className="flex flex-col disabled">
        {(["JSON", "CSV", "PARQUET"] as const).map((format) => {
          const hasUnsupportedTypes = datasetColumns.some((column) =>
            UNSUPPORTED_TYPE_COLUMNS[format]?.has(column),
          );

          const isDisabled =
            disableOptions || configDisabled || hasUnsupportedTypes;

          return (
            <button
              onClick={() => {
                onDownload(format);
                close();
              }}
              className={clsx(
                "flex items-center h-4 px-1 border-none",
                isDisabled
                  ? "not-allowed text-disabled bg-popover"
                  : "bg-popover hover",
              )}
              disabled={isDisabled}
              key={format}
            >
              {format === "PARQUET"
                ? format.charAt(0) + format.slice(1).toLowerCase()
                : format}
            </button>
          );
        })}
      </div>
    </div>
  );
}

export default DownloadMenu;
