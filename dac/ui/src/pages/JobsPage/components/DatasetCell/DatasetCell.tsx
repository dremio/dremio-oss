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

import { useCallback, useMemo } from "react";
import Immutable from "immutable";
import { checkTypeToShowOverlay } from "@app/utils/datasetUtils";
import { getIconByEntityType } from "@app/utils/iconUtils";
// @ts-ignore
import VersionContext from "dremio-ui-common/components/VersionContext.js";
import { Popover } from "dremio-ui-lib/components";
import { Tooltip as DremioTooltip } from "dremio-ui-lib";
import DatasetSummaryOverlay from "@app/components/Dataset/DatasetSummaryOverlay";

import * as classes from "./DatasetCell.module.less";

type DatasetCellProps = {
  job: any;
};

const getVersionContext = (versionContextObj: any) => {
  try {
    return JSON.parse(versionContextObj);
  } catch (e) {
    return;
  }
};

export const DatasetCell = ({ job }: DatasetCellProps) => {
  const {
    isInternalQuery,
    datasetType,
    datasetName,
    versionContext,
    datasetArray,
    hasSummary,
  } = useMemo(() => {
    const datasetArray = job.queriedDatasets;
    const firstDataset = job.queriedDatasets?.[0] || {};
    const datasetType = firstDataset.datasetType;
    const versionContextObj = firstDataset.versionContext;

    return {
      isInternalQuery: job?.queryType === "UI_INITIAL_PREVIEW",
      datasetName: firstDataset.datasetName,
      hasSummary: checkTypeToShowOverlay(datasetType) || !!versionContextObj,
      datasetType,
      versionContext: getVersionContext(versionContextObj),
      datasetArray,
    };
  }, [job]);

  const datasetPathList = job.queriedDatasets?.[0]?.datasetPathsList;
  const datasetArrayImmutable = useMemo(
    () => Immutable.fromJS(datasetPathList),
    [datasetPathList],
  );

  const renderDatasetIconAndName = useCallback(() => {
    return (
      <div className={classes["datasetCell"]}>
        <span className={classes["datasetCell__icon"]}>
          <div className="shrink-0">
            <dremio-icon
              style={{
                inlineSize: 24,
                blockSize: 24,
              }}
              name={getIconByEntityType(
                !isInternalQuery ? datasetType : "OTHER",
                !!versionContext,
              )}
            ></dremio-icon>
          </div>
        </span>
        <div className={classes["datasetCell__name"]}>
          {datasetName}
          {versionContext && (
            <span className={classes["datasetCell__versionContext"]}>
              {"("}
              <VersionContext versionContext={versionContext} />
              {")"}
            </span>
          )}
        </div>
      </div>
    );
  }, [datasetName, datasetType, isInternalQuery, versionContext]);

  if (!datasetArray) {
    return null;
  }

  if (hasSummary) {
    return (
      <Popover
        role="tooltip"
        placement="right"
        delay={750}
        mode="hover"
        showArrow
        portal
        content={
          <DatasetSummaryOverlay
            inheritedTitle={datasetName}
            datasetType={datasetType}
            fullPath={datasetArrayImmutable}
            openWikiDrawer={() => {}}
            showColumns
            versionContext={versionContext}
            hideMainActionButtons
          />
        }
      >
        {renderDatasetIconAndName()}
      </Popover>
    );
  } else {
    return (
      <DremioTooltip
        interactive
        type="richTooltip"
        enterDelay={1000}
        title={
          <div className={classes["datasetCell__wrapper"]}>
            {renderDatasetIconAndName()}
          </div>
        }
      >
        {renderDatasetIconAndName()}
      </DremioTooltip>
    );
  }
};
