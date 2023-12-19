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

import { useRef, useState } from "react";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";
import { getIconType, addTooltip } from "../../datasetSummaryUtils";
import { getIconPath } from "@app/utils/getIconPath";
import { VersionContextType } from "dremio-ui-common/components/VersionContext.js";
import QueryDataset from "@app/components/QueryDataset/QueryDataset";
import SummaryAction from "@app/components/DatasetSummary/components/SummaryAction/SummaryAction";

import * as classes from "./SummaryItemLabel.module.less";

type DatasetSummaryItemLabelProps = {
  disableActionButtons: boolean;
  datasetType: string;
  title: string;
  canAlter: boolean;
  canSelect: boolean;
  resourceId: string;
  fullPath: string;
  hasReflection: boolean;
  editLink?: string;
  queryLink?: string;
  hideSqlEditorIcon?: boolean;
  versionContext?: VersionContextType;
};

const SummaryItemLabel = (props: DatasetSummaryItemLabelProps) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const {
    title,
    canAlter,
    canSelect,
    editLink,
    queryLink,
    resourceId,
    datasetType,
    fullPath,
    disableActionButtons,
    hideSqlEditorIcon,
    hasReflection,
    versionContext,
  } = props;

  const titleRef = useRef(null);
  const iconName = getIconType(datasetType, !!versionContext) ?? "";
  const disable = disableActionButtons
    ? classes["dataset-item-header-disable-action-buttons"]
    : "";

  const isView = ["dataset-view", "iceberg-view"].includes(iconName);

  return (
    <div className={classes["dataset-item-header-container"]}>
      <div className={classes["dataset-item-header"]}>
        {iconName ? (
          <dremio-icon
            class={classes["dataset-item-header-icon"]}
            name={`entities/${iconName}`}
            style={{ width: 26, height: 26 }}
          />
        ) : (
          <div className={classes["dataset-item-header-empty-icon"]}></div>
        )}

        {showTooltip ? (
          <Tooltip interactive placement="top" title={title}>
            <p className={classes["dataset-item-header-title"]}>{title}</p>
          </Tooltip>
        ) : (
          <p
            ref={titleRef}
            onMouseEnter={() => addTooltip(titleRef, setShowTooltip)}
            className={classes["dataset-item-header-title"]}
          >
            {title}
          </p>
        )}
        {hasReflection && (
          <img
            className={classes["dataset-item-header-reflection"]}
            src={getIconPath("interface/reflections-outlined")}
          />
        )}
      </div>

      <div
        className={`${classes["dataset-item-header-action-container"]} ${disable}`}
      >
        {!hideSqlEditorIcon && (
          <span className="mx-1">
            <QueryDataset
              resourceId={resourceId}
              fullPath={fullPath}
              tooltipPortal
              className={classes["dataset-item-header-action-icon"]}
              versionContext={versionContext}
            />
          </span>
        )}
        {iconName && (
          <SummaryAction
            canAlter={canAlter}
            canSelect={canSelect}
            isView={isView}
            editLink={editLink}
            queryLink={queryLink}
            resourceId={resourceId}
            versionContext={versionContext ?? ({} as VersionContextType)}
          />
        )}
      </div>
    </div>
  );
};

export default SummaryItemLabel;
