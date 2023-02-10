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
import { Tooltip, IconButton } from "dremio-ui-lib";
import LinkWithHref from "@app/components/LinkWithRef/LinkWithRef";
import {
  getIconType,
  addTooltip,
  openInNewTab,
} from "../../datasetSummaryUtils";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";

import * as classes from "./SummaryItemLabel.module.less";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";

const DATASET_PATH_FROM_OVERLAY = "datasetPathFromOverlay";

type DatasetSummaryItemLabelProps = {
  disableActionButtons: boolean;
  datasetType: string;
  title: string;
  editLink?: string;
  selfLink?: string;
  canAlter: boolean;
  resourceId: string;
  fullPath: string;
  isSqlEditorTab: boolean;
};

const SummaryItemLabel = (props: DatasetSummaryItemLabelProps) => {
  const projectId = getSonarContext()?.getSelectedProjectId?.();
  const [showTooltip, setShowTooltip] = useState(false);
  const {
    title,
    canAlter,
    editLink,
    selfLink,
    isSqlEditorTab,
    resourceId,
    datasetType,
    fullPath,
    disableActionButtons,
  } = props;

  const newQueryLink = sqlPaths.sqlEditor.link({ projectId });
  const titleRef = useRef(null);
  const iconName = getIconType(datasetType);
  const newQueryUrlParams = "?context=" + encodeURIComponent(resourceId);
  const newTabLink = sqlPaths.newQuery.link({ projectId, resourceId });
  const disable = disableActionButtons
    ? classes["dataset-item-header-disable-action-buttons"]
    : "";

  const queryButtonProps = isSqlEditorTab
    ? {
        onClick: () =>
          openInNewTab(newTabLink, fullPath, DATASET_PATH_FROM_OVERLAY),
      }
    : {
        as: LinkWithHref,
        onClick: () =>
          sessionStorage.setItem(DATASET_PATH_FROM_OVERLAY, fullPath),
        to: {
          pathname: newQueryLink,
          search: newQueryUrlParams,
        },
      };

  const toLink = canAlter && editLink ? editLink : selfLink;

  return (
    <div className={classes["dataset-item-header-container"]}>
      <div className={classes["dataset-item-header"]}>
        {iconName ? (
          // @ts-ignore
          <dremio-icon
            class={classes["dataset-item-header-icon"]}
            name={`entities/${iconName}`}
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
      </div>

      <div
        className={`${classes["dataset-item-header-action-container"]} ${disable}`}
      >
        <IconButton {...queryButtonProps} tooltip="Query.Dataset">
          <dremio-icon
            class={classes["dataset-item-header-action-icon"]}
            name="navigation-bar/sql-runner"
          />
        </IconButton>

        <IconButton
          as={LinkWithRef}
          to={toLink ? wrapBackendLink(toLink) : ""}
          tooltip="Go.To.Dataset"
        >
          <dremio-icon
            class={classes["dataset-item-header-action-icon"]}
            name="navigation-bar/go-to-dataset"
          />
        </IconButton>
      </div>
    </div>
  );
};

export default SummaryItemLabel;
