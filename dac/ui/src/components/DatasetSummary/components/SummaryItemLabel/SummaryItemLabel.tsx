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
import { getIconType, addTooltip } from "../../datasetSummaryUtils";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
// @ts-ignore
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import { getIconPath } from "@app/utils/getIconPath";
import QueryDataset from "@app/components/QueryDataset/QueryDataset";

import * as classes from "./SummaryItemLabel.module.less";

type DatasetSummaryItemLabelProps = {
  disableActionButtons: boolean;
  datasetType: string;
  title: string;
  canAlter: boolean;
  resourceId: string;
  fullPath: string;
  hasReflection: boolean;
  editLink?: string;
  selfLink?: string;
  hideSqlEditorIcon?: boolean;
};

const SummaryItemLabel = (props: DatasetSummaryItemLabelProps) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const {
    title,
    canAlter,
    editLink,
    selfLink,
    resourceId,
    datasetType,
    fullPath,
    disableActionButtons,
    hideSqlEditorIcon,
    hasReflection,
  } = props;

  const titleRef = useRef(null);
  const iconName = getIconType(datasetType);
  const disable = disableActionButtons
    ? classes["dataset-item-header-disable-action-buttons"]
    : "";

  const toLink = canAlter && editLink ? editLink : selfLink;
  const isView = iconName && iconName === "dataset-view";
  return (
    <div className={classes["dataset-item-header-container"]}>
      <div className={classes["dataset-item-header"]}>
        {iconName ? (
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
          <QueryDataset
            resourceId={resourceId}
            fullPath={fullPath}
            className={classes["dataset-item-header-action-icon"]}
          />
        )}

        {iconName &&
          (isView ? (
            <IconButton
              as={LinkWithRef}
              to={toLink ? wrapBackendLink(toLink) : ""}
              tooltip="Common.Edit"
            >
              <dremio-icon
                class={classes["dataset-item-header-action-icon"]}
                name="interface/edit"
              />
            </IconButton>
          ) : (
            <IconButton
              as={LinkWithHref}
              to={selfLink ? wrapBackendLink(selfLink) : ""}
              tooltip="Go.To.Table"
            >
              <dremio-icon
                class={classes["dataset-item-header-action-icon"]}
                name="navigation-bar/go-to-dataset"
              />
            </IconButton>
          ))}
      </div>
    </div>
  );
};

export default SummaryItemLabel;
