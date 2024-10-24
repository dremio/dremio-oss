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

import { IconButton } from "dremio-ui-lib/components";
import { browserHistory } from "react-router";
import { isNotSoftware } from "dyn-load/utils/versionUtils";
import { useMultiTabIsEnabled } from "#oss/components/SQLScripts/useMultiTabIsEnabled";
import { isTabbableUrl } from "#oss/utils/explorePageTypeUtils";
import * as classes from "./CollapsiblePanelWrapper.module.less";

type CollapsiblePanelWrapperProps = {
  headerTitle: string | React.ReactNode;
  panelContent: React.ReactNode;
  headerIcon: React.ReactNode;
  datasetDetailsCollapsed: boolean;
  handleDatasetDetailsCollapse: () => void;
};

export const CollapsiblePanelWrapper = (
  props: CollapsiblePanelWrapperProps,
) => {
  const isOpen = !props.datasetDetailsCollapsed;
  const isMultiTabsEnabled = useMultiTabIsEnabled();
  const location = browserHistory.getCurrentLocation();
  return (
    <div
      className={classes["collapsible-details-panel"]}
      style={{
        width: isOpen ? 320 : 36,
      }}
    >
      {isOpen ? (
        <div className={classes["collapsible-details-panel__header-open"]}>
          {props.headerIcon}
          <div className={classes["collapsible-details-panel__header-right"]}>
            <div className="text-semibold text-ellipsis pr-1">
              {props.headerTitle}
            </div>
            <IconButton
              tooltip="Collapse details panel"
              onClick={props.handleDatasetDetailsCollapse}
              className={classes["collapsible-details-panel__action-icon"]}
            >
              <dremio-icon name="scripts/CollapseRight" alt="collapse" />
            </IconButton>
          </div>
        </div>
      ) : (
        <div className={classes["collapsible-details-panel__header-closed"]}>
          <IconButton
            tooltip={"Open details panel"}
            onClick={props.handleDatasetDetailsCollapse}
            className={classes["collapsible-details-panel__action-icon"]}
          >
            <dremio-icon name="interface/meta" alt="meta" />
          </IconButton>
        </div>
      )}

      {isOpen ? (
        <div
          className={classes["collapsible-details-panel__content"]}
          style={{
            height: `calc(100vh - ${
              isMultiTabsEnabled && isTabbableUrl(location) ? "0" : "65"
            }px - 40px - ${isNotSoftware() ? 39 : 60}px)`,
          }}
        >
          {props.panelContent}
        </div>
      ) : null}
    </div>
  );
};
