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
import { Skeleton } from "dremio-ui-lib/components";
import { Wiki } from "../Wiki/Wiki";
import { getIconType } from "#oss/components/DatasetSummary/datasetSummaryUtils";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import exploreUtils from "#oss/utils/explore/exploreUtils";
import {
  isEntityWikiEditAllowed,
  getEntityTypeFromObject,
} from "dyn-load/utils/entity-utils";
import { CollapsiblePanelWrapper } from "../CollapsiblePanelWrapper/CollapsiblePanelWrapper";
import { getExtraSummaryPanelIcon } from "dyn-load/utils/summary-utils";

import * as classes from "../CollapsiblePanelWrapper/CollapsiblePanelWrapper.module.less";

type DatasetDetailsPanelProps = {
  datasetDetailsCollapsed: boolean;
  handleDatasetDetailsCollapse: () => void;
  handleDatasetDetails: any;
  datasetDetails: Immutable.Map<any, any>;
  hideGoToButton?: boolean;
};

export const DatasetDetailsPanel = (props: DatasetDetailsPanelProps) => {
  const { datasetDetails, hideGoToButton } = props;
  const title =
    datasetDetails.get("displayFullPath")?.last() ||
    datasetDetails.get("fullPath")?.last() ||
    datasetDetails.get("name");
  const versionContext = getVersionContextFromId(
    datasetDetails.get("entityId"),
  );
  const iconEntity =
    getIconType(getEntityTypeFromObject(datasetDetails), !!versionContext) ||
    "";
  const iconName =
    iconEntity && datasetDetails ? `entities/${iconEntity}` : "interface/meta";
  const isError = datasetDetails.get("error");
  const isLoadingDetails = !!datasetDetails.get("fromTreeNode") && !isError;

  return (
    <CollapsiblePanelWrapper
      panelContent={
        <Wiki
          entityId={datasetDetails.get("entityId") || datasetDetails.get("id")}
          isEditAllowed={isEntityWikiEditAllowed(datasetDetails)}
          className="bottomContent"
          dataset={datasetDetails}
          hideSqlEditorIcon={exploreUtils.isSqlEditorTab(window.location)}
          hideGoToButton={hideGoToButton}
          isPanel
          isLoadingDetails={isLoadingDetails}
          handlePanelDetails={props.handleDatasetDetails}
        />
      }
      headerIcon={
        isLoadingDetails ? (
          <Skeleton width="4ch" />
        ) : (
          <dremio-icon
            key={iconName}
            name={iconName}
            class={classes["dataset-details-panel__icon"]}
          />
        )
      }
      headerTitle={
        isLoadingDetails ? (
          <Skeleton width="18ch" />
        ) : (
          <div className="text-semibold text-ellipsis pr-1 flex items-center">
            <span className="text-ellipsis">{title}</span>
            {getExtraSummaryPanelIcon(datasetDetails, { marginLeft: 4 })}
          </div>
        )
      }
      handleDatasetDetailsCollapse={props.handleDatasetDetailsCollapse}
      datasetDetailsCollapsed={props.datasetDetailsCollapsed}
    />
  );
};
