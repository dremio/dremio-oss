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
import { ENTITY_TYPES } from "#oss/constants/Constants";
import { PureEntityIcon } from "#oss/pages/HomePage/components/EntityIcon";
import {
  isEntityWikiEditAllowed,
  getEntityTypeFromObject,
} from "dyn-load/utils/entity-utils";
import { useEntityPanelDetails } from "./utils";
import { CollapsiblePanelWrapper } from "../CollapsiblePanelWrapper/CollapsiblePanelWrapper";

import * as classes from "../CollapsiblePanelWrapper/CollapsiblePanelWrapper.module.less";

type EntityDetailsPanelProps = {
  entityDetailsCollapsed: boolean;
  handleEntityDetailsCollapse: () => void;
  handleEntityDetails: any;
  entityDetails: Immutable.Map<any, any>;
};

export const EntityDetailsPanel = (props: EntityDetailsPanelProps) => {
  const { entityDetails } = props;

  const entityStatus = useEntityPanelDetails(
    entityDetails,
    props.handleEntityDetails,
  );
  const isLoadingDetails = entityStatus === "pending";

  const title = entityDetails
    ? entityDetails.get("displayFullPath")?.last() ||
      entityDetails.get("fullPath")?.last() ||
      entityDetails.get("name")
    : "Entity Details";
  const versionContext = getVersionContextFromId(entityDetails.get("entityId"));
  const iconEntity =
    getIconType(getEntityTypeFromObject(entityDetails), !!versionContext) || "";
  const iconName =
    iconEntity && entityDetails ? `entities/${iconEntity}` : "interface/meta";
  const isSource =
    getEntityTypeFromObject(entityDetails) === ENTITY_TYPES.source;

  return (
    <CollapsiblePanelWrapper
      panelContent={
        <Wiki
          entityId={entityDetails.get("entityId") || entityDetails.get("id")}
          isEditAllowed={isEntityWikiEditAllowed(entityDetails)}
          className="bottomContent"
          dataset={entityDetails}
          hideSqlEditorIcon={exploreUtils.isSqlEditorTab(window.location)}
          isPanel
          isLoadingDetails={isLoadingDetails}
        />
      }
      headerIcon={
        isLoadingDetails ? (
          <Skeleton width="4ch" />
        ) : isSource ? (
          <PureEntityIcon
            disableHoverListener
            entityType={entityDetails.get("entityType")}
            sourceStatus={entityDetails.getIn(["state", "status"], null)}
            sourceType={entityDetails.get("type")}
            style={{ width: 26, height: 26 }}
          />
        ) : (
          <dremio-icon
            key={iconName}
            name={iconName}
            class={classes["entity-details-panel__icon"]}
          />
        )
      }
      headerTitle={
        isLoadingDetails ? (
          <Skeleton width="18ch" />
        ) : (
          <div className="text-semibold text-ellipsis pr-1">{title}</div>
        )
      }
      handleDatasetDetailsCollapse={props.handleEntityDetailsCollapse}
      datasetDetailsCollapsed={props.entityDetailsCollapsed}
    />
  );
};
