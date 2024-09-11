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

import { PureEntityIcon } from "../EntityIcon";
import { getIconType } from "@app/components/DatasetSummary/datasetSummaryUtils";
import {
  getEntityTypeFromObject,
  isEntityWikiEditAllowed,
} from "dyn-load/utils/entity-utils";
import { IconButton } from "dremio-ui-lib/components";
import { Wiki } from "@app/pages/ExplorePage/components/Wiki/Wiki";
import { ENTITY_TYPES } from "@app/constants/Constants";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { getExtraSummaryPanelIcon } from "dyn-load/utils/summary-utils";
import { isNotSoftware } from "@app/utils/versionUtils";

import * as classes from "./CatalogDetailsPanel.module.less";

type CatalogDetailsPanelProps = {
  panelItem: Immutable.Map<any, any>;
  handleDatasetDetailsCollapse: () => void;
  handlePanelDetails: (dataset: any) => void;
};

const CatalogDetailsPanel = ({
  panelItem,
  handleDatasetDetailsCollapse,
  handlePanelDetails,
}: CatalogDetailsPanelProps) => {
  const panelIsSource = panelItem?.get("entityType") === ENTITY_TYPES.source;
  const panelName = panelItem?.get("name") || panelItem?.get("fullPath").last();
  const versionContext = getVersionContextFromId(
    panelItem?.get("entityId") || panelItem?.get("id"),
  );
  return (
    <section
      role="contentinfo"
      aria-label={`Dataset details: ${panelName}`}
      className={classes["catalog-details-panel"]}
    >
      <div className={classes["catalog-details-panel__header"]}>
        {panelIsSource ? (
          <PureEntityIcon
            disableHoverListener
            entityType={panelItem.get("entityType")}
            sourceStatus={panelItem.getIn(["state", "status"], null)}
            sourceType={panelItem?.get("type")}
            style={{ width: 24, height: 24 }}
          />
        ) : (
          <span className="mr-05">
            <dremio-icon
              name={`entities/${getIconType(
                getEntityTypeFromObject(panelItem),
                !!versionContext,
              )}`}
              key={panelName} // <use> href doesn't always update
              style={{ width: 24, height: 24 }}
            />
          </span>
        )}
        <div className={classes["catalog-details-panel__header-content"]}>
          <div className="text-ellipsis pr-1 flex items-center">
            <div className="text-ellipsis">{panelName}</div>
            {getExtraSummaryPanelIcon(panelItem, { marginLeft: 4 })}
          </div>
          <IconButton
            tooltip="Close details panel"
            onClick={handleDatasetDetailsCollapse}
            className={classes["catalog-details-panel__action-icon"]}
          >
            <dremio-icon name="interface/close-big" alt="close" />
          </IconButton>
        </div>
      </div>
      <div
        style={{
          height: `calc(100vh - 48px - ${isNotSoftware() ? 40 : 64}px)`,
          overflow: "hidden",
        }}
      >
        <Wiki
          entityId={panelItem.get("entityId") || panelItem.get("id")}
          isEditAllowed={isEntityWikiEditAllowed(panelItem)}
          className="bottomContent"
          dataset={panelItem}
          handlePanelDetails={handlePanelDetails}
          isPanel
        />
      </div>
    </section>
  );
};

export default CatalogDetailsPanel;
