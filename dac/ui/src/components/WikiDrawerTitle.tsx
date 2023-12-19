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

import { useCallback } from "react";
import { useIntl } from "react-intl";
import Immutable from "immutable";
import { IconButton } from "dremio-ui-lib/components";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import { getIconPath } from "@app/utils/getIconPath";
import { getIconType } from "@app/components/DatasetSummary/datasetSummaryUtils";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { PHYSICAL_DATASET } from "@app/constants/datasetTypes";

type WikiDrawerTitleProps = {
  datasetDetails: Immutable.Map<string, any> | undefined;
  fullPath: Immutable.List<string> | undefined;
  closeWikiDrawer: (e: any) => void;
};

function WikiDrawerTitle({
  datasetDetails,
  fullPath,
  closeWikiDrawer,
}: WikiDrawerTitleProps) {
  const { formatMessage } = useIntl();

  const versionContext = getVersionContextFromId(
    datasetDetails?.get("entityId")
  );

  const iconName = getIconType(
    datasetDetails?.get("datasetType"),
    versionContext
  );

  const openDatasetInNewTab = useCallback(() => {
    const queryLink = datasetDetails.getIn(["links", "query"]);
    const editLink = datasetDetails.getIn(["links", "edit"]);
    const canAlter = datasetDetails.getIn(["permissions", "canAlter"]);
    const canSelect = datasetDetails.getIn(["permissions", "canSelect"]);
    const fullPath = datasetDetails.get("fullPath");
    const toLink = (canAlter || canSelect) && editLink ? editLink : queryLink;
    const urldetails = new URL(window.location.origin + toLink);
    let pathname = urldetails.pathname + "/wiki" + urldetails.search;

    const { type, value } = versionContext ?? {};

    if (type && value) {
      if (pathname.includes("mode=edit")) {
        pathname += `&refType=${type}&refValue=${value}`;
      } else if (datasetDetails.get("datasetType") === PHYSICAL_DATASET) {
        pathname += `?refType=${type}&refValue=${value}&sourceName=${fullPath.get(
          0
        )}`;
      }
    }

    window.open(wrapBackendLink(pathname), "_blank");
  }, [datasetDetails, versionContext]);

  return (
    <div className="wikiOverlayTitle">
      <div className="wikiOverlayTitle__info">
        {iconName && (
          <dremio-icon
            name={`entities/${iconName}`}
            class="wikiOverlayTitle__info-datasetIcon"
          />
        )}
        <div className="wikiOverlayTitle__info-name">{fullPath?.last()}</div>
        {datasetDetails?.get("hasReflection") && (
          <img
            src={getIconPath("interface/reflections-outlined")}
            className="wikiOverlayTitle__info-reflection"
          />
        )}
      </div>
      {/* used to prevent auto-focus on <IconButton> when opening the panel */}
      <button className="wikiOverlayTitle__hidden-button" />
      <div className="wikiOverlayTitle__actions">
        {datasetDetails && (
          <IconButton
            tooltip={formatMessage({ id: "Wiki.OpenNewTab" })}
            onClick={openDatasetInNewTab}
            className="wikiOverlayTitle__actions-button"
          >
            <dremio-icon name="interface/external-link" />
          </IconButton>
        )}
        <IconButton
          tooltip={formatMessage({ id: "Common.Close" })}
          onClick={closeWikiDrawer}
          className="wikiOverlayTitle__actions-button"
          id="close-wiki-drawer"
        >
          <dremio-icon name="interface/close-big" />
        </IconButton>
      </div>
    </div>
  );
}

export default WikiDrawerTitle;
