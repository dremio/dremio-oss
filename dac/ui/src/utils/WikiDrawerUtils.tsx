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
// @ts-ignore
import { IconButton } from "dremio-ui-lib/components";
// @ts-ignore
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import { getIconPath } from "@app/utils/getIconPath";
import { getIconType } from "@app/components/DatasetSummary/datasetSummaryUtils";
import { intl } from "@app/utils/intl";

const openDatasetInNewTab = (datasetDetails: Immutable.Map<string, any>) => {
  const selfLink = datasetDetails.getIn(["links", "query"]);
  const editLink = datasetDetails.getIn(["links", "edit"]);
  const canAlter = datasetDetails.getIn(["permissions", "canAlter"]);
  const toLink = canAlter && editLink ? editLink : selfLink;
  const urldetails = new URL(window.location.origin + toLink);
  const pathname = urldetails.pathname + "/wiki" + urldetails.search;
  window.open(wrapBackendLink(pathname), "_blank");
};

export const getCommonWikiDrawerTitle = (
  datasetDetails: Immutable.Map<string, any> | undefined,
  fullPath: Immutable.List<string> | undefined,
  closeWikiDrawer: (e: any) => void
) => {
  const { formatMessage } = intl;
  const iconName = getIconType(datasetDetails?.get("datasetType"));

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
            onClick={() => openDatasetInNewTab(datasetDetails)}
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
};
