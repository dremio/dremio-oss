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

import { VIRTUAL_DATASET } from "#oss/constants/datasetTypes";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import {
  constructLinkWithRefInfo,
  constructSummaryBaseLink,
} from "./pathUtils";
import { Button } from "dremio-ui-lib/components";
import LinkWithRef from "#oss/components/LinkWithRef/LinkWithRef";
import { intl } from "./intl";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";

export const getSummaryActions = ({
  dataset,
  shouldRenderLineageButton,
  hideGoToButton,
  classes,
}: any) => {
  const selfLink = dataset.getIn(["links", "query"]);
  const editLink = dataset.getIn(["links", "edit"]);
  const resourceId = dataset.getIn(["fullPath", 0]);
  const isView = dataset.get("datasetType") === VIRTUAL_DATASET;
  const versionContext = getVersionContextFromId(dataset.get("entityId"));

  const versionedEditLink = constructLinkWithRefInfo(editLink, versionContext);
  const versionedSelfLink = constructLinkWithRefInfo(
    selfLink,
    versionContext,
    resourceId,
  );
  const versionedDetailsLink = constructLinkWithRefInfo(
    constructSummaryBaseLink(isView, "wiki", selfLink, editLink),
    versionContext,
    resourceId,
  );
  const versionedLineageLink = constructLinkWithRefInfo(
    constructSummaryBaseLink(isView, "graph", selfLink, editLink),
    versionContext,
    resourceId,
  );
  const versionedHistoryLink = constructLinkWithRefInfo(
    constructSummaryBaseLink(isView, "history", selfLink, editLink),
    versionContext,
    resourceId,
  );

  const toLink = versionedEditLink ? versionedEditLink : versionedSelfLink;
  return (
    <>
      {!hideGoToButton ? (
        isView ? (
          <Button
            as={LinkWithRef}
            to={toLink ? wrapBackendLink(toLink) : ""}
            tooltip={intl.formatMessage({ id: "Edit.Dataset" })}
            variant="secondary"
            className={classes["summary-actions__button"]}
          >
            <dremio-icon
              class={classes["dataset-item-header-action-icon"]}
              name="interface/edit"
            />
            {intl.formatMessage({ id: "Common.Edit" })}
          </Button>
        ) : (
          <Button
            as={LinkWithRef}
            to={toLink ? wrapBackendLink(toLink) : ""}
            tooltip={intl.formatMessage({
              id: "Go.To.Table",
            })}
            variant="secondary"
            className={classes["summary-actions__button"]}
          >
            <dremio-icon
              class={classes["dataset-item-header-action-icon"]}
              name="navigation-bar/go-to-dataset"
            />
            {intl.formatMessage({ id: "Go.To" })}
          </Button>
        )
      ) : null}
      <Button
        as={LinkWithRef}
        to={versionedDetailsLink ? wrapBackendLink(versionedDetailsLink) : ""}
        tooltip={intl.formatMessage({ id: "Open.Details" })}
        variant="secondary"
        className={classes["summary-actions__button"]}
      >
        <dremio-icon
          alt={intl.formatMessage({ id: "Open.Details" })}
          name="interface/meta"
        />
      </Button>
      {shouldRenderLineageButton && (
        <Button
          as={LinkWithRef}
          to={versionedLineageLink ? wrapBackendLink(versionedLineageLink) : ""}
          tooltip={intl.formatMessage({ id: "Open.Lineage" })}
          variant="secondary"
          className={classes["summary-actions__button"]}
        >
          <dremio-icon
            alt={intl.formatMessage({ id: "Open.Lineage" })}
            name="sql-editor/graph"
          />
        </Button>
      )}
      {!!versionContext && (
        <Button
          as={LinkWithRef}
          to={versionedHistoryLink ? wrapBackendLink(versionedHistoryLink) : ""}
          tooltip={intl.formatMessage({ id: "Open.History" })}
          variant="secondary"
          className={classes["summary-actions__button"]}
        >
          <dremio-icon
            alt={intl.formatMessage({ id: "Open.History" })}
            name="interface/history"
          />
        </Button>
      )}
    </>
  );
};

export const getExtraSummaryPanelIcon = (...args: any) => {
  return null;
};
