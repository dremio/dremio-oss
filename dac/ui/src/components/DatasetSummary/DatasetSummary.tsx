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

import { Link } from "react-router";
import { useIntl } from "react-intl";
import Immutable from "immutable";
import SummaryItemLabel from "./components/SummaryItemLabel/SummaryItemLabel";
import SummarySubHeader from "./components/SummarySubHeader/SummarySubHeader";
import SummaryStats from "./components/SummaryStats/SummaryStats";
import SummaryColumns from "./components/SummaryColumns/SummaryColumns";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import * as classes from "./DatasetSummary.module.less";
import clsx from "clsx";
import { ReactNode } from "react";
// @ts-ignore
import { TagList } from "dremio-ui-lib";
import CopyButton from "@app/components/Buttons/CopyButton";
import { VersionContextType } from "dremio-ui-common/components/VersionContext.js";
import { getEdition } from "@inject/utils/versionUtils";

type DatasetSummaryProps = {
  title: string;
  dataset: Immutable.Map<string, any>;
  fullPath: string;
  disableActionButtons: boolean;
  location: Record<string, any>;
  detailsView?: boolean;
  tagsComponent?: ReactNode;
  openWikiDrawer: (dataset: any) => void;
  showColumns?: boolean;
  hideSqlEditorIcon?: boolean;
  versionContext?: VersionContextType;
};

const DatasetSummary = ({
  dataset,
  fullPath,
  title,
  disableActionButtons,
  location,
  detailsView,
  tagsComponent,
  openWikiDrawer,
  showColumns,
  hideSqlEditorIcon,
  versionContext,
}: DatasetSummaryProps) => {
  const { formatMessage } = useIntl();
  const datasetType = dataset.get("datasetType");
  const createdAt = dataset.get("createdAt");
  const hasReflection = dataset.get("hasReflection");
  const ownerEmail = dataset.get("ownerName");
  const lastModifyUserEmail = dataset.get("lastModifyingUserEmail");
  const lastModified = dataset.get("lastModified");
  const selfLink = dataset.getIn(["links", "query"]);
  const editLink = dataset.getIn(["links", "edit"]);
  const jobsLink = wrapBackendLink(dataset.getIn(["links", "jobs"]));
  const jobCount = dataset.get("jobCount");
  const descendantsCount = dataset.get("descendants");
  const fields = dataset.get("fields");
  const canAlter = dataset.getIn(["permissions", "canAlter"]);
  const fieldsCount = fields && fields.size;
  const resourceId = dataset.getIn(["fullPath", 0]);
  const tags = dataset.get("tags");
  const canSeeDatasetGraph = dataset.getIn([
    "permissions",
    "canExploreDatasetGraph",
  ]);

  const shouldRenderLineageButton =
    canSeeDatasetGraph && getEdition() !== "Community Edition";

  return (
    <div
      onClick={(e) => e.stopPropagation()}
      className={
        detailsView
          ? classes["dataset-summary-wiki-container"]
          : classes["dataset-summary-container"]
      }
    >
      <div
        className={clsx(
          !detailsView && classes["dataset-summary-top-section"],
          !showColumns && classes["dataset-summary-sqleditor"]
        )}
      >
        {!detailsView && (
          <SummaryItemLabel
            fullPath={fullPath}
            resourceId={resourceId}
            datasetType={datasetType}
            canAlter={canAlter}
            title={title}
            hasReflection={hasReflection}
            selfLink={selfLink}
            editLink={editLink}
            disableActionButtons={disableActionButtons}
            hideSqlEditorIcon={hideSqlEditorIcon}
          />
        )}
        {!detailsView ? (
          <SummarySubHeader
            subTitle={fullPath}
            versionContext={versionContext}
          />
        ) : (
          <div className={classes["dataset-summary-dataset-path"]}>
            {/* need to render an invisible button here since opening the drawer will auto-select the button */}
            <button className={classes["hidden-button"]} />
            <SummarySubHeader
              subTitle={fullPath}
              versionContext={versionContext}
              detailsView
            />
            <CopyButton
              text={fullPath}
              title={formatMessage({ id: "Path.Copy" })}
              className={classes["copy-button"]}
            />
          </div>
        )}
        {detailsView ? (
          <div className={classes["dataset-summary-tags-stats"]}>
            {tagsComponent}
          </div>
        ) : tags?.size > 0 ? (
          <div className={classes["dataset-summary-tags-stats"]}>
            <TagList
              tags={tags}
              className={classes["dataset-summary-tagsWrapper"]}
            />
          </div>
        ) : (
          <></>
        )}
        <SummaryStats
          location={location}
          jobsLink={jobsLink}
          jobCount={jobCount}
          createdAt={createdAt}
          ownerEmail={ownerEmail}
          lastModifyUserEmail={lastModifyUserEmail}
          lastModified={lastModified}
          descendantsCount={descendantsCount}
          dataset={dataset}
          detailsView={detailsView}
        />
        {!detailsView && (
          <div className={classes["dataset-summary-button-wrapper"]}>
            <div
              className={clsx(
                classes["dataset-summary-open-details-button"],
                showColumns ? "margin-bottom--double" : ""
              )}
              onClick={() => {
                openWikiDrawer(dataset);
              }}
            >
              <dremio-icon
                name="interface/meta"
                class={classes["dataset-summary-open-details-icon"]}
              />
              {formatMessage({ id: "Wiki.OpenDetails" })}
            </div>
            {shouldRenderLineageButton && (
              <Link
                to={wrapBackendLink(`${selfLink}/graph`)}
                target="_blank"
                alt="Open graph button"
                className={clsx(
                  classes["dataset-summary-open-details-button"],
                  showColumns ? "margin-bottom--double" : ""
                )}
              >
                <dremio-icon
                  name="sql-editor/graph"
                  class={classes["dataset-summary-open-details-icon"]}
                />
                {formatMessage({ id: "Dataset.Summary.Lineage" })}
              </Link>
            )}
          </div>
        )}
      </div>
      {!detailsView && (
        <SummaryColumns
          showColumns={showColumns}
          fields={fields}
          fieldsCount={fieldsCount}
        />
      )}
    </div>
  );
};

export default DatasetSummary;
