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

import { ReactNode } from "react";
import classNames from "clsx";
import { useSelector } from "react-redux";
//@ts-ignore
import ImmutablePropTypes from "react-immutable-proptypes";
import { useIntl } from "react-intl";

import TagsView from "../Labels/Labels";
import ViewStateWrapper from "#oss/components/ViewStateWrapper";
import DataColumns from "../DataColumns/DataColumns";
import WikiModalWithSave from "./WikiModalWithSave";
import Collapsible from "../Collapsible/Collapsible";
import DatasetSummaryOverlay from "#oss/components/Dataset/DatasetSummaryOverlay";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { hideForNonDefaultBranch } from "dremio-ui-common/utilities/versionContext.js";
//@ts-ignore
import { isNotSoftware } from "dyn-load/utils/versionUtils";
import { getEntityTypeFromObject } from "#oss/utils/entity-utils";
import { ENTITY_TYPES_LIST } from "#oss/constants/Constants";
import Message from "#oss/components/Message";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { getSummaryDataset } from "#oss/selectors/datasets";

import * as classes from "./WikiWrapper.module.less";

type SaveProps = {
  saveVal: { text: string; version: string };
};

type toolbarProps = {
  component: ReactNode;
  componentClass: string;
  name: string;
  onClickHandle: () => void;
  tooltip: string;
};
interface WikiWrapperProps {
  tagsSaving: boolean;
  getLoadViewState: (showLoadMask: boolean) => void;
  showLoadMask: boolean;
  extClassName: string;
  wikiViewState: ImmutablePropTypes.map;
  wrapperStylesFix: { height: string };
  messageStyle: { top: number };
  columnDetails: ImmutablePropTypes.list;
  columnToolbar: toolbarProps[];
  wikiToolbar: toolbarProps[];
  renderWikiContent: () => null | ReactNode;
  isWikiInEditMode: boolean;
  entityId: string;
  onChange: () => void;
  wiki: string;
  wikiVersion: number;
  saveWiki: (saveVal: SaveProps) => void;
  cancelWikiEdit: () => void;
  sidebarCollapsed: boolean;
  tagsViewState: ImmutablePropTypes.map;
  getTags: (tags: string) => void;
  tags: ImmutablePropTypes.orderedMap;
  isEditAllowed: boolean;
  tagsVersion: string | null;
  setOriginalTags: any;
  startSearch: () => void;
  renderCollapseIcon: () => ReactNode;
  isReadMode: boolean;
  dataset: ImmutablePropTypes.map;
  wikiSummary?: boolean;
  addTag?: (tagName: string) => void;
  removeTag?: (tagName: string) => void;
  overlay?: boolean;
  searchTerm?: string;
  isPanel?: boolean;
  hideSqlEditorIcon?: boolean;
  hideGoToButton?: boolean;
  isPanelError?: boolean;
  handlePanelDetails?: (dataset: any) => void;
}

const WikiWrapper = ({
  tagsSaving,
  getLoadViewState,
  showLoadMask,
  extClassName,
  wikiViewState,
  wrapperStylesFix,
  messageStyle,
  columnDetails,
  columnToolbar,
  wikiToolbar,
  renderWikiContent,
  isWikiInEditMode,
  entityId,
  onChange,
  wiki,
  wikiVersion,
  saveWiki,
  cancelWikiEdit,
  sidebarCollapsed,
  tagsViewState,
  getTags,
  tags,
  isEditAllowed,
  addTag,
  tagsVersion,
  setOriginalTags,
  removeTag,
  startSearch,
  renderCollapseIcon,
  isReadMode = false,
  dataset,
  wikiSummary = false,
  overlay = false,
  searchTerm,
  isPanel = false,
  hideSqlEditorIcon = false,
  hideGoToButton = false,
  isPanelError,
  handlePanelDetails,
}: WikiWrapperProps) => {
  const intl = useIntl();
  const { t } = getIntlContext();
  const versionContext = getVersionContextFromId(entityId);
  const shouldShowWikiSection = hideForNonDefaultBranch(versionContext);
  const isSmallerView = isPanel || overlay;
  let fullPath = dataset?.get("fullPath") || dataset.get("fullPathList");
  if (dataset?.getIn(["fullPath", "0"]) === "tmp") {
    fullPath = dataset?.get("displayFullPath");
  }
  const type = getEntityTypeFromObject(dataset);
  const isEntity =
    ENTITY_TYPES_LIST.includes(type?.toLowerCase()) &&
    !dataset.get("queryable");
  const summaryDataset = useSelector((state) =>
    getSummaryDataset(state, fullPath?.join(",")),
  );

  const datasetOverviewComponent = () => {
    return (
      <div
        className={
          !isSmallerView
            ? classes["dataset-wrapper"]
            : classes["dataset-wrapper-overlay"]
        }
      >
        <DatasetSummaryOverlay
          fullPath={fullPath}
          detailsView
          isPanel={isPanel}
          versionContext={versionContext}
          hideSqlEditorIcon={hideSqlEditorIcon}
          hideGoToButton={hideGoToButton}
          handlePanelDetails={handlePanelDetails}
          tagsComponent={
            <ViewStateWrapper
              viewState={tagsViewState}
              className={classNames(
                classes["sectionItem"],
                isPanel && classes["tab-wrapper-panel"],
              )}
              style={wrapperStylesFix}
              hideChildrenWhenFailed={false}
              messageStyle={messageStyle}
            >
              <TagsView
                tagsSaving={tagsSaving}
                className={classes["tags"]}
                tags={getTags(tags)}
                onAddTag={isEditAllowed ? addTag : null}
                onRemoveTag={isEditAllowed ? removeTag : null}
                tagsVersion={tagsVersion}
                fullPath={fullPath}
                onTagClick={startSearch}
                setOriginalTags={setOriginalTags}
                isEditAllowed={isEditAllowed}
                entityId={entityId}
              />
            </ViewStateWrapper>
          }
        />
      </div>
    );
  };

  const dataColumnsComponent = () => {
    return (
      <Collapsible
        title={`${intl.formatMessage({ id: "Common.Columns" })} (${
          columnDetails?.size
        })`}
        toolbar={columnToolbar}
        body={
          columnDetails.size > 0 ? (
            <DataColumns
              columns={columnDetails?.toJS()}
              searchTerm={searchTerm}
            />
          ) : (
            <div className={classNames(classes["noColumns"])}>
              {intl.formatMessage({ id: "Wiki.NoColumn" })}
            </div>
          )
        }
        bodyClass={isSmallerView ? classes["collapsibleBodyOverlay"] : ""}
        bodyStyle={isSmallerView ? { height: "33%" } : { height: "50%" }}
      />
    );
  };

  const wikiDetailsComponent = () => {
    return (
      <Collapsible
        title={intl.formatMessage({ id: "Common.Wiki" })}
        toolbar={wikiToolbar}
        body={
          <>
            <ViewStateWrapper viewState={wikiViewState}>
              {renderWikiContent()}
            </ViewStateWrapper>
          </>
        }
        bodyClass={isSmallerView ? classes["collapsibleBodyOverlay"] : ""}
        bodyStyle={isSmallerView ? { height: "33%" } : { height: "50%" }}
      />
    );
  };

  return (
    <>
      {!isSmallerView ? (
        <ViewStateWrapper
          viewState={getLoadViewState(showLoadMask)}
          style={{
            height: `calc(100vh - 65px - ${isNotSoftware() ? 39 : 60}px)`, // to be refactored
            display: "flex",
            flex: 1,
            minHeight: 0,
            flexDirection: "column",
          }}
        >
          {summaryDataset?.get?.("schemaOutdated") && (
            <Message
              messageType="warning"
              message={t("Sonar.Dataset.DataGraph.OutdatedWarning")}
              className={classes["no-margin"]}
            />
          )}
          <div className={classes["layout"]} data-qa="wikiSection">
            <div
              className={classNames(
                classes["leftColumn"],
                classes["sectionsContainer"],
                sidebarCollapsed && classes["leftColumnCollapsedRightSection"],
                extClassName,
              )}
              data-qa="wikiWrapper"
            >
              <ViewStateWrapper
                viewState={wikiViewState}
                style={{
                  ...wrapperStylesFix,
                  height: "100%",
                  display: "flex",
                  flexDirection: "column",
                }}
                hideChildrenWhenFailed={false}
                messageStyle={messageStyle}
              >
                {dataColumnsComponent()}
                {shouldShowWikiSection && wikiDetailsComponent()}
              </ViewStateWrapper>
            </div>
            {!sidebarCollapsed ? (
              <div
                className={classNames(
                  classes["rightColumn"],
                  classes["sectionsContainer"],
                )}
                data-qa="tagsSection"
              >
                <ViewStateWrapper
                  viewState={tagsViewState}
                  className={classes["datasetOverviewContainer"]}
                  style={wrapperStylesFix}
                  hideChildrenWhenFailed={false}
                  messageStyle={messageStyle}
                >
                  <div className={classes["sectionTitle"]}>
                    {intl.formatMessage({ id: "Wiki.DatasetOverview" })}
                    {renderCollapseIcon()}
                  </div>
                  {datasetOverviewComponent()}
                </ViewStateWrapper>
              </div>
            ) : (
              <div className={classes["collapsedRightSection"]}>
                {renderCollapseIcon()}
              </div>
            )}
          </div>
        </ViewStateWrapper>
      ) : (
        <ViewStateWrapper
          viewState={getLoadViewState(showLoadMask)}
          style={{
            height: "100%",
            display: "flex",
            flexDirection: "column",
            minHeight: 0,
            width: isPanel ? 320 : 400,
            overflowY: "auto",
          }} //todo
        >
          {!isEntity && ( // Hide Overview section for entities until BE supports entity metadata
            <Collapsible
              title={intl.formatMessage({ id: "Wiki.DatasetOverview" })}
              toolbar={[]}
              body={datasetOverviewComponent()}
              bodyStyle={isSmallerView ? { flex: "0" } : {}}
            />
          )}
          {!isEntity && !isPanelError && dataColumnsComponent()}
          {shouldShowWikiSection && wikiDetailsComponent()}
        </ViewStateWrapper>
      )}
      <WikiModalWithSave
        wikiSummary={wikiSummary}
        fullPath={fullPath}
        entityType={dataset?.get("datasetType")}
        isOpen={isWikiInEditMode}
        entityId={entityId}
        onChange={onChange}
        wikiValue={wiki}
        wikiVersion={wikiVersion}
        save={saveWiki}
        cancel={cancelWikiEdit}
        isReadMode={isReadMode}
      />
    </>
  );
};

export default WikiWrapper;
