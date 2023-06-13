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

import classNames from "clsx";
import TagsView from "../Labels/Labels";
import ViewStateWrapper from "@app/components/ViewStateWrapper";
import DataColumns from "../DataColumns/DataColumns";
import WikiModalWithSave from "./WikiModalWithSave";
import Collapsible from "../Collapsible/Collapsible";
import * as classes from "./WikiWrapper.module.less";
import { ReactNode } from "react";
import DatasetSummaryOverlay from "@app/components/Dataset/DatasetSummaryOverlay";
import { useIntl } from "react-intl";
//@ts-ignore
import ImmutablePropTypes from "react-immutable-proptypes";

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
  getLoadViewState: (showLoadMask: boolean) => void;
  showLoadMask: boolean;
  showWikiContent: boolean;
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
  addTag?: (tagName: string) => void;
  removeTag?: (tagName: string) => void;
  startSearch: () => void;
  showTags: boolean;
  renderCollapseIcon: () => ReactNode;
  isReadMode: boolean;
  dataset: ImmutablePropTypes.map;
  overlay?: boolean;
  searchTerm?: string;
}

const WikiWrapper = ({
  getLoadViewState,
  showLoadMask,
  showWikiContent,
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
  removeTag,
  startSearch,
  showTags,
  renderCollapseIcon,
  isReadMode = false,
  dataset,
  overlay = false,
  searchTerm,
}: WikiWrapperProps) => {
  const intl = useIntl();

  const datasetOverviewComponent = () => {
    let fullPath = dataset?.get("fullPath");
    if (dataset?.getIn(["fullPath", "0"]) === "tmp") {
      fullPath = dataset?.get("displayFullPath");
    }
    return (
      <div
        className={
          !overlay
            ? classes["dataset-wrapper"]
            : classes["dataset-wrapper-overlay"]
        }
      >
        <DatasetSummaryOverlay
          fullPath={fullPath}
          detailsView
          tagsComponent={
            showTags ? (
              <ViewStateWrapper
                viewState={tagsViewState}
                className={classes["sectionItem"]}
                style={wrapperStylesFix}
                hideChildrenWhenFailed={false}
                messageStyle={messageStyle}
              >
                <TagsView
                  className={classes["tags"]}
                  tags={getTags(tags)}
                  onAddTag={isEditAllowed ? addTag : null}
                  onRemoveTag={isEditAllowed ? removeTag : null}
                  onTagClick={startSearch}
                  isEditAllowed={isEditAllowed}
                />
              </ViewStateWrapper>
            ) : null
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
            <div
              className={classNames(
                classes["noColumns"],
                overlay
                  ? classes["noColumnsOverlayHeight"]
                  : classes["noColumnsHeight"]
              )}
            >
              {intl.formatMessage({ id: "Wiki.NoColumn" })}
            </div>
          )
        }
        bodyClass={overlay ? classes["collapsibleBodyOverlay"] : ""}
        bodyStyle={overlay ? undefined : { height: "100%" }}
      />
    );
  };

  const wikiDetailsComponent = () => {
    return (
      <Collapsible
        title={intl.formatMessage({ id: "Common.Wiki" })}
        toolbar={wikiToolbar}
        body={<>{renderWikiContent()}</>}
        bodyClass={overlay ? classes["collapsibleBodyOverlay"] : ""}
        bodyStyle={overlay ? undefined : { height: "100%" }}
      />
    );
  };

  return (
    <>
      {!overlay ? (
        <ViewStateWrapper
          viewState={getLoadViewState(showLoadMask)}
          style={{ height: "100vh", display: "flex", flex: 1, minHeight: 0 }} //todo
        >
          <div className={classes["layout"]} data-qa="wikiSection">
            {showWikiContent && (
              <div
                className={classNames(
                  classes["leftColumn"],
                  classes["sectionsContainer"],
                  extClassName
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
                  <div className={classes["sectionItem"]}>
                    {dataColumnsComponent()}
                  </div>
                  <div className={classes["sectionItem"]}>
                    {wikiDetailsComponent()}
                  </div>
                </ViewStateWrapper>
              </div>
            )}
            {!sidebarCollapsed ? (
              <div
                className={classNames(
                  classes["rightColumn"],
                  classes["sectionsContainer"]
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
            width: "400px",
          }} //todo
        >
          <Collapsible
            title={intl.formatMessage({ id: "Wiki.DatasetOverview" })}
            toolbar={[]}
            body={datasetOverviewComponent()}
            bodyClass={classes["collapsibleBodyOverview"]}
          />
          {dataColumnsComponent()}
          {wikiDetailsComponent()}
        </ViewStateWrapper>
      )}
      <WikiModalWithSave
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
