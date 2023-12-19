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
import clsx from "clsx";
import { useState, useMemo, useEffect } from "react";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { connect } from "react-redux";

import { intl } from "@app/utils/intl";
import SubHeaderTabs from "@app/components/SubHeaderTabs";
import { getLocation } from "@app/selectors/routing";
import SearchDatasetsPopover from "../DatasetList/SearchDatasetsPopover";
import {
  RESOURCE_LIST_SORT_MENU,
  DATA_SCRIPT_TABS,
  starTabNames,
} from "@app/components/Tree/resourceTreeUtils";
import SortDropDownMenu from "@app/components/SortDropDownMenu";
import SQLScripts from "../SQLScripts/SQLScripts";
import TreeNode from "./TreeNode";
//@ts-ignore
import { TabsNavigationItem } from "dremio-ui-lib";
import "./TreeBrowser.less";
import * as classes from "./TreeBrowser.less";
import { useFilterTreeArs } from "@app/utils/datasetTreeUtils";
import { getHomeSource, getSortedSources } from "@app/selectors/home";
import { useIsArsEnabled } from "@inject/utils/arsUtils";
import { useMultiTabIsEnabled } from "../SQLScripts/useMultiTabIsEnabled";
import { isTabbableUrl } from "@app/utils/explorePageTypeUtils";

export const TreeBrowser = (props) => {
  const {
    location,
    sidebarCollapsed,
    isCollapsable,
    resourceTree,
    isSqlEditorTab,
    starredItems,
    starNode,
    unstarNode,
    changeStarredTab,
    selectedStarredTab,
    homeSource: arsHomeSource,
  } = props;

  const isTabsRendered = useMultiTabIsEnabled() && isTabbableUrl(location);

  const [selectedTab, setSelectedTab] = useState(DATA_SCRIPT_TABS.Data);
  const [sort, setSort] = useState(RESOURCE_LIST_SORT_MENU[1]);

  const [collapaseText, setCollapseText] = useState();
  const [starredTabsArray, setStarredTabsArray] = useState([
    intl.formatMessage({ id: "Resource.Tree.All" }),
  ]);

  const [isArsLoading, isArsEnabled] = useIsArsEnabled();
  useEffect(() => {
    if (isArsLoading || isArsEnabled) return;

    setStarredTabsArray([
      intl.formatMessage({ id: "Resource.Tree.All" }),
      intl.formatMessage({ id: "Resource.Tree.Starred" }) +
        ` (${starredItems && starredItems.length})`,
    ]);
  }, [isArsEnabled, starredItems, isArsLoading]);

  useEffect(() => {
    if (location && location.state && location.state.renderScriptTab) {
      setSelectedTab(DATA_SCRIPT_TABS.Scripts);
    }
  }, [location, setSelectedTab]);

  useEffect(() => {
    if (!isSqlEditorTab) {
      setSelectedTab(DATA_SCRIPT_TABS.Data);
    }
  }, [isSqlEditorTab, setSelectedTab]);

  useEffect(() => {
    setCollapseText(
      sidebarCollapsed
        ? intl.formatMessage({ id: "Explore.Left.Panel.Collapse.Text.Close" })
        : intl.formatMessage({ id: "Explore.Left.Panel.Collapse.Text.Open" })
    );
  }, [sidebarCollapsed]);

  const treeFilterFunc = useFilterTreeArs();

  const [homeSource, sortedTree] = useMemo(() => {
    let tempResourceTree = resourceTree;
    if (isArsLoading) tempResourceTree = Immutable.fromJS([]); //Don't render while loading

    if (isArsEnabled && treeFilterFunc) {
      const sorted = treeFilterFunc(resourceTree);
      tempResourceTree = sorted;
    }

    const tempHomeSource = Array.from(tempResourceTree)[0];

    const hasHomeNode =
      tempHomeSource &&
      (tempHomeSource.get("type") === "HOME" ||
        (arsHomeSource &&
          tempHomeSource.get("name") === arsHomeSource.get("name")));

    // Remove home item for new copied list
    const tempOtherSources = hasHomeNode
      ? Array.from(tempResourceTree).splice(1)
      : Array.from(tempResourceTree);

    const tempSortedTree = tempOtherSources.sort(sort.compare);

    return hasHomeNode
      ? [tempHomeSource, tempSortedTree]
      : [undefined, tempSortedTree];
  }, [
    resourceTree,
    sort,
    isArsEnabled,
    isArsLoading,
    treeFilterFunc,
    arsHomeSource,
  ]);

  const renderSubHeadingTabs = () => {
    return (
      selectedTab === DATA_SCRIPT_TABS.Data && (
        <div className="TreeBrowser__subHeading">
          <SubHeaderTabs
            onClickFunc={changeStarredTab}
            tabArray={starredTabsArray}
            selectedTab={selectedStarredTab}
          />
          <SortDropDownMenu
            menuList={RESOURCE_LIST_SORT_MENU}
            sortValue={sort}
            setSortValue={setSort}
          />
        </div>
      )
    );
  };

  const renderHome = () => {
    if (homeSource) {
      return (
        <TreeNode
          node={homeSource}
          key={0}
          isStarredLimitReached={starredItems.length === 25}
          {...props}
        />
      );
    }
  };

  const renderItems = () => {
    if (sortedTree.length > 0) {
      return sortedTree.map((currNode, index) => {
        return (
          <TreeNode
            node={currNode}
            key={index}
            isStarredLimitReached={starredItems.length === 25}
            isSqlEditorTab={isSqlEditorTab}
            selectedStarredTab={selectedStarredTab}
            {...props}
          />
        );
      });
    } else if (
      homeSource === undefined &&
      sortedTree.length === 0 &&
      selectedStarredTab === starTabNames.starred
    ) {
      return (
        <span className="TreeBrowser--empty">
          {intl.formatMessage({ id: "Resource.Tree.No.Stars" })}
        </span>
      );
    }
  };

  const renderTabs = () => {
    return props.isSqlEditorTab ? (
      <>
        <TabsNavigationItem
          name="Data"
          activeTab={selectedTab}
          onClick={() => setSelectedTab(DATA_SCRIPT_TABS.Data)}
        >
          {intl.formatMessage({ id: "Dataset.Data" })}
        </TabsNavigationItem>
        <TabsNavigationItem
          name="Scripts"
          activeTab={selectedTab}
          onClick={() => setSelectedTab(DATA_SCRIPT_TABS.Scripts)}
        >
          {intl.formatMessage({ id: "Common.Scripts" })}
        </TabsNavigationItem>
      </>
    ) : (
      <div className="TreeBrowser-tab">Data</div>
    );
  };

  const renderCollapseIcon = () => {
    return (
      <dremio-icon
        title={collapaseText}
        alt={intl.formatMessage({ id: "Explore.Left.Panel.Collapse.Alt" })}
        name={
          sidebarCollapsed ? "scripts/CollapseRight" : "scripts/CollapseLeft"
        }
        onClick={props.handleSidebarCollapse}
        class={classes["collapseButton"]}
      ></dremio-icon>
    );
  };

  const renderTabsContent = () => {
    if (selectedTab === DATA_SCRIPT_TABS.Data) {
      return (
        <>
          {renderSubHeadingTabs()}
          <SearchDatasetsPopover
            changeSelectedNode={() => {}}
            dragType={props.dragType}
            addtoEditor={props.addtoEditor}
            shouldAllowAdd
            isStarredLimitReached={starredItems.length === 25}
            starNode={starNode}
            starredItems={starredItems}
            unstarNode={unstarNode}
          />
          <div className="TreeBrowser-items">
            {renderHome()}
            {renderItems()}
          </div>
        </>
      );
    } else if (selectedTab === DATA_SCRIPT_TABS.Scripts) {
      return <SQLScripts />;
    }
  };

  return (
    <div
      className={clsx("TreeBrowser", {
        "--withTabs": isTabsRendered,
      })}
    >
      <div
        className={`TreeBrowser-heading ${!isSqlEditorTab ? "--dataset" : ""} ${
          sidebarCollapsed ? "--collapsed" : ""
        }`}
      >
        {!sidebarCollapsed && renderTabs()}
        {isCollapsable && renderCollapseIcon()}
      </div>
      {!sidebarCollapsed && renderTabsContent()}
    </div>
  );
};

TreeBrowser.propTypes = {
  resourceTree: PropTypes.instanceOf(Immutable.List),
  isNodeExpanded: PropTypes.func,
  selectedNodeId: PropTypes.string,
  addtoEditor: PropTypes.func,
  dragType: PropTypes.string,
  isSqlEditorTab: PropTypes.bool,
  location: PropTypes.object,
  handleSidebarCollapse: PropTypes.func,
  sidebarCollapsed: PropTypes.bool,
  isCollapsable: PropTypes.bool,
  formatIdFromNode: PropTypes.func,
  isDatasetsDisabled: PropTypes.bool,
  isSourcesHidden: PropTypes.bool,
  shouldAllowAdd: PropTypes.bool,
  shouldShowOverlay: PropTypes.bool,
  handleSelectedNodeChange: PropTypes.func,
  isNodeExpandable: PropTypes.func,
  isExpandable: PropTypes.bool,
  starredItems: PropTypes.array,
  starNode: PropTypes.func,
  unstarNode: PropTypes.func,
  changeStarredTab: PropTypes.func,
  selectedStarredTab: PropTypes.string,
  homeSource: PropTypes.object,
};

TreeBrowser.defaultProps = {
  resourceTree: Immutable.List(),
};

TreeBrowser.contextTypes = {
  loggedInUser: PropTypes.object,
};

const mapStateToProps = (state) => ({
  homeSource: getHomeSource(getSortedSources(state)),
  location: getLocation(state),
});

export default connect(mapStateToProps)(TreeBrowser);
