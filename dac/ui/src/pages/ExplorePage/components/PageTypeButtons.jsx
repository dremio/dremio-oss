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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import classNames from "clsx";
import Immutable from "immutable";
import { withRouter } from "react-router";
import { PageTypes, pageTypesProp } from "@app/pages/ExplorePage/pageTypes";
import { changePageTypeInUrl } from "@app/pages/ExplorePage/pageTypeUtils";
import { formatMessage } from "@app/utils/locale";
import exploreUtils from "@app/utils/explore/exploreUtils";
import { buttonsContainer } from "./PageTypeButtons.less";
import * as classes from "./PageTypeButtons.less";
import { compose } from "redux";
import { withAvailablePageTypes } from "dyn-load/utils/explorePageTypes";
import { Tab, TabList, getControlledTabProps } from "dremio-ui-lib/components";
import { PHYSICAL_DATASET } from "@app/constants/datasetTypes";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";

class ButtonController extends PureComponent {
  static propTypes = {
    selectedPageType: pageTypesProp,
    pageType: pageTypesProp.isRequired,
    text: PropTypes.string,
    icon: PropTypes.string,
    dataQa: PropTypes.string,
    disabled: PropTypes.bool,
    //withRouter props
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired,
  };

  setPageType = () => {
    const {
      selectedPageType,
      pageType, // new page type to select
      location,
      router,
    } = this.props;

    if (this.props.disabled) return;

    if (selectedPageType !== pageType) {
      const pathname = changePageTypeInUrl(location.pathname, pageType);

      //Type is used for sub-pags like join,group by. Remove when navigating
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { type: omit, ...query } = location.query || {};

      router.push({ ...location, pathname, query });
    }
  };

  render() {
    const { selectedPageType, pageType, text, icon, dataQa } = this.props;

    return (
      <Tab
        className={classNames({
          [classes["disabled"]]: this.props.disabled,
        })}
        onClick={this.setPageType}
        data-qa={dataQa}
        {...getControlledTabProps({
          id: pageType,
          controls: `${pageType}Panel`,
          currentTab: selectedPageType,
        })}
        disabled={this.props.disabled}
      >
        <div className="dremio-icon-label">
          <dremio-icon name={icon} alt={text} />
          {text}
        </div>
      </Tab>
    );
  }
}

const PageTypeButton = withRouter(ButtonController);

const buttonsConfigs = {
  [PageTypes.default]: {
    intlId: "Dataset.Data",
    icon: "navigation-bar/sql-runner",
    dataQa: "Data",
  },
  [PageTypes.wiki]: {
    intlId: "Common.Details",
    icon: "interface/dataset-details",
    dataQa: "Wiki",
  },
  [PageTypes.graph]: {
    intlId: "Dataset.Lineage",
    icon: "sql-editor/graph",
    dataQa: "Graph",
  },
  [PageTypes.reflections]: {
    intlId: "Reflection.Reflections",
    icon: "sql-editor/reflections",
    dataQa: "Reflections",
  },
  [PageTypes.history]: {
    intlId: "Common.History",
    icon: "interface/history",
    dataQa: "History",
  },
};

export class PageTypeButtonsView extends PureComponent {
  static propTypes = {
    selectedPageType: pageTypesProp,
    dataset: PropTypes.instanceOf(Immutable.Map),
    showWiki: PropTypes.bool,
    dataQa: PropTypes.string,
    location: PropTypes.object,
    fetchFeatureFlag: PropTypes.func,
    availablePageTypes: PropTypes.arrayOf(PropTypes.string).isRequired,
  };

  render() {
    const {
      selectedPageType,
      dataQa,
      location,
      availablePageTypes: pageTypes,
      dataset,
    } = this.props;
    const isDatasetPage = exploreUtils.isExploreDatasetPage(location);
    const shouldHideTabs = location.query?.hideTabs;

    // Show tabbed content for dataset
    if (pageTypes.length > 1 && isDatasetPage && !shouldHideTabs) {
      const isVersionedTable =
        dataset.get("datasetType") === PHYSICAL_DATASET &&
        !!getVersionContextFromId(dataset.get("entityId"));

      return (
        <TabList
          aria-label="Page type tabs"
          className={buttonsContainer}
          data-qa={dataQa}
        >
          {pageTypes.map((pageType) => {
            const { intlId, ...rest } = buttonsConfigs[pageType];

            const showDefinitionTab =
              pageType === PageTypes.default && isVersionedTable;

            const message = showDefinitionTab ? "Common.Definition" : intlId;

            const newProps = {
              ...rest,
              ...(showDefinitionTab && {
                intlId: "Common.Definition",
                icon: "navigation-bar/dataset",
                dataQa: "Definition",
              }),
            };

            return (
              <PageTypeButton
                key={pageType}
                selectedPageType={selectedPageType}
                text={formatMessage(message)}
                pageType={pageType}
                //Disable for join,groupBy, etc which use "type" param
                disabled={!!location.query?.type}
                {...newProps}
              />
            );
          })}
        </TabList>
      );
    } else {
      // Hide all tabs for New Query and Updated Queries
      return null;
    }
  }
}

export const PageTypeButtons = compose(
  withAvailablePageTypes,
  withRouter
)(PageTypeButtonsView);
