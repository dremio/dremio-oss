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
import { PureComponent, Component } from "react";
import PropTypes from "prop-types";
import classNames from "clsx";
import Immutable from "immutable";
import { withRouter } from "react-router";
import { connect } from "react-redux";
import { isWikAvailable } from "@app/selectors/explore";
import { PageTypes, pageTypesProp } from "@app/pages/ExplorePage/pageTypes";
import { changePageTypeInUrl } from "@app/pages/ExplorePage/pageTypeUtils";
import { formatMessage } from "@app/utils/locale";
import { fetchFeatureFlag } from "@inject/actions/featureFlag";
import PageTypeButtonsMixin from "dyn-load/pages/ExplorePage/components/PageTypeButtonsMixin";
import exploreUtils from "@app/utils/explore/exploreUtils";
import {
  buttonsContainer,
  button,
  buttonActive,
  icon as iconClass,
  iconActive,
} from "./PageTypeButtons.less";
import * as classes from "./PageTypeButtons.less";
import { isCommunity } from "dyn-load/utils/versionUtils";
import config from "@inject/utils/config";
import { REFLECTION_ARCTIC_ENABLED } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";

export class SinglePageTypeButton extends Component {
  static propTypes = {
    text: PropTypes.string,
    icon: PropTypes.string,
    isSelected: PropTypes.bool,
    onClick: PropTypes.func,
    dataQa: PropTypes.string,
    classname: PropTypes.string,
    disabled: PropTypes.bool,
  };

  render() {
    const { isSelected, onClick, text, icon, dataQa, classname } = this.props;

    return (
      <span
        className={classNames(button, isSelected && buttonActive, classname, {
          [classes["disabled"]]: this.props.disabled,
        })}
        onClick={onClick}
        data-qa={dataQa}
      >
        {icon && (
          <dremio-icon
            name={icon}
            class={classNames(iconClass, isSelected && iconActive)}
            alt={text}
          />
        )}
        {text}
      </span>
    );
  }
}

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
      <SinglePageTypeButton
        isSelected={selectedPageType === pageType}
        text={text}
        icon={icon}
        onClick={this.setPageType}
        dataQa={dataQa}
        disabled={this.props.disabled}
      />
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
    icon: "sql-editor/catalog",
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

const mapStateToProps = (state, { location }) => {
  let supportFlags = state.supportFlags;
  if (isCommunity?.()) {
    supportFlags = {
      [REFLECTION_ARCTIC_ENABLED]: config.arcticReflectionsEnabled,
    };
  }

  return {
    showWiki: isWikAvailable(state, location),
    supportFlags,
  };
};

@PageTypeButtonsMixin
export class PageTypeButtonsView extends PureComponent {
  static propTypes = {
    selectedPageType: pageTypesProp,
    dataset: PropTypes.instanceOf(Immutable.Map),
    showWiki: PropTypes.bool,
    dataQa: PropTypes.string,
    location: PropTypes.object,
    fetchFeatureFlag: PropTypes.func,
  };

  getAvailablePageTypes() {
    return [PageTypes.default];
  }

  render() {
    const { selectedPageType, dataQa, location } = this.props;
    const pageTypes = this.getAvailablePageTypes();
    const isDatasetPage = exploreUtils.isExploreDatasetPage(location);
    const shouldHideTabs = location.query?.hideTabs;

    // Show tabbed content for dataset
    if (pageTypes.length > 1 && isDatasetPage && !shouldHideTabs) {
      return (
        <span className={buttonsContainer} data-qa={dataQa}>
          {pageTypes.map((pageType) => {
            let { intlId, ...rest } = buttonsConfigs[pageType];
            if (pageType === "wiki") intlId = "Common.Details";
            return (
              <PageTypeButton
                key={pageType}
                selectedPageType={selectedPageType}
                text={formatMessage(intlId)}
                pageType={pageType}
                //Disable for join,groupBy, etc which use "type" param
                disabled={!!location.query?.type}
                {...rest}
              />
            );
          })}
        </span>
      );
    } else {
      // Hide all tabs for New Query and Updated Queries
      return null;
    }
  }
}

export const PageTypeButtons = withRouter(
  connect(mapStateToProps, { fetchFeatureFlag })(PageTypeButtonsView)
);
