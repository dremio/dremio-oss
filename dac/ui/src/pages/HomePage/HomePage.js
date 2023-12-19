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
import { Component } from "react";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { connect } from "react-redux";

import { getHomeSourceUrl, getSortedSources } from "@app/selectors/home";
import ApiUtils from "@app/utils/apiUtils/apiUtils";
import { sourceTypesIncludeS3 } from "@app/utils/sourceUtils";
import { loadSourceListData } from "@app/actions/resources/sources";

import { isNotSoftware } from "dyn-load/utils/versionUtils";
import { getViewState } from "@app/selectors/resources";
import { fetchFeatureFlag } from "@inject/actions/featureFlag";
import { page } from "@app/uiTheme/radium/general";

import ProjectActivationHOC from "@inject/containers/ProjectActivationHOC";
import { SonarSideNav } from "@app/exports/components/SideNav/SonarSideNav";
import {
  HomePageTop,
  showHomePageTop,
} from "@inject/pages/HomePage/HomePageTop";
import NavCrumbs, {
  showNavCrumbs,
} from "@inject/components/NavCrumbs/NavCrumbs";
import QlikStateModal from "../ExplorePage/components/modals/QlikStateModal";
import LeftTree from "./components/LeftTree";
import "./HomePage.less";
import HomePageActivating from "@inject/pages/HomePage/HomePageActivating";
import { intl } from "@app/utils/intl";
import { ErrorBoundary } from "@app/components/ErrorBoundary/ErrorBoundary";
import { isSonarUrlabilityEnabled } from "@app/exports/utilities/featureFlags";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { compose } from "redux";
import { withRouter } from "react-router";
import { withCatalogARSFlag } from "@inject/utils/arsUtils";
import { ARCTIC_ENTITY_PRIVILEGES } from "@inject/featureFlags/flags/ARCTIC_ENTITY_PRIVILEGES";

const PROJECT_CONTEXT = "projectContext";

class HomePage extends Component {
  static propTypes = {
    userInfo: PropTypes.object,
    sources: PropTypes.instanceOf(Immutable.List).isRequired,
    sourcesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    routeParams: PropTypes.object,
    location: PropTypes.object.isRequired,
    loadSourceListData: PropTypes.func,
    fetchFeatureFlag: PropTypes.func,
    children: PropTypes.node,
    style: PropTypes.object,
    isProjectInactive: PropTypes.bool,

    // HOC
    isArsEnabled: PropTypes.bool,
    isArsLoading: PropTypes.bool,
    router: PropTypes.any,
    homeSourceUrl: PropTypes.string,
  };

  state = {
    sourceTypes: [],
  };

  doRedirect = () => {
    const { isArsEnabled, homeSourceUrl, location, router } = this.props;
    if (!isArsEnabled) return;

    const pathname =
      rmProjectBase(location.pathname, {
        projectId: router.params?.projectId,
      }) || "/";

    if (pathname === "/" && homeSourceUrl) {
      router.replace(homeSourceUrl);
    }
  };

  UNSAFE_componentWillMount() {
    this.props.loadSourceListData();
  }

  componentDidMount() {
    isNotSoftware() && this.props.fetchFeatureFlag(ARCTIC_ENTITY_PRIVILEGES);
    this.setStateWithSourceTypesFromServer();
    this.doRedirect();
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    if (nextProps.sourcesViewState.get("invalidated")) {
      nextProps.loadSourceListData();
    }
  }

  componentDidUpdate(prevProps) {
    const { isProjectInactive, isArsEnabled, homeSourceUrl } = this.props;
    if (prevProps.isProjectInactive && !isProjectInactive) {
      this.props.loadSourceListData();
    }
    if (
      prevProps.isArsEnabled !== isArsEnabled ||
      prevProps.homeSourceUrl !== homeSourceUrl
    ) {
      this.doRedirect();
    }
  }

  setStateWithSourceTypesFromServer() {
    ApiUtils.fetchJson(
      "source/type",
      (json) => {
        this.setState({ sourceTypes: json.data });
      },
      () => {
        console.error(
          laDeprecated(
            'Failed to load source types. Can not check if S3 is supported. Will not show "Add Sample Source".'
          )
        );
      }
    );
  }

  getUser() {
    const { userInfo } = this.props;
    return userInfo && userInfo.size > 0
      ? `@${userInfo.get("homeConfig").get("owner")}`
      : "";
  }

  // Note were are getting the "ref" to the SearchBar React object.
  render() {
    const { isProjectInactive, homeSourceUrl } = this.props;
    const homePageSearchClass = showHomePageTop()
      ? " --withSearch"
      : " --withoutSearch";

    const homePageNavCrumbClass = showNavCrumbs ? " --withNavCrumbs" : "";

    const storage = isSonarUrlabilityEnabled() ? sessionStorage : localStorage;
    const projectName =
      storage.getItem(PROJECT_CONTEXT) &&
      JSON.parse(storage.getItem(PROJECT_CONTEXT)).name;

    return (
      <div id="home-page" style={page}>
        <div className="page-content">
          <SonarSideNav />
          {!isProjectInactive && (
            <div className={"homePageBody"}>
              <HomePageTop />
              {isNotSoftware() && <NavCrumbs />}
              <div
                className={
                  "homePageLeftTreeDiv" +
                  homePageSearchClass +
                  homePageNavCrumbClass
                }
              >
                <LeftTree
                  homeSourceUrl={homeSourceUrl}
                  sourcesViewState={this.props.sourcesViewState}
                  sources={this.props.sources}
                  sourceTypesIncludeS3={sourceTypesIncludeS3(
                    this.state.sourceTypes
                  )}
                  routeParams={this.props.routeParams}
                  className="col-lg-2 col-md-3"
                  currentProject={projectName}
                />
                <ErrorBoundary
                  title={intl.formatMessage(
                    { id: "Support.error.section" },
                    {
                      section: intl.formatMessage({
                        id: "SectionLabel.catalog",
                      }),
                    }
                  )}
                >
                  {this.props.children}
                </ErrorBoundary>
              </div>
            </div>
          )}
          {isProjectInactive && <HomePageActivating />}
        </div>
        <QlikStateModal />
      </div>
    );
  }
}

function mapStateToProps(state, { isArsEnabled }) {
  const sources = getSortedSources(state);
  const sourcesViewState = getViewState(state, "AllSources");
  const homeSourceUrl =
    sourcesViewState.get("isInProgress") == null ||
    sourcesViewState.get("isInProgress")
      ? null
      : getHomeSourceUrl(sources, isArsEnabled);
  return {
    sources,
    homeSourceUrl,
    userInfo: state.home.config.get("userInfo"),
    sourcesViewState,
  };
}

export default compose(
  withRouter,
  withCatalogARSFlag,
  connect(mapStateToProps, {
    loadSourceListData,
    fetchFeatureFlag,
  }),
  ProjectActivationHOC
)(HomePage);
