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

import { getSortedSources } from "@app/selectors/home";
import ApiUtils from "@app/utils/apiUtils/apiUtils";
import { sourceTypesIncludeS3 } from "@app/utils/sourceUtils";
import { loadSourceListData } from "@app/actions/resources/sources";
import localStorageUtils from "@inject/utils/storageUtils/localStorageUtils";
import { PROJECT_STATES } from "@inject/pages/SettingPage/subpages/projects/ProjectConst";

import { getViewState } from "@app/selectors/resources";
import { getProjects } from "@inject/selectors/projects";
import { fetchProjects } from "@inject/actions/admin";
import { page } from "@app/uiTheme/radium/general";

import SideNav from "@app/components/SideNav/SideNav";
import {
  HomePageTop,
  showHomePageTop,
} from "@inject/pages/HomePage/HomePageTop";
import QlikStateModal from "../ExplorePage/components/modals/QlikStateModal";
import LeftTree from "./components/LeftTree";
import "./HomePage.less";
import HomePageActivating from "@inject/pages/HomePage/HomePageActivating";

const PROJECT_CONTEXT = "projectContext";
class HomePage extends Component {
  static propTypes = {
    userInfo: PropTypes.object,
    sources: PropTypes.instanceOf(Immutable.List).isRequired,
    sourcesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    routeParams: PropTypes.object,
    location: PropTypes.object.isRequired,
    loadSourceListData: PropTypes.func,
    fetchProjects: PropTypes.func,
    children: PropTypes.node,
    style: PropTypes.object,
    projectList: PropTypes.array,
  };

  state = {
    sourceTypes: [],
  };

  componentWillMount() {
    this.props.loadSourceListData();
    if (this.props.fetchProjects) {
      this.props.fetchProjects();
    }
  }

  componentDidMount() {
    this.setStateWithSourceTypesFromServer();
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.sourcesViewState.get("invalidated")) {
      nextProps.loadSourceListData();
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
          la(
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
    const homePageSearchClass = showHomePageTop()
      ? " --withSearch"
      : " --withoutSearch";

    const currentProject =
      localStorageUtils.getProjectContext &&
      localStorageUtils.getProjectContext();
    let isProjectInactive = false;

    if (currentProject) {
      const filteredProject = this.props.projectList.filter(
        (pr) => pr.id === currentProject.id
      )[0];
      if (filteredProject && filteredProject.state === PROJECT_STATES.ACTIVE) {
        isProjectInactive = false;
      } else if (
        filteredProject &&
        (filteredProject.state === PROJECT_STATES.INACTIVE ||
          filteredProject.state === PROJECT_STATES.ACTIVATING)
      ) {
        isProjectInactive = true;
      }
    }

    const projectName =
      localStorage.getItem(PROJECT_CONTEXT) &&
      JSON.parse(localStorage.getItem(PROJECT_CONTEXT)).name;

    return (
      <div id="home-page" style={page}>
        <div className="page-content">
          <SideNav />
          {!isProjectInactive && (
            <div className={"homePageBody"}>
              <HomePageTop />
              <div className={"homePageLeftTreeDiv" + homePageSearchClass}>
                <LeftTree
                  sourcesViewState={this.props.sourcesViewState}
                  sources={this.props.sources}
                  sourceTypesIncludeS3={sourceTypesIncludeS3(
                    this.state.sourceTypes
                  )}
                  routeParams={this.props.routeParams}
                  className="col-lg-2 col-md-3"
                  currentProject={projectName}
                />
                {this.props.children}
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

function mapStateToProps(state) {
  return {
    sources: getSortedSources(state),
    projectList: getProjects ? getProjects(state) : [],
    userInfo: state.home.config.get("userInfo"),
    sourcesViewState: getViewState(state, "AllSources"),
  };
}

export default connect(mapStateToProps, {
  loadSourceListData,
  fetchProjects,
})(HomePage);
