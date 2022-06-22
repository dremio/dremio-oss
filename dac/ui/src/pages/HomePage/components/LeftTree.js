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
import { connect } from "react-redux";
import classNames from "classnames";
import { FormattedMessage, injectIntl } from "react-intl";
import Immutable from "immutable";

import PropTypes from "prop-types";
import FinderNav from "components/FinderNav";
import FinderNavItem from "components/FinderNavItem";
import ViewStateWrapper from "components/ViewStateWrapper";
import SpacesSection from "@app/pages/HomePage/components/SpacesSection";
import { Link } from "react-router";

import {
  isExternalSourceType,
  isDataPlaneSourceType,
  isDataLakeSourceType,
} from "@app/constants/sourceTypes";

import ApiUtils from "utils/apiUtils/apiUtils";
import { createSampleSource } from "actions/resources/sources";
import {
  toggleDataplaneSourcesExpanded,
  toggleExternalSourcesExpanded,
  toggleInternalSourcesExpanded,
  toggleDatasetsExpanded,
} from "actions/ui/ui";
import * as VersionUtils from "@app/utils/versionUtils";
import sendEventToIntercom from "@inject/sagas/utils/sendEventToIntercom";
import INTERCOM_EVENTS from "@inject/constants/intercomEvents";
import { spacesSourcesListSpinnerStyleFinderNav } from "@app/pages/HomePage/HomePageConstants";
import DataPlaneSection from "./DataPlaneSection/DataPlaneSection";
import { getAdminStatus } from "dyn-load/pages/HomePage/components/modals/SpaceModalMixin";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";

@injectIntl
export class LeftTree extends Component {
  state = {
    isAddingSampleSource: false,
  };

  static contextTypes = {
    location: PropTypes.object,
    loggedInUser: PropTypes.object,
    router: PropTypes.object,
  };

  static propTypes = {
    className: PropTypes.string,
    sources: PropTypes.instanceOf(Immutable.List).isRequired,
    sourceTypesIncludeS3: PropTypes.bool,
    sourcesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    createSampleSource: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired,
    dataplaneSourcesExpanded: PropTypes.bool,
    externalSourcesExpanded: PropTypes.bool,
    internalSourcesExpanded: PropTypes.bool,
    datasetsExpanded: PropTypes.bool,
    toggleDatasetsExpanded: PropTypes.func,
    toggleDataplaneSourcesExpanded: PropTypes.func,
    toggleExternalSourcesExpanded: PropTypes.func,
    toggleInternalSourcesExpanded: PropTypes.func,
    currentProject: PropTypes.string,
    isAdmin: PropTypes.bool,
  };

  addSampleSource = () => {
    const edition = VersionUtils.getEditionFromConfig();
    edition === "DCS" &&
      sendEventToIntercom(INTERCOM_EVENTS.SOURCE_ADD_COMPLETE);
    this.setState({ isAddingSampleSource: true });
    return this.props.createSampleSource().then((response) => {
      if (response && !response.error) {
        const nextSource = ApiUtils.getEntityFromResponse("source", response);
        this.context.router.push(nextSource.getIn(["links", "self"]));
      }
      this.setState({ isAddingSampleSource: false });
      return null;
    });
  };

  getHomeObject() {
    return {
      name: this.context.loggedInUser.userName,
      links: {
        self: "/home",
      },
      resourcePath: "/home",
      iconClass: "Home",
    };
  }

  getCanAddSource() {
    const { isAdmin } = this.props;
    const canCreateSource =
      localStorageUtils.getUserPermissions()?.canCreateSource;

    return isAdmin || canCreateSource;
  }

  getAddSourceHref(isExternalSource, isDataPlaneSource = false) {
    return {
      ...this.context.location,
      state: {
        modal: "AddSourceModal",
        isExternalSource,
        isDataPlaneSource,
      },
    };
  }

  onScroll(e) {
    const scrollTop = e.currentTarget.scrollTop;
    const scrollHeight = e.currentTarget.scrollHeight;
    const clientHeight = e.currentTarget.clientHeight;
    scrollTop > 0
      ? this.setState({ addTopShadow: true })
      : this.setState({ addTopShadow: false });

    scrollHeight - scrollTop !== clientHeight
      ? this.setState({ addBotShadow: true })
      : this.setState({ addBotShadow: false });
  }

  render() {
    const {
      className,
      sources,
      sourcesViewState,
      intl,
      currentProject,
      toggleDatasetsExpanded,
      toggleExternalSourcesExpanded,
      toggleInternalSourcesExpanded,
      toggleDataplaneSourcesExpanded,
      datasetsExpanded,
      dataplaneSourcesExpanded,
      externalSourcesExpanded,
      internalSourcesExpanded,
    } = this.props;
    const { location } = this.context;
    const { formatMessage } = intl;
    const isHomeActive = location && location.pathname === "/";
    const classes = classNames("left-tree", "left-tree-holder", className);
    const homeItem = this.getHomeObject();
    const dataLakeSources = sources.filter((source) =>
      isDataLakeSourceType(source.get("type"))
    );
    const externalSources = sources.filter(
      (source) =>
        isExternalSourceType(source.get("type")) &&
        !isDataPlaneSourceType(source.get("type"))
    );
    const dataPlaneSources = sources.filter((source) =>
      isDataPlaneSourceType(source.get("type"))
    );

    return (
      <div className={classes}>
        <h3
          className={`header-viewer ${
            this.state.addTopShadow ? "add-shadow-top" : ""
          }`}
        >
          {currentProject ? (
            currentProject
          ) : (
            <FormattedMessage id="Dataset.Datasets" />
          )}
        </h3>
        <div className="scrolling-container" onScroll={(e) => this.onScroll(e)}>
          <ul className="home-wrapper">
            <FinderNavItem item={homeItem} isHomeActive={isHomeActive} />
          </ul>
          <SpacesSection
            isCollapsed={datasetsExpanded}
            isCollapsible
            onToggle={toggleDatasetsExpanded}
          />
          <div className="sources-title">
            {formatMessage({ id: "Source.Sources" })}
          </div>
          {dataPlaneSources.size > 0 && (
            <DataPlaneSection
              isCollapsed={dataplaneSourcesExpanded}
              isCollapsible
              onToggle={toggleDataplaneSourcesExpanded}
              dataPlaneSources={dataPlaneSources}
              sourcesViewState={sourcesViewState}
            />
          )}
          {dataLakeSources.size > 0 && (
            <div className="left-tree-wrap">
              <ViewStateWrapper
                viewState={sourcesViewState}
                spinnerStyle={spacesSourcesListSpinnerStyleFinderNav}
              >
                <FinderNav
                  isCollapsed={internalSourcesExpanded}
                  isCollapsible
                  onToggle={toggleInternalSourcesExpanded}
                  navItems={dataLakeSources}
                  title={formatMessage({ id: "Source.DataLakes" })}
                  addTooltip={formatMessage({ id: "Source.AddDataLake" })}
                  isInProgress={sourcesViewState.get("isInProgress")}
                  listHref="/sources/datalake/list"
                />
              </ViewStateWrapper>
            </div>
          )}
          {externalSources.size > 0 && (
            <div className="left-tree-wrap">
              <ViewStateWrapper viewState={sourcesViewState}>
                <FinderNav
                  isCollapsed={externalSourcesExpanded}
                  isCollapsible
                  onToggle={toggleExternalSourcesExpanded}
                  location={location}
                  navItems={externalSources}
                  title={formatMessage({ id: "Source.ExternalSources" })}
                  addTooltip={formatMessage({
                    id: "Source.AddExternalSource",
                  })}
                  isInProgress={sourcesViewState.get("isInProgress")}
                  listHref="/sources/external/list"
                />
              </ViewStateWrapper>
            </div>
          )}
        </div>
        {this.getCanAddSource() && (
          <div
            className={`add-source-container ${
              this.state.addBotShadow ? "add-shadow-bot" : ""
            }`}
          >
            <Link
              className="add-source"
              data-qa={`add-source`}
              to={this.getAddSourceHref(true)}
            >
              <dremio-icon name="interface/add-small" class="add-source-icon" />
              {formatMessage({
                id: "Source.AddSource",
              })}
            </Link>
          </div>
        )}
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    externalSourcesExpanded: state.ui.get("externalSourcesExpanded"),
    internalSourcesExpanded: state.ui.get("internalSourcesExpanded"),
    dataplaneSourcesExpanded: state.ui.get("dataplaneSourcesExpanded"),
    datasetsExpanded: state.ui.get("datasetsExpanded"),
    isAdmin: getAdminStatus(state),
  };
}

export default connect(mapStateToProps, {
  createSampleSource,
  toggleDataplaneSourcesExpanded,
  toggleExternalSourcesExpanded,
  toggleInternalSourcesExpanded,
  toggleDatasetsExpanded,
})(LeftTree);
