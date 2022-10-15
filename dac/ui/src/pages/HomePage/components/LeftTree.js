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
import EmptyStateContainer from "./EmptyStateContainer";
import { IconButton } from "dremio-ui-lib";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
import { Link } from "react-router";

import {
  isDatabaseType,
  isDataPlaneSourceType,
  isMetastoreSourceType,
  isObjectStorageSourceType,
} from "@app/constants/sourceTypes";

import ApiUtils from "utils/apiUtils/apiUtils";
import { createSampleSource } from "actions/resources/sources";
import {
  toggleDataplaneSourcesExpanded,
  toggleExternalSourcesExpanded,
  toggleMetastoreSourcesExpanded,
  toggleObjectStorageSourcesExpanded,
  toggleDatasetsExpanded,
} from "actions/ui/ui";
import * as VersionUtils from "@app/utils/versionUtils";
import sendEventToIntercom from "@inject/sagas/utils/sendEventToIntercom";
import INTERCOM_EVENTS from "@inject/constants/intercomEvents";
import { spacesSourcesListSpinnerStyleFinderNav } from "@app/pages/HomePage/HomePageConstants";
import DataPlaneSection from "./DataPlaneSection/DataPlaneSection";
import { getAdminStatus } from "dyn-load/pages/HomePage/components/modals/SpaceModalMixin";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import { withErrorBoundary } from "@app/components/ErrorBoundary/withErrorBoundary";
import { intl } from "@app/utils/intl";
import "./LeftTree.less";
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
    toggleMetastoreSourcesExpanded: PropTypes.func,
    metastoreSourcesExpanded: PropTypes.bool,
    objectStorageSourcesExpanded: PropTypes.bool,
    toggleObjectStorageSourcesExpanded: PropTypes.func,
    currentProject: PropTypes.string,
    isAdmin: PropTypes.bool,
    canCreateSource: PropTypes.bool,
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
    const { canCreateSource: cloudCanCreateSource, isAdmin } = this.props;

    // software permission for creating a source is stored in localstorage,
    // while the permission on cloud is stored in Redux
    const canCreateSource =
      localStorageUtils.getUserPermissions()?.canCreateSource ||
      cloudCanCreateSource;

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
      toggleObjectStorageSourcesExpanded,
      toggleMetastoreSourcesExpanded,
      toggleDataplaneSourcesExpanded,
      datasetsExpanded,
      dataplaneSourcesExpanded,
      externalSourcesExpanded,
      metastoreSourcesExpanded,
      objectStorageSourcesExpanded,
    } = this.props;
    const { location } = this.context;
    const { formatMessage } = intl;
    const isHomeActive = location && location.pathname === "/";
    const classes = classNames("left-tree", "left-tree-holder", className);
    const homeItem = this.getHomeObject();

    const metastoreSource = sources.filter((source) =>
      isMetastoreSourceType(source.get("type"))
    );

    const objectStorageSource = sources.filter((source) =>
      isObjectStorageSourceType(source.get("type"))
    );

    const databases = sources.filter(
      (source) =>
        isDatabaseType(source.get("type")) &&
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
            <IconButton
              className="add-source-button"
              tooltip="Source.AddSource"
              as={LinkWithRef}
              to={this.getAddSourceHref(true)}
              data-qa="add-sources"
            >
              <dremio-icon name="interface/add-small" class="add-space-icon" />
            </IconButton>
          </div>
          {sources.size < 1 && (
            <EmptyStateContainer
              title="Sources.noSources"
              icon="interface/empty-add-data"
              linkInfo={{
                href: this.getAddSourceHref(true),
                "data-qa": "add-sources",
                label: "Sources.AddSource.LowerCase",
              }}
            />
          )}
          {dataPlaneSources.size > 0 && (
            <DataPlaneSection
              isCollapsed={dataplaneSourcesExpanded}
              isCollapsible
              onToggle={toggleDataplaneSourcesExpanded}
              dataPlaneSources={dataPlaneSources}
              sourcesViewState={sourcesViewState}
            />
          )}
          {metastoreSource.size > 0 && (
            <div className="left-tree-wrap">
              <ViewStateWrapper
                viewState={sourcesViewState}
                spinnerStyle={spacesSourcesListSpinnerStyleFinderNav}
              >
                <FinderNav
                  isCollapsed={metastoreSourcesExpanded}
                  isCollapsible
                  onToggle={toggleMetastoreSourcesExpanded}
                  navItems={metastoreSource}
                  title={formatMessage({ id: "Source.Metastores" })}
                  addTooltip={formatMessage({ id: "Source.Add.Metastore" })}
                  isInProgress={sourcesViewState.get("isInProgress")}
                  listHref="/sources/metastore/list"
                />
              </ViewStateWrapper>
            </div>
          )}
          {objectStorageSource.size > 0 && (
            <div className="left-tree-wrap">
              <ViewStateWrapper
                viewState={sourcesViewState}
                spinnerStyle={spacesSourcesListSpinnerStyleFinderNav}
              >
                <FinderNav
                  isCollapsed={objectStorageSourcesExpanded}
                  isCollapsible
                  onToggle={toggleObjectStorageSourcesExpanded}
                  navItems={objectStorageSource}
                  title={formatMessage({ id: "Source.Object.Storage" })}
                  addTooltip={formatMessage({
                    id: "Source.Add.Object.Storage",
                  })}
                  isInProgress={sourcesViewState.get("isInProgress")}
                  listHref="/sources/objectStorage/list"
                />
              </ViewStateWrapper>
            </div>
          )}

          {databases.size > 0 && (
            <div className="left-tree-wrap">
              <ViewStateWrapper viewState={sourcesViewState}>
                <FinderNav
                  isCollapsed={externalSourcesExpanded}
                  isCollapsible
                  onToggle={toggleExternalSourcesExpanded}
                  location={location}
                  navItems={databases}
                  title={formatMessage({ id: "Source.DatabaseSources" })}
                  addTooltip={formatMessage({
                    id: "Source.AddDatabaseSource",
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
    metastoreSourcesExpanded: state.ui.get("metastoreSourcesExpanded"),
    objectStorageSourcesExpanded: state.ui.get("objectStorageSourcesExpanded"),
    dataplaneSourcesExpanded: state.ui.get("dataplaneSourcesExpanded"),
    datasetsExpanded: state.ui.get("datasetsExpanded"),
    isAdmin: getAdminStatus(state),
    canCreateSource: state.privileges?.project?.canCreateSource,
  };
}

export default withErrorBoundary({
  title: intl.formatMessage(
    { id: "Support.error.section" },
    {
      section: intl.formatMessage({
        id: "SectionLabel.catalog.sources.browser",
      }),
    }
  ),
})(
  connect(mapStateToProps, {
    createSampleSource,
    toggleDataplaneSourcesExpanded,
    toggleExternalSourcesExpanded,
    toggleObjectStorageSourcesExpanded,
    toggleMetastoreSourcesExpanded,
    toggleDatasetsExpanded,
  })(LeftTree)
);
