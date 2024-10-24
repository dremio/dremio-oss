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

import { Component, createRef } from "react";
import { connect } from "react-redux";
import { browserHistory } from "react-router";
import { compose } from "redux";
import classNames from "clsx";
import { FormattedMessage, injectIntl } from "react-intl";
import Immutable from "immutable";
import { Tooltip } from "dremio-ui-lib";
import PropTypes from "prop-types";
import FinderNav from "components/FinderNav";
import FinderNavItem from "components/FinderNavItem";
import ViewStateWrapper from "components/ViewStateWrapper";
import SpacesSection from "#oss/pages/HomePage/components/SpacesSection";
import EmptyStateContainer from "./EmptyStateContainer";
import { Button, IconButton } from "dremio-ui-lib/components";
import LinkWithRef from "#oss/components/LinkWithRef/LinkWithRef";
import { fetchSupportFlags } from "#oss/actions/supportFlags";

import {
  isDatabaseType,
  isDataPlaneSourceType,
  isObjectStorageSourceType,
  isLakehouseSourceType,
} from "@inject/constants/sourceTypes";

import ApiUtils from "utils/apiUtils/apiUtils";
import { createSampleSource } from "actions/resources/sources";
import {
  toggleExternalSourcesExpanded,
  toggleObjectStorageSourcesExpanded,
  toggleDatasetsExpanded,
  toggleLakehouseSourcesExpanded,
} from "#oss/actions/ui/ui";
import { sonarEvents } from "dremio-ui-common/sonar/sonarEvents.js";
import { spacesSourcesListSpinnerStyleFinderNav } from "#oss/pages/HomePage/HomePageConstants";
import DataPlaneSection from "./DataPlaneSection/DataPlaneSection";
import { getAdminStatus } from "dyn-load/pages/HomePage/components/modals/SpaceModalMixin";
import localStorageUtils from "#oss/utils/storageUtils/localStorageUtils";
import { withErrorBoundary } from "#oss/components/ErrorBoundary/withErrorBoundary";
import { intl } from "#oss/utils/intl";
import {
  rmProjectBase,
  addProjectBase as wrapBackendLink,
} from "dremio-ui-common/utilities/projectBase.js";
import * as commonPaths from "dremio-ui-common/paths/common.js";
import additionalLeftTreeControls from "@inject/shared/AdditionalLeftTreeControls";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { ALLOW_LAKEHOUSE_CATALOG_DELETE } from "@inject/endpoints/SupportFlags/supportFlagConstants";

import "./LeftTree.less";
@injectIntl
export class LeftTree extends Component {
  constructor(props) {
    super(props);
    this.headerRef = createRef();
  }
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
    homeSourceUrl: PropTypes.string,
    fetchSupportFlags: PropTypes.func,

    // HOC
    location: PropTypes.any,
  };

  componentDidMount() {
    if (ALLOW_LAKEHOUSE_CATALOG_DELETE) {
      this.props.fetchSupportFlags(ALLOW_LAKEHOUSE_CATALOG_DELETE);
    }
  }

  addSampleSource = () => {
    sonarEvents.sourceAddComplete();
    this.setState({ isAddingSampleSource: true });
    return this.props.createSampleSource().then((response) => {
      if (response && !response.error) {
        const nextSource = ApiUtils.getEntityFromResponse("source", response);
        this.context.router.push(
          wrapBackendLink(nextSource.getIn(["links", "self"])),
        );
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
    const { canCreateSource: cloudCanCreateSource } = this.props;

    // software permission for creating a source is stored in localstorage,
    // while the permission on cloud is stored in Redux
    const canCreateSource =
      localStorageUtils.getUserPermissions()?.canCreateSource ||
      cloudCanCreateSource;

    return canCreateSource;
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
      toggleLakehouseSourcesExpanded,
      datasetsExpanded,
      lakehouseSourcesExpanded,
      externalSourcesExpanded,
      objectStorageSourcesExpanded,
    } = this.props;
    const { location } = this.context;
    const { formatMessage } = intl;
    const loc = rmProjectBase(location.pathname) || "/";
    const projectId = getSonarContext()?.getSelectedProjectId?.();
    const isHomeActive = location && loc === "/";
    const classes = classNames("left-tree", "left-tree-holder", className);
    const homeItem = this.getHomeObject();

    const lakehouseSource = sources.filter((source) =>
      isLakehouseSourceType(source.get("type")),
    );

    const objectStorageSource = sources.filter((source) =>
      isObjectStorageSourceType(source.get("type")),
    );

    const databases = sources.filter(
      (source) =>
        isDatabaseType(source.get("type")) &&
        !isDataPlaneSourceType(source.get("type")),
    );

    const showEmptySourceContainer =
      lakehouseSource.size < 1 &&
      objectStorageSource.size < 1 &&
      databases.size < 1;

    return (
      <div className={classes}>
        <h3
          ref={this.headerRef}
          className={`header-viewer ${
            this.state.addTopShadow ? "add-shadow-top" : ""
          }`}
        >
          {this.headerRef?.current?.offsetWidth <
          this.headerRef?.current?.scrollWidth ? (
            <Tooltip title={currentProject ?? "Dataset.Datasets"}>
              <span>
                {currentProject ?? <FormattedMessage id="Dataset.Datasets" />}
              </span>
            </Tooltip>
          ) : (
            (currentProject ?? <FormattedMessage id="Dataset.Datasets" />)
          )}
        </h3>
        <div className="scrolling-container" onScroll={(e) => this.onScroll(e)}>
          <ul className="home-wrapper">
            <FinderNavItem item={homeItem} isHomeActive={isHomeActive} />
          </ul>
          {additionalLeftTreeControls?.()?.renderAdditionalSection({
            viewState: sourcesViewState,
            sources: sources,
            location: this.context.location,
            canAddSource: this.getCanAddSource(),
          })}
          <SpacesSection
            isCollapsed={datasetsExpanded}
            isCollapsible
            onToggle={toggleDatasetsExpanded}
          />
          <div className="sources-title">
            {formatMessage({ id: "Source.Sources" })}
            {this.getCanAddSource() && (
              <IconButton
                className="add-source-button"
                tooltip={formatMessage({ id: "Source.AddSource" })}
                as={LinkWithRef}
                to={this.getAddSourceHref(true)}
                data-qa="add-sources"
              >
                <dremio-icon
                  name="interface/add-small"
                  class="add-space-icon"
                />
              </IconButton>
            )}
          </div>
          {!sourcesViewState.get("isInProgress") &&
            showEmptySourceContainer && (
              <EmptyStateContainer
                title="Sources.noSources"
                icon="interface/empty-add-data"
                linkInfo={
                  this.getCanAddSource()
                    ? {
                        href: this.getAddSourceHref(true),
                        "data-qa": "add-sources",
                        label: "Sources.AddSource.LowerCase",
                      }
                    : null
                }
              />
            )}
          {lakehouseSource.size > 0 && (
            <DataPlaneSection
              isCollapsed={lakehouseSourcesExpanded}
              isCollapsible
              onToggle={toggleLakehouseSourcesExpanded}
              dataPlaneSources={lakehouseSource}
              sourcesViewState={sourcesViewState}
            />
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
                  listHref={commonPaths.objectStorage.link({ projectId })}
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
                  listHref={commonPaths.external.link({ projectId })}
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
            <Button
              variant="tertiary"
              data-qa="add-source"
              onClick={() => browserHistory.push(this.getAddSourceHref(true))}
            >
              <dremio-icon name="interface/add-small" class="add-source-icon" />
              {formatMessage({
                id: "Source.AddSource",
              })}
            </Button>
          </div>
        )}
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    externalSourcesExpanded: state.ui.get("externalSourcesExpanded"),
    objectStorageSourcesExpanded: state.ui.get("objectStorageSourcesExpanded"),
    datasetsExpanded: state.ui.get("datasetsExpanded"),
    lakehouseSourcesExpanded: state.ui.get("lakehouseSourcesExpanded"),
    isAdmin: getAdminStatus(state),
    canCreateSource: state.privileges?.project?.canCreateSource,
  };
}

export default compose(
  withErrorBoundary({
    title: intl.formatMessage(
      { id: "Support.error.section" },
      {
        section: intl.formatMessage({
          id: "SectionLabel.catalog.sources.browser",
        }),
      },
    ),
  }),
  connect(mapStateToProps, {
    createSampleSource,
    fetchSupportFlags,
    toggleExternalSourcesExpanded,
    toggleObjectStorageSourcesExpanded,
    toggleDatasetsExpanded,
    toggleLakehouseSourcesExpanded,
  }),
)(LeftTree);
