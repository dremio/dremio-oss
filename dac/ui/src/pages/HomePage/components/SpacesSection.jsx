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
import { connect } from "react-redux";
import Immutable from "immutable";
import { injectIntl } from "react-intl";
import { withRouter } from "react-router";
import { compose } from "redux";
import EmptyStateContainer from "@app/pages/HomePage/components/EmptyStateContainer";
import FinderNav from "@app/components/FinderNav";
import SpacesLoader from "@app/pages/HomePage/components/SpacesLoader";
import ViewStateWrapper, {
  viewStatePropType,
} from "@app/components/ViewStateWrapper";
import { ALL_SPACES_VIEW_ID } from "@app/actions/resources/spaces";
import { getViewState } from "@app/selectors/resources";
import { getSortedSpaces } from "@app/selectors/home";
import { FLEX_COL_START } from "@app/uiTheme/radium/flexStyle";
import { spacesSourcesListSpinnerStyle } from "@app/pages/HomePage/HomePageConstants";

import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import * as VersionUtils from "@app/utils/versionUtils";
import { getAdminStatus } from "dyn-load/pages/HomePage/components/modals/SpaceModalMixin";
import * as commonPaths from "dremio-ui-common/paths/common.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
const mapStateToProps = (state) => ({
  spaces: getSortedSpaces(state),
  spacesViewState: getViewState(state, ALL_SPACES_VIEW_ID),
  isAdmin: getAdminStatus(state),
});

export class SpacesSection extends PureComponent {
  static propTypes = {
    //#region react-redux

    spaces: PropTypes.instanceOf(Immutable.List).isRequired,
    spacesViewState: viewStatePropType,
    isAdmin: PropTypes.bool,

    //#endregion

    // injectIntl
    intl: PropTypes.object.isRequired,

    // withRouter
    location: PropTypes.object.isRequired,

    // collapsible
    isCollapsible: PropTypes.bool,
    isCollapsed: PropTypes.bool,
    onToggle: PropTypes.func,
  };

  getAddSpaceHref() {
    const { isAdmin } = this.props;
    let canAddSpace = localStorageUtils.getUserData()?.admin;

    if (VersionUtils.getEditionFromConfig() === "DCS") {
      canAddSpace = isAdmin;
    }

    return canAddSpace
      ? { ...this.props.location, state: { modal: "SpaceModal" } }
      : "";
  }

  getInitialSpacesContent() {
    const addHref = this.getAddSpaceHref();
    return this.props.spaces.size === 0 ? (
      <EmptyStateContainer
        title="Space.NoSpaces"
        icon="entities/empty-space"
        linkInfo={
          addHref && {
            href: addHref,
            "data-qa": "add-spaces",
            label: "Space.AddSpace",
          }
        }
      />
    ) : null;
  }

  render() {
    const {
      spacesViewState,
      spaces,
      intl,
      isCollapsed,
      isCollapsible,
      onToggle,
    } = this.props;

    return (
      <div className="left-tree-wrap">
        <SpacesLoader />
        <ViewStateWrapper
          viewState={spacesViewState}
          style={FLEX_COL_START}
          spinnerStyle={spacesSourcesListSpinnerStyle}
        >
          <FinderNav
            isCollapsed={isCollapsed}
            isCollapsible={isCollapsible}
            onToggle={onToggle}
            noMarginTop
            navItems={spaces}
            title={intl.formatMessage({ id: "Space.Spaces" })}
            addTooltip={intl.formatMessage({ id: "Space.AddSpace" })}
            isInProgress={spacesViewState.get("isInProgress")}
            addHref={this.getAddSpaceHref()}
            listHref={commonPaths.spacesList.link({
              projectId: getSonarContext()?.getSelectedProjectId?.(),
            })}
          >
            {this.getInitialSpacesContent()}
          </FinderNav>
        </ViewStateWrapper>
      </div>
    );
  }
}

export default compose(
  connect(mapStateToProps),
  withRouter,
  injectIntl
)(SpacesSection);
