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

import { connect } from "react-redux";
import { useIntl } from "react-intl";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { compose } from "redux";
import { withRouter } from "react-router";

/****************************************************/
/*                                                  */
/* THE COMMENTED OUT CODE IS FOR WIDE MODE, WHICH   */
/* FOR NOW IS HIDDEN                                */
/*                                                  */
/****************************************************/

import { getExploreState } from "@app/selectors/explore";
import { getLocation } from "selectors/routing";

import { showConfirmationDialog } from "actions/confirmation";
import { resetNewQuery } from "actions/explore/view";
import { EXPLORE_VIEW_ID } from "reducers/explore/view";
import { parseResourceId } from "utils/pathUtils";

import SideNavAdmin from "dyn-load/components/SideNav/SideNavAdmin";
import SideNavExtra from "dyn-load/components/SideNav/SideNavExtra";

import localStorageUtils, {
  useProjectContext,
} from "@inject/utils/storageUtils/localStorageUtils";
import getIconColor from "@app/utils/getIconColor";
import getUserIconInitials from "@app/utils/userIcon";
import ProjectActivationHOC from "@inject/containers/ProjectActivationHOC";

import SideNavHoverMenu from "./SideNavHoverMenu";
import AccountMenu from "./AccountMenu";
import "@app/components/IconFont/css/DremioIcons-old.css";
import "@app/components/IconFont/css/DremioIcons.css";

import { isActive } from "./SideNavUtils";
import HelpMenu from "./HelpMenu";
import { TopAction } from "./components/TopAction";
import CatalogsMenu from "./CatalogsMenu";
import clsx from "clsx";
import { isDcsEdition } from "dyn-load/utils/versionUtils";

import "./SideNav.less";

const getAppMode = ({ isArctic, isSonar, organization }) => {
  if (organization) {
    return "organization";
  }

  if (isArctic) {
    return "arctic";
  }

  if (isSonar) {
    return "sonar";
  }
};

const getLogo = (appMode) => {
  switch (appMode) {
    case "arctic":
      return "corporate/arctic";
    case "sonar":
      return "corporate/sonar";
    case "organization":
      return "corporate/dremio";
  }
};

const isArctic = () => window.location.pathname.indexOf("/arctic") === 0;

const SideNav = (props) => {
  const {
    socketIsOpen,
    user,
    router,
    location,
    currentSql,
    narwhalOnly,
    isProjectInactive,
    organization = false,
  } = props;

  const loc = location.pathname;
  const intl = useIntl();
  const ctx = useProjectContext();
  const appMode = getAppMode({
    isArctic:
      isArctic() ||
      (localStorageUtils ? localStorageUtils.isDataPlaneOnly(ctx) : false),
    isSonar: localStorageUtils ? localStorageUtils.isQueryEngine(ctx) : false,
    organization,
  });
  const isActiveProject =
    !isProjectInactive && (appMode === "sonar" || appMode === "arctic");
  const isActiveSonarProject = !isProjectInactive && appMode === "sonar";
  const isDCS = isDcsEdition();

  const logoSVG = getLogo(appMode);
  const { backgroundColor: userBgColor, color: userColor } = getIconColor(
    user.get("userId")
  );
  const userName = user.get("userName");
  const userNameFirst2 = getUserIconInitials(user);
  const userTooltip = intl.formatMessage({ id: "SideNav.User" }) + userName;

  const getNewQueryHref = () => {
    const resourceId = parseResourceId(location.pathname, user.get("userName"));
    return "/new_query?context=" + encodeURIComponent(resourceId);
  };

  const handleClick = (e) => {
    if (e.metaKey || e.ctrlKey) {
      // DX-10607, DX-11299 pass to default link behaviour, when cmd/ctrl is pressed on click
      return;
    }
    if (location.pathname === "/new_query") {
      if (currentSql && currentSql.trim()) {
        showConfirmationDialog({
          title: intl.formatMessage({ id: "Common.UnsavedWarning" }),
          text: [
            intl.formatMessage({ id: "NewQuery.UnsavedChangesWarning" }),
            intl.formatMessage({ id: "NewQuery.UnsavedChangesWarningPrompt" }),
          ],
          confirmText: intl.formatMessage({ id: "Common.Continue" }),
          cancelText: intl.formatMessage({ id: "Common.Cancel" }),
          confirm: () => {
            resetNewQuery(EXPLORE_VIEW_ID);
          },
        });
      } else {
        resetNewQuery(EXPLORE_VIEW_ID); // even if there's no SQL, clear any errors
      }
    } else {
      router.push(getNewQueryHref());
    }
    e.preventDefault();
  };

  const LogoAction = (
    <TopAction
      url="/"
      icon={logoSVG}
      alt="Logo"
      logo
      tooltip={false}
      socketIsOpen={appMode === "organization" || socketIsOpen}
      tooltipProps={{ placement: "right" }}
      className="dremioLogoWithTextContainer"
      iconClassName="dremioLogoWithText"
    />
  );

  // display only the company logo
  if (narwhalOnly) {
    return (
      <div className="sideNav">
        <div className="sideNav__topSection">{LogoAction}</div>
      </div>
    );
  }

  return (
    <div
      className={clsx(
        "sideNav",
        { arctic: appMode === "arctic" },
        { sonar: appMode === "sonar" },
        { organization: appMode === "organization" }
      )}
    >
      <div className="sideNav__topSection">
        {LogoAction}
        {((!isActiveProject && !isDCS) || isActiveProject) && (
          <TopAction
            tooltipProps={{ placement: "right" }}
            active={isActive({
              name: "/",
              dataset: true,
              loc,
              isArctic: appMode === "arctic",
            })}
            url={appMode === "arctic" ? `/arctic/${ctx?.id}/data` : "/"}
            icon="navigation-bar/dataset"
            alt="SideNav.Datasets"
          />
        )}
        {((appMode !== "arctic" && !isProjectInactive && !isDCS) ||
          (isActiveSonarProject && isDCS)) && (
          <>
            <TopAction
              tooltipProps={{ placement: "right" }}
              active={isActive({ name: "/new_query", loc, sql: true })}
              url={getNewQueryHref()}
              icon="navigation-bar/sql-runner"
              alt="SideNav.NewQuery"
              data-qa="new-query-button"
              onClick={() => handleClick}
            />
            <TopAction
              tooltipProps={{ placement: "right" }}
              active={isActive({ loc, jobs: true })}
              url="/jobs"
              icon="navigation-bar/jobs"
              alt="SideNav.Jobs"
              data-qa="select-jobs"
            />
          </>
        )}
        {isActiveProject && isDCS && (
          <TopAction
            tooltipProps={{ placement: "right" }}
            active={isActive({ name: "/admin", loc, admin: true })}
            url={
              appMode === "arctic" ? `/arctic/${ctx?.id}/settings` : "/settings"
            }
            icon="interface/settings"
            alt={`SideNav.${
              appMode === "arctic" ? "Catalog" : "Project"
            }Settings`}
            data-qa="settings"
          />
        )}
      </div>

      <div className="sideNav__bottomSection">
        {isDCS && <hr className="sideNav__bottomSection__divider" />}
        {!isDCS && (
          <>
            <SideNavExtra />
            <SideNavAdmin user={user} />
          </>
        )}
        {isDCS && (
          <>
            <SideNavHoverMenu
              tooltipStringId="SideNav.DremioServices"
              menu={<CatalogsMenu />}
              icon="navigation-bar/go-to-catalogs"
              isDCS={isDCS}
            />
            {appMode === "arctic" ||
              (appMode === "sonar" && (
                <TopAction
                  tooltipProps={{ placement: "right" }}
                  url="/"
                  icon="navigation-bar/organization"
                  alt={intl.formatMessage({ id: "SideNav.Organization" })}
                  data-qa="go-to-landing-page"
                />
              ))}
          </>
        )}
        <SideNavHoverMenu
          tooltipStringId="SideNav.Help"
          menu={<HelpMenu />}
          icon="interface/help"
          isDCS={isDCS}
        />
        <SideNavHoverMenu
          aria-label="User options"
          tooltipString={userTooltip}
          menu={<AccountMenu />}
          isDCS={isDCS}
          divBlob={
            <div className="sideNav-item__customHoverMenu">
              <div className="sideNav-items">
                <div
                  className="sideNav__user sideNav-item__dropdownIcon"
                  style={{ backgroundColor: userBgColor, color: userColor }}
                  data-qa="navigation-bar/user-settings"
                >
                  <span>{userNameFirst2}</span>
                </div>
              </div>
            </div>
          }
        />
      </div>
    </div>
  );
};

SideNav.propTypes = {
  narwhalOnly: PropTypes.bool,
  location: PropTypes.object.isRequired,
  currentSql: PropTypes.string,
  user: PropTypes.instanceOf(Immutable.Map),
  socketIsOpen: PropTypes.bool.isRequired,
  showConfirmationDialog: PropTypes.func,
  resetNewQuery: PropTypes.func,
  isProjectInactive: PropTypes.bool,
  router: PropTypes.shape({
    isActive: PropTypes.func,
    push: PropTypes.func,
  }),
};

const mapStateToProps = (state) => {
  const explorePage = getExploreState(state); //todo explore page state should not be here
  return {
    user: state.account.get("user"),
    socketIsOpen: state.serverStatus.get("socketIsOpen"),
    location: getLocation(state),
    currentSql: explorePage ? explorePage.view.currentSql : null,
  };
};

const mapDispatchToProps = {
  showConfirmationDialog,
  resetNewQuery,
};

export default compose(
  withRouter,
  connect(mapStateToProps, mapDispatchToProps)
)(ProjectActivationHOC(SideNav));
