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

import { getLocation } from "selectors/routing";

import { showConfirmationDialog } from "actions/confirmation";
import { resetNewQuery } from "actions/explore/view";
import { parseResourceId } from "utils/pathUtils";

import SideNavAdmin from "dyn-load/components/SideNav/SideNavAdmin";
import SideNavExtra from "dyn-load/components/SideNav/SideNavExtra";

import localStorageUtils, {
  useProjectContext,
} from "@inject/utils/storageUtils/localStorageUtils";
import ProjectActivationHOC from "@inject/containers/ProjectActivationHOC";
import { usePrivileges } from "@inject/utils/sideNavUtils";
import SideNavHoverMenu from "./SideNavHoverMenu";
import AccountMenu from "./AccountMenu";
import "@app/components/IconFont/css/DremioIcons-old.css";
import "@app/components/IconFont/css/DremioIcons.css";
import { Avatar } from "dremio-ui-lib/components";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";
import { isActive } from "./SideNavUtils";
import HelpMenu from "./HelpMenu";
import { TopAction } from "./components/TopAction";
import clsx from "clsx";
import * as PATHS from "../../exports/paths";
import CatalogsMenu from "./CatalogsMenu";
import { useFeatureFlag } from "@app/exports/providers/useFeatureFlag";
import { ARCTIC_CATALOG } from "@app/exports/flags/ARCTIC_CATALOG";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import * as commonPaths from "dremio-ui-common/paths/common.js";
import * as jobPaths from "dremio-ui-common/paths/jobs.js";
import * as orgPaths from "dremio-ui-common/paths/organization.js";
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
import * as adminPaths from "dremio-ui-common/paths/admin.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import "./SideNav.less";
import { getSessionContext } from "dremio-ui-common/contexts/SessionContext.js";

const SideNav = (props) => {
  const {
    socketIsOpen,
    user,
    router,
    location,
    narwhalOnly,
    isProjectInactive,
    className,
    headerAction,
    actions = null,
    showOrganization = true,
  } = props;

  const organizationLanding =
    typeof getSessionContext().getOrganizationId === "function";

  useFeatureFlag(ARCTIC_CATALOG);
  //urlability
  const loc = rmProjectBase(location.pathname) || "/";
  const projectId =
    router.params?.projectId || getSonarContext()?.getSelectedProjectId?.();
  usePrivileges(projectId);
  const intl = useIntl();
  const ctx = useProjectContext();
  const isDDPOnly = localStorageUtils
    ? localStorageUtils.isDataPlaneOnly(ctx)
    : false;
  const logoSVG = isDDPOnly ? "corporate/dremio-ddp" : "corporate/dremio";
  const userName = user.get("userName");
  const userTooltip = intl.formatMessage({ id: "SideNav.User" }) + userName;

  const getNewQueryHref = () => {
    const resourceId = parseResourceId(location.pathname, user.get("userName"));
    return sqlPaths.newQuery.link({
      resourceId,
      projectId,
    });
  };

  const LogoAction = headerAction || (
    <TopAction
      url={commonPaths.projectBase.link({ projectId })}
      icon={logoSVG}
      alt="Logo"
      logo
      tooltip={false}
      socketIsOpen={socketIsOpen}
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
    <div className={clsx("sideNav", className)}>
      <div className="sideNav__topSection">
        {LogoAction}
        {actions}
        {actions === null && !isProjectInactive && (
          <TopAction
            tooltipProps={{ placement: "right" }}
            active={isActive({ name: "/", dataset: true, loc, isDDPOnly })}
            url={commonPaths.projectBase.link({ projectId })}
            icon="navigation-bar/dataset"
            alt="SideNav.Datasets"
          />
        )}
        {actions === null && !isDDPOnly && !isProjectInactive && (
          <>
            <TopAction
              tooltipProps={{ placement: "right" }}
              active={isActive({ name: PATHS.newQuery(), loc, sql: true })}
              url={getNewQueryHref()}
              icon="navigation-bar/sql-runner"
              alt="SideNav.NewQuery"
              data-qa="new-query-button"
            />
            <TopAction
              tooltipProps={{ placement: "right" }}
              active={isActive({ loc, jobs: true })}
              url={jobPaths.jobs.link({ projectId })}
              icon="navigation-bar/jobs"
              alt="SideNav.Jobs"
              data-qa="select-jobs"
            />
            {organizationLanding && (
              <TopAction
                tooltipProps={{ placement: "right" }}
                active={isActive({ loc, admin: true })}
                url={adminPaths.general.link({ projectId })}
                icon="interface/settings"
                alt="SideNav.AdminMenuProjectSetting"
                data-qa="select-admin-settings"
              />
            )}
          </>
        )}
      </div>

      <div className="sideNav__bottomSection">
        {!organizationLanding && <SideNavExtra />}
        {organizationLanding && (
          <SideNavHoverMenu
            tooltipStringId="SideNav.DremioServices"
            menu={<CatalogsMenu />}
            icon="navigation-bar/go-to-catalogs"
            isDCS={true}
          />
        )}
        {organizationLanding &&
          (showOrganization ? (
            <TopAction
              tooltipProps={{ placement: "right" }}
              url={orgPaths.organization.link()}
              icon="navigation-bar/organization"
              alt={intl.formatMessage({ id: "SideNav.Organization" })}
              data-qa="go-to-landing-page"
            />
          ) : null)}

        {!organizationLanding && <SideNavAdmin user={user} />}

        <SideNavHoverMenu
          tooltipStringId={"SideNav.Help"}
          menu={<HelpMenu />}
          icon={"interface/help"}
        />
        <SideNavHoverMenu
          aria-label="User options"
          tooltipString={userTooltip}
          menu={<AccountMenu />}
          divBlob={<Avatar initials={nameToInitials(userName)} />}
        />
      </div>
    </div>
  );
};

SideNav.propTypes = {
  narwhalOnly: PropTypes.bool,
  location: PropTypes.object.isRequired,
  user: PropTypes.instanceOf(Immutable.Map),
  socketIsOpen: PropTypes.bool.isRequired,
  showConfirmationDialog: PropTypes.func,
  resetNewQuery: PropTypes.func,
  isProjectInactive: PropTypes.bool,
  router: PropTypes.shape({
    isActive: PropTypes.func,
    push: PropTypes.func,
    params: PropTypes.object,
  }),
  className: PropTypes.string,
  headerAction: PropTypes.any,
  actions: PropTypes.any,
  showOrganization: PropTypes.bool,
};

const mapStateToProps = (state) => {
  return {
    user: state.account.get("user"),
    socketIsOpen: state.serverStatus.get("socketIsOpen"),
    location: getLocation(state),
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
