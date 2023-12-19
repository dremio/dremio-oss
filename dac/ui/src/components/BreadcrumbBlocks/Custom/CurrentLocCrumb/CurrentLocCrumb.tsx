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
import BreadcrumbLink from "../../Common/BreadcrumbLink/BreadcrumbLink";
import exploreUtils from "@app/utils/explore/exploreUtils";
import { connect } from "react-redux";
import { withRouter, WithRouterProps } from "react-router";
import { intl } from "@app/utils/intl";
import * as PATHS from "@app/exports/paths";
//@ts-ignore
import * as commonPaths from "dremio-ui-common/paths/common";
//@ts-ignore
import * as adminPaths from "dremio-ui-common/paths/admin";
//@ts-ignore
import * as jobPaths from "dremio-ui-common/paths/jobs";
//@ts-ignore
import * as orgPaths from "dremio-ui-common/paths/organization";
//@ts-ignore
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { parseResourceId } from "utils/pathUtils";

type CurrentLocCrumbProps = {
  branch?: Record<string, any>;
  user: any;
};

const CurrentLocCrumb = (props: WithRouterProps & CurrentLocCrumbProps) => {
  let text, to, iconName;
  const { formatMessage } = intl;
  const {
    location: { pathname: curPath },
    branch,
    user,
  } = props;
  const pathname = rmProjectBase(curPath) || "/";
  const splitPath = pathname.split("/").slice(1);
  const branchName = branch?.reference?.name;
  const projectId = getSonarContext()?.getSelectedProjectId?.();

  const getNewQueryHref = () => {
    const resourceId = parseResourceId(
      location.pathname,
      `@${user?.get("userName")}`
    );
    return sqlPaths.newQuery.link({
      resourceId,
      projectId,
    });
  };

  // Dataset Paths
  if (
    curPath.startsWith(commonPaths.projectBase.link({ projectId })) ||
    pathname.indexOf("/home") === 0 ||
    pathname.indexOf("/sources") === 0 ||
    pathname.indexOf("/source") === 0 ||
    pathname.indexOf("/space") === 0 ||
    pathname.indexOf("/spaces") === 0
  ) {
    text = formatMessage({ id: "Common.Datasets" });
    to = commonPaths.projectBase.link({ projectId });
    iconName = "navigation-bar/dataset";
  }

  // Sql editor Paths
  if (exploreUtils.isSqlEditorTab(location)) {
    text = formatMessage({ id: "SQL.SQLEditor" });
    to = getNewQueryHref();
    iconName = "navigation-bar/sql-runner";
  }

  // Jobs paths
  if (
    curPath.startsWith(jobPaths.jobs.link({ projectId })) ||
    pathname.indexOf("/job") === 0
  ) {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { attempts: _, ...query } = props.location.query || {};
    text = formatMessage({ id: "Job.Jobs" });
    to = {
      pathname: jobPaths.jobs.link({ projectId }),
      hash: props.location.hash,
      query,
    };
    iconName = "navigation-bar/jobs";
  }

  // Project settings path
  if (splitPath[0] === "settings") {
    text = formatMessage({ id: "Admin.Settings.Projects" });
    to = adminPaths.general.link({ projectId });
    iconName = "interface/settings";
  }

  // Org settings path
  if (splitPath[1] === "settings") {
    text = formatMessage({ id: "Admin.Organization.Setting" });
    to = orgPaths.general.link();
    iconName = "interface/settings";
  }

  // Catalog paths
  if (pathname.indexOf("/arctic") === 0) {
    text = formatMessage({ id: "Common.Catalog" });
    to = `${PATHS.arcticCatalogDataBase({
      arcticCatalogId: splitPath[1],
    })}/${branchName}`;
    iconName = "navigation-bar/catalog";
  }

  // Catalog Settings path
  if (splitPath[2] === "settings") {
    text = formatMessage({ id: "Settings.Catalog" });
    to = PATHS.arcticCatalogSettings({ arcticCatalogId: splitPath[1] });
    iconName = "interface/settings";
  }

  // Catalog Settings path
  if (splitPath[2] === "jobs") {
    text = formatMessage({ id: "SideNav.Jobs" });
    to = PATHS.arcticCatalogJobs({ arcticCatalogId: splitPath[1] });
    iconName = "navigation-bar/jobs";
  }

  return (
    text &&
    to &&
    iconName && <BreadcrumbLink text={text} to={to} iconName={iconName} />
  );
};

const mapStateToProps = (state: Record<string, any>) => {
  if (Object.keys(state.nessie).length === 0) {
    return {};
  }
  return {
    user: state.account.get("user"),
    branch: state.nessie[Object.keys(state.nessie)[0]],
  };
};

// @ts-ignore
export default withRouter(connect(mapStateToProps, {})(CurrentLocCrumb));
