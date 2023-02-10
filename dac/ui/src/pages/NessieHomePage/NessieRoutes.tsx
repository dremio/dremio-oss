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
import { IndexRoute, Redirect, Route, IndexRedirect } from "react-router";
import NamespaceTable from "./components/NamespaceTable/NamespaceTable";
import TableDetailsPage from "./components/TableDetailsPage/TableDetailsPage";
import RepoView from "./components/RepoView/RepoView";
import CommitDetailsPage from "./components/CommitDetailsPage/CommitDetailsPage";
import BranchHistory from "./components/BranchHistory/BranchHistory";
import NessieProjectHomePage from "./components/NessieProjectHomePage/NessieProjectHomePage";
import NessieSourceHomePage from "./components/NessieSourceHomePage/NessieSourceHomePage";
import ArcticSourceWithNessie from "@app/exports/pages/ArcticSource/ArcticSource";
import { ArcticSourceRoutes } from "@inject/additionalRequiredRoutes";

import * as PATHS from "@app/exports/paths";

const CommonRoutes = [
  <Route key="branches" path="branches" component={RepoView} />,
  <Route
    key="commitDetails"
    path="commit/:branchName/:commitHash"
    component={CommitDetailsPage}
  />,
  <Route
    key="branchHistory"
    path="branches/:branchName"
    component={BranchHistory}
  />,
  <Route key="tableDetails" path="table/*" component={TableDetailsPage} />,
  <Route key="viewDetails" path="view/*" component={TableDetailsPage} />,
];

function nessieRoutes() {
  return (
    <Route component={NessieProjectHomePage}>
      <IndexRoute component={NamespaceTable} />
      <Route path="namespace/*" component={NamespaceTable} />
      {CommonRoutes}
    </Route>
  );
}

export function nessieSourceRoutes() {
  return [
    <Redirect
      key="nessieSourceRoutes"
      from="/sources/dataplane/:sourceId"
      to="/sources/dataplane/:sourceId/branches"
    />,
    <Route
      key="nessieSourceHomePage"
      path="/sources/dataplane/:sourceId"
      component={NessieSourceHomePage}
    >
      {CommonRoutes}
    </Route>,
  ];
}

export function arcticSourceRoutes() {
  return [
    <Route
      key="arcticSourceHomePage"
      path={PATHS.arcticSourceBase({
        sourceId: ":sourceId",
        projectId: ":projectId",
      })}
      component={ArcticSourceWithNessie}
    >
      <IndexRedirect
        to={PATHS.arcticSourceCommitsBase({
          sourceId: ":sourceId",
          projectId: ":projectId",
        })}
      />
      {ArcticSourceRoutes}
      <Route
        key="arctic-not-found"
        path={`${PATHS.arcticSourceBase({
          sourceId: ":sourceId",
          projectId: ":projectId",
        })}/*`}
        component={() => null}
      />
    </Route>,
  ];
}

export default nessieRoutes;
