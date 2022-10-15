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
import * as React from "react";
import { IndexRedirect, Route } from "react-router";
import { type RouteObject } from "react-router6";
import { OrgLanding } from "./pages/OrgLanding/OrgLanding";
import { ArcticCatalogs } from "./pages/ArcticCatalogs/ArcticCatalogs";
import { SonarProjects } from "./pages/SonarProjects/SonarProjects";
import ArcticCatalogWithNessie from "./pages/ArcticCatalog/ArcticCatalog";
import * as PATHS from "./paths";
import { OrganizationProvider } from "./providers/OrganizationProvider";
import ArcticCatalogData from "./pages/ArcticCatalog/components/ArcticCatalogData/ArcticCatalogData";
import ArcticCatalogCommits from "./pages/ArcticCatalog/components/ArcticCatalogCommits/ArcticCatalogCommits";
import ArcticCatalogTags from "./pages/ArcticCatalog/components/ArcticCatalogTags/ArcticCatalogTags";
import ArcticCommitDetails from "./pages/ArcticCatalog/components/ArcticCommitDetails/ArcticCommitDetails";
import RepoView from "../../src/pages/NessieHomePage/components/RepoView/RepoView";
import { FeatureSwitch } from "./components/FeatureSwitch/FeatureSwitch";
import { ARCTIC_CATALOG } from "./flags/ARCTIC_CATALOG";
import { NotFoundPage } from "./pages/NotFoundPage";
import { ORGANIZATION_LANDING } from "./flags/ORGANIZATION_LANDING";

export const routes: RouteObject[] = [
  {
    path: PATHS.arcticCatalogs(),
    element: (
      <FeatureSwitch
        flag={ARCTIC_CATALOG}
        renderEnabled={() => <ArcticCatalogs />}
        renderDisabled={() => <NotFoundPage />}
      />
    ),
  },
  {
    path: PATHS.sonarProjects(),
    element: <SonarProjects />,
  },
  {
    path: PATHS.organization(),
    element: (
      <FeatureSwitch
        flag={ORGANIZATION_LANDING}
        renderEnabled={() => (
          <OrganizationProvider>
            {({ organization }: any) => (
              <OrgLanding organization={organization} />
            )}
          </OrganizationProvider>
        )}
        renderDisabled={() => <NotFoundPage />}
      />
    ),
  },
].filter(Boolean);

const ArcticCatalogRoutes = [
  <Route
    key="arctic-catalog"
    path={PATHS.arcticCatalog({ arcticCatalogId: ":arcticCatalogId" })}
    component={ArcticCatalogWithNessie}
  >
    <IndexRedirect
      to={PATHS.arcticCatalogDataBase({ arcticCatalogId: ":arcticCatalogId" })}
    />
    <Route
      path={PATHS.arcticCatalogDataBase({
        arcticCatalogId: ":arcticCatalogId",
      })}
      component={ArcticCatalogData}
    />
    <Route
      path={PATHS.arcticCatalogData({
        arcticCatalogId: ":arcticCatalogId",
        branchId: ":branchName",
      })}
      component={ArcticCatalogData}
    />
    <Route
      path={PATHS.arcticCatalogData({
        arcticCatalogId: ":arcticCatalogId",
        branchId: ":branchName",
        namespace: "*",
      })}
      component={ArcticCatalogData}
    />
    <Route
      path={PATHS.arcticCatalogCommitsBase({
        arcticCatalogId: ":arcticCatalogId",
      })}
      component={ArcticCatalogCommits}
    />
    <Route
      path={PATHS.arcticCatalogCommits({
        arcticCatalogId: ":arcticCatalogId",
        branchId: ":branchName",
      })}
      component={ArcticCatalogCommits}
    />
    <Route
      path={PATHS.arcticCatalogCommits({
        arcticCatalogId: ":arcticCatalogId",
        branchId: ":branchName",
        namespace: "*",
      })}
      component={ArcticCatalogCommits}
    />
    <Route
      path={PATHS.arcticCatalogCommit({
        arcticCatalogId: ":arcticCatalogId",
        branchId: ":branchName",
        commitId: ":commitId",
      })}
      component={ArcticCommitDetails}
    />
    <Route
      path={PATHS.arcticCatalogTags({ arcticCatalogId: ":arcticCatalogId" })}
      component={ArcticCatalogTags}
    />
    <Route
      path={PATHS.arcticCatalogBranches({
        arcticCatalogId: ":arcticCatalogId",
      })}
      component={() => <RepoView showHeader={false} />}
    />
    <Route
      path={PATHS.arcticCatalogSettings({
        arcticCatalogId: ":arcticCatalogId",
      })}
      component={() => <div>Settings go here</div>}
    />
    <Route
      key="arctic-not-found"
      path={`${PATHS.arcticCatalog({ arcticCatalogId: ":arcticCatalogId" })}/*`}
      component={() => null}
    />
  </Route>,
];
export const ArcticSourceRoutes = [
  <Route
    key="commits"
    path={PATHS.arcticSourceCommitsNonBase()}
    component={ArcticCatalogCommits}
  />,
  <Route
    key="commits-branch-namespace"
    path={PATHS.arcticSourceCommits({ branchId: ":branchName" })}
    component={ArcticCatalogCommits}
  />,
  <Route
    key="commits-namespace"
    path={PATHS.arcticSourceCommits({
      branchId: ":branchName",
      namespace: "*",
    })}
    component={ArcticCatalogCommits}
  />,
  <Route
    key="commitDetails"
    path={PATHS.arcticSourceCommit({
      branchId: ":branchName",
      commitId: ":commitId",
    })}
    component={ArcticCommitDetails}
  />,
  <Route
    key="tags"
    path={PATHS.arcticSourceTagsNonBase()}
    component={ArcticCatalogTags}
  />,
  <Route
    key="branches"
    path={PATHS.arcticSourceBranchesNonBase()}
    component={() => <RepoView showHeader={false} />}
  />,
];
export const renderedRoutes: JSX.Element = (
  <>
    {routes.map((route) => (
      <Route
        key={route.path}
        path={route.path}
        component={() => route.element as React.ReactElement}
      />
    ))}
    {ArcticCatalogRoutes}
  </>
);
