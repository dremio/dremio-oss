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
import React from 'react';
import { IndexRoute, Redirect, Route } from 'react-router';
import NamespaceTable from './components/NamespaceTable/NamespaceTable';
import TableDetailsPage from './components/TableDetailsPage/TableDetailsPage';
import RepoView from './components/RepoView/RepoView';
import CommitDetailsPage from './components/CommitDetailsPage/CommitDetailsPage';
import BranchHistory from './components/BranchHistory/BranchHistory';
import NessieProjectHomePage from './components/NessieProjectHomePage/NessieProjectHomePage';
import NessieSourceHomePage from './components/NessieSourceHomePage/NessieSourceHomePage';

const CommonRoutes = [
  <Route path='branches' component={RepoView} />,
  <Route path='commit/:branchName/:commitHash' component={CommitDetailsPage} />,
  <Route path='branches/:branchName' component={BranchHistory} />,
  <Route path='table/:id' component={TableDetailsPage} />
];

function nessieRoutes() {
  return (
    <Route component={NessieProjectHomePage}>
      <IndexRoute component={NamespaceTable} />
      <Route path='namespace/:id' component={NamespaceTable} />
      {CommonRoutes}
    </Route>
  );
}

export function nessieSourceRoutes() {
  return [
    <Redirect from='/sources/dataplane/:sourceId' to='/sources/dataplane/:sourceId/branches' />,
    <Route path='/sources/dataplane/:sourceId' component={NessieSourceHomePage}>
      {CommonRoutes}
    </Route>
  ];
}

export default nessieRoutes;
