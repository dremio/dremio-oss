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
import { IndexRedirect, Route } from "react-router";
import { UserIsAdmin } from "@app/components/Auth/authWrappers";
import AdminModals from "@app/pages/AdminPage/AdminModals";
import Page from "@app/components/Page";
import AdminPage from "@app/pages/AdminPage/AdminPage";
import Acceleration from "@inject/pages/AdminPage/subpages/acceleration/Acceleration";
import NodeActivity from "@app/pages/AdminPage/subpages/NodeActivity/NodeActivity";
import Users from "@app/pages/AdminPage/subpages/Users";
import Advanced from "@app/pages/AdminPage/subpages/Advanced";
import Provisioning from "@app/pages/AdminPage/subpages/Provisioning";
import Activation from "@inject/pages/AdminPage/subpages/Activation";
import Support from "@app/pages/AdminPage/subpages/Support";
import Queues from "@inject/pages/AdminPage/subpages/WLM/Queues";
import QAssignments from "@inject/pages/AdminPage/subpages/WLM/QAssignments";
import * as adminPaths from "dremio-ui-common/paths/admin.js";

export const AdminPageRouting = () => (
  <Route component={UserIsAdmin(AdminModals)}>
    <Route component={Page}>
      <Route path={adminPaths.admin.fullRoute()} component={AdminPage}>
        <IndexRedirect to={adminPaths.nodeActivity.fullRoute()} />
        <Route
          path={adminPaths.reflections.fullRoute()}
          component={Acceleration}
        />
        <Route
          path={adminPaths.nodeActivity.fullRoute()}
          component={NodeActivity}
        />
        <Route
          path={adminPaths.users.fullRoute()}
          component={UserIsAdmin(Users)}
        />
        <Route path={adminPaths.advanced.fullRoute()} component={Advanced} />
        <Route path={adminPaths.engines.fullRoute()} component={Provisioning} />
        <Route
          path={adminPaths.activation.fullRoute()}
          component={Activation}
        />
        <Route path={adminPaths.support.fullRoute()} component={Support} />
        <Route path={adminPaths.queues.fullRoute()} component={Queues} />
        <Route
          path={adminPaths.engineRouting.fullRoute()}
          component={QAssignments}
        />
      </Route>
    </Route>
  </Route>
);

export const EulaRoute = () => null;
