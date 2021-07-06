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
import { IndexRedirect, Route } from 'react-router';
import { UserIsAdmin } from '@app/components/Auth/authWrappers';
import AdminModals from '@app/pages/AdminPage/AdminModals';
import Page from '@app/components/Page';
import AdminPage from '@app/pages/AdminPage/AdminPage';
import Acceleration from '@inject/pages/AdminPage/subpages/acceleration/Acceleration';
import NodeActivity from '@app/pages/AdminPage/subpages/NodeActivity/NodeActivity';
import Users from '@app/pages/AdminPage/subpages/Users';
import Roles from '@inject/pages/AdminPage/subpages/Roles';
import Advanced from '@app/pages/AdminPage/subpages/Advanced';
import Provisioning from '@app/pages/AdminPage/subpages/Provisioning';
import Activation from '@inject/pages/AdminPage/subpages/Activation';
import Support from '@app/pages/AdminPage/subpages/Support';
import Queues from '@inject/pages/AdminPage/subpages/WLM/Queues';
import QAssignments from '@inject/pages/AdminPage/subpages/WLM/QAssignments';

export const AdminPageRouting = () => (
  <Route component={UserIsAdmin(AdminModals)}>
    <Route component={Page}>
      <Route path='/admin' component={AdminPage} >
        <IndexRedirect to='/admin/nodeActivity' />
        <Route path='/admin/acceleration' component={Acceleration} />
        <Route path='/admin/nodeActivity' component={NodeActivity} />
        <Route path='/admin/users' component={UserIsAdmin(Users)} />
        <Route path='/admin/roles' component={Roles} />
        <Route path='/admin/advanced' component={Advanced} />
        <Route path='/admin/provisioning' component={Provisioning} />
        <Route path='/admin/activation' component={Activation}/>
        <Route path='/admin/support' component={Support} />
        <Route path='/admin/queues' component={Queues} />
        <Route path='/admin/rules' component={QAssignments} />
      </Route>
    </Route>
  </Route>
);
