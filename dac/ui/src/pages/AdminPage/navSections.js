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
import * as adminPaths from "dremio-ui-common/paths/admin.js";

export default function () {
  return Promise.resolve(
    [
      {
        icon: "settings/node-activity",
        title: "Admin.Engines.NodeActivity",
        url: adminPaths.nodeActivity.link(),
      },
      {
        icon: "settings/engines",
        title: "Admin.Engines.ElasticEngines",
        url: adminPaths.engines.link(),
      },
      {
        icon: "settings/support",
        title: "Admin.Engines.Support",
        url: adminPaths.support.link(),
      },
      {
        icon: "settings/queue-control",
        title: "Admin.Engines.QueueControl",
        url: adminPaths.advanced.link(),
      },
      {
        icon: "settings/users",
        title: "Admin.UserManagement.Users",
        url: adminPaths.users.link(),
      },
    ].filter(Boolean),
  );
}

export const getTitle = () => {
  return "Settings";
};
