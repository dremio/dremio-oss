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

export default function () {
  return Promise.resolve(
    [
      {
        icon: "settings/node-activity",
        title: "Admin.Engines.NodeActivity",
        url: "/admin/nodeActivity",
      },
      {
        icon: "settings/engines",
        title: "Admin.Engines.ElasticEngines",
        url: "/admin/provisioning",
      },
      {
        icon: "settings/support",
        title: "Admin.Engines.Support",
        url: "/admin/support",
      },
      {
        icon: "settings/queue-control",
        title: "Admin.Engines.QueueControl",
        url: "/admin/advanced",
      },
      {
        icon: "settings/users",
        title: "Admin.UserManagement.Users",
        url: "/admin/users",
      },
    ].filter(Boolean)
  );
}

export const getTitle = () => {
  return "Settings";
};
