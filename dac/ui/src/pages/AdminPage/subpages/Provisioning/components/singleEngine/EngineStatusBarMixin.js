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

import { CLUSTER_STATE } from '@app/constants/provisioningPage/provisioningConstants';

export const getItems = (statusCounts) => {
  const items = [
    {status: CLUSTER_STATE.running, label: la('Online'), count: statusCounts.active},
    {status: CLUSTER_STATE.starting, label: la('Starting'), count: statusCounts.pending},
    {status: CLUSTER_STATE.provisioning, label: la('Provisioning'), count: statusCounts.disconnected},
    {status: CLUSTER_STATE.stopping, label: la('Stopping'), count: statusCounts.decommissioning}
  ];

  return items;
};
