/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import ApiUtils from 'utils/apiUtils/apiUtils';

export default {
  fetchClusterStats() {
    return ApiUtils.fetch('cluster/stats').then((response) => {
      const endpoints = new Map();

      // anonymize the endpoint - give it a simple index based id and remove the address.
      function anonymize(endpoint) {
        if (!endpoints.has(endpoint.address)) {
          endpoints.set(endpoint.address, endpoints.size);
        }

        const index = endpoints.get(endpoint.address);
        endpoint.id = index;
        delete endpoint.address;
      }

      return response.json().then((data) => {
        data.coordinators.forEach(anonymize);
        data.executors.forEach(anonymize);

        return data;
      });
    });
  }
};
