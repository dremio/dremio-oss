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
package com.dremio.service.coordinator;

import com.dremio.service.Service;

/** Interface to cluster service set manager. */
public interface ClusterServiceSetManager extends Service {
  /**
   * Get a provider which returns the up-to-date list of endpoints for a given role.
   *
   * @param role the role to look up for
   * @return a provider for a collection of endpoints
   * @throws NullPointerException if role is {@code null}
   */
  ServiceSet getServiceSet(ClusterCoordinator.Role role);

  /**
   * Get or create a {@link ServiceSet} for the given service name
   *
   * @param serviceName
   * @return
   */
  ServiceSet getOrCreateServiceSet(String serviceName);

  /**
   * Delete a {@link ServiceSet} for the given service name
   *
   * @param serviceName
   */
  void deleteServiceSet(String serviceName);

  /**
   * Get the set of service names registered in the ClusterCoordinator ServiceSet. NOTE: There is no
   * guarantee of return object consistency depending on how Dremio is tracking the registered
   * serivces.
   *
   * @return An Iterable of service names.
   */
  Iterable<String> getServiceNames() throws Exception;
}
