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
package com.dremio.provision.service;

import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterType;

/**
 * Interface to provide implementations for different Provisioning services
 * to support general APIs to handle Dremio deployment
 */
public interface ProvisioningServiceDelegate {

  /**
   * Get clusterType API
   * @return {@link ClusterType}
   */
  ClusterType getType();

  /**
   * Start created cluster API
   * @param cluster
   * @return {@link Cluster}
   * @throws Exception
   */
  ClusterEnriched startCluster(Cluster cluster) throws ProvisioningHandlingException;

  /**
   * Resize cluster API
   * @param cluster
   * @return {@link Cluster}
   * @throws Exception
   */
  ClusterEnriched resizeCluster(Cluster cluster) throws ProvisioningHandlingException;

  /**
   * Stop cluster API
   * @param cluster
   * @throws Exception
   */
  void stopCluster(Cluster cluster) throws ProvisioningHandlingException;

  /**
   * Get all the info about the cluster API
   * @param cluster
   * @return {@link Cluster}
   * @throws Exception
   */
  ClusterEnriched getClusterInfo(Cluster cluster) throws ProvisioningHandlingException;
}
