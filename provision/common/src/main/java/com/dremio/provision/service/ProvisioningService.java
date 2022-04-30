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
package com.dremio.provision.service;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterConfig;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterId;
import com.dremio.provision.ClusterState;
import com.dremio.provision.ClusterType;
import com.dremio.service.Service;

/**
 * Interface for provisioning services implementations
 * We will start with YARN, but could/will extend to Mesos, AWS, Kubernetes
 * APIs here should be async. and return data that is available at the time of API call
 * subsequent calls to the "getter" APIs will return more info as available
 */
public interface ProvisioningService extends Service {
  /**
   * Property for configuring container heap size (in MB)
   */
  public static final String YARN_HEAP_SIZE_MB_PROPERTY = "provisioning.yarn.heapsize";

  /**
   * Create Cluster configuration API as well as starting cluster
   * @param clusterconfig
   * @return {@link ClusterEnriched}
   * @throws ProvisioningHandlingException
   * @throws NullPointerException when null preconditions are not met
   */
  ClusterEnriched createCluster(ClusterConfig clusterconfig) throws ProvisioningHandlingException;

  /**
   * Modify Cluster configuration API. Only modifies KVStore
   * @param clusterId
   * @param desiredState
   * @param clusterconfig
   * @return {@link ClusterEnriched}
   * @throws ProvisioningHandlingException
   */
  ClusterEnriched modifyCluster(ClusterId clusterId, ClusterState desiredState, ClusterConfig clusterconfig) throws
    ProvisioningHandlingException;

  /**
   * Start created cluster API
   * @param clusterId
   * @return {@link ClusterEnriched}
   * @throws ProvisioningHandlingException
   * @throws NullPointerException when null preconditions are not met
   */
  ClusterEnriched startCluster(ClusterId clusterId) throws ProvisioningHandlingException;

  /**
   * Resize cluster API
   * @param newContainersCount
   * @return {@link ClusterEnriched}
   * @throws ProvisioningHandlingException
   * @throws NullPointerException when null preconditions are not met
   */
  ClusterEnriched resizeCluster(ClusterId clusterId, int newContainersCount) throws ProvisioningHandlingException;

  /**
   * Stop cluster API
   * @param clusterId
   * @return {@link ClusterEnriched}
   * @throws ProvisioningHandlingException
   * @throws NullPointerException when null preconditions are not met
   */
  ClusterEnriched stopCluster(ClusterId clusterId) throws ProvisioningHandlingException;

  /**
   * Delete cluster configuration API
   * @param id
   * @throws ProvisioningHandlingException
   * @throws NullPointerException when null preconditions are not met
   */
  void deleteCluster(ClusterId id) throws ProvisioningHandlingException;

  /**
   * Get all the info about the cluster API
   * @param id
   * @return {@link ClusterEnriched}
   * @throws ProvisioningHandlingException
   * @throws NullPointerException when null preconditions are not met
   */
  ClusterEnriched getClusterInfo(ClusterId id) throws ProvisioningHandlingException;

  /**
   * Return info about all the clusters API
   * @return Iterable<ClusterEnriched>
   * @throws ProvisioningHandlingException
   */
  Iterable<ClusterEnriched> getClustersInfo() throws ProvisioningHandlingException;

  /**
   * Return info about clusters of a particular {@link ClusterType}
   * @param type
   * @return Iterable<ClusterEnriched>
   * @throws ProvisioningHandlingException
   */
  Iterable<ClusterEnriched> getClusterInfoByType(ClusterType type) throws ProvisioningHandlingException;

  /**
   * Get all the clusters of a particular type in a particular state
   * @param type
   * @param state
   * @return Iterable<ClusterEnriched>
   * @throws ProvisioningHandlingException
   */
  Iterable<ClusterEnriched> getClusterInfoByTypeByState(ClusterType type, ClusterState state) throws ProvisioningHandlingException;

  /**
   * Given a name, start the cluster(s) if they can be autostarted.
   * @param name The name of the cluster.
   * @return A future that notifies when the cluster is up or throws ActionDisallowed, NoClusterException as appropriate.
   */
  CompletableFuture<Void> autostartCluster(String name);

  void restartPreviewEngine();

  /**
   * Stop the collection of clusterIds if possible. No exceptions are thrown if the stop is disallowed.
   */
  void stopClusters(Collection<ClusterId> clusters);

  /**
   * update cluster information and save it into the kvstore.
   */
  void updateCluster(ClusterId clusterId);

  /**
   * Health check of the cluster state
   * @param cluster
   */
  void checkClusterState(Cluster cluster);

  public List<ClusterId> getRunningStoppableClustersByName(String name);

  public List<ClusterId> getStartingClustersByName(String name);

  /**
   * Return info about clusters of a particular {@link ClusterType}
   * @param name
   * @return List<ClusterId>
   * @throws ProvisioningService.NoClusterException
   */
  public List<ClusterId> getClustersByName(String name);

  /**
   * Exception that identifies an action that is not allowed.
   */
  public static class ActionDisallowed extends RuntimeException{}

  /**
   * Exception that identifies no cluster exists with provided name
   */
  public static class NoClusterException extends RuntimeException{}

}
