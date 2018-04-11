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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.nodes.NodeProvider;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.VersionExtractor;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterConfig;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterId;
import com.dremio.provision.ClusterSpec;
import com.dremio.provision.ClusterState;
import com.dremio.provision.ClusterType;
import com.dremio.provision.DistroType;
import com.dremio.provision.Property;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
/**
 * Base implementation of Provisioning Service
 * Children will supply particular implementation for some methods
 */
public class ProvisioningServiceImpl implements ProvisioningService, ProvisioningStateListener {

  private static final Logger logger = LoggerFactory.getLogger(ProvisioningServiceImpl.class);
  public static final String TABLE_NAME = "provisioning";
  public static final int DEFAULT_HEAP_MEMORY = 4096;
  public static final int MIN_MEMORY_REQUIRED = 8192;


  private final Map<ClusterType, ProvisioningServiceDelegate> concreteServices = Maps.newHashMap();
  private final NodeProvider executionNodeProvider;
  private final ScanResult classpathScan;
  private final Provider<KVStoreProvider> kvStoreProvider;

  private KVStore<ClusterId, Cluster> store;

  public ProvisioningServiceImpl(
      final Provider<KVStoreProvider> storeProvider,
      final NodeProvider executionNodeProvider,
      ScanResult classpathScan) {
    this.kvStoreProvider = Preconditions.checkNotNull(storeProvider, "store provider is required");
    this.executionNodeProvider = executionNodeProvider;
    this.classpathScan = classpathScan;
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting provisioning service");
    store = kvStoreProvider.get().getStore(ProvisioningStoreCreator.class);
    Set<Class<? extends ProvisioningServiceDelegate>> serviceClasses =
      classpathScan.getImplementations(ProvisioningServiceDelegate.class);
    for (Class<? extends ProvisioningServiceDelegate> provisioningServiceClass : serviceClasses) {
      try {
        Constructor<? extends ProvisioningServiceDelegate> ctor =
          provisioningServiceClass.getConstructor(ProvisioningStateListener.class, NodeProvider.class);
        ProvisioningServiceDelegate provisioningService = ctor.newInstance(this, executionNodeProvider);
        concreteServices.put(provisioningService.getType(), provisioningService);
      } catch (InstantiationException e) {
        logger.error("Unable to create instance of % class", provisioningServiceClass.getName(), e);
      } catch (IllegalAccessException e) {
        logger.error("Unable to create instance of % class", provisioningServiceClass.getName(), e);
      }
    }
  }

  /**
   * Cluster Store creator
   */
  public static class ProvisioningStoreCreator implements StoreCreationFunction<KVStore<ClusterId, Cluster>> {

    @Override
    public KVStore<ClusterId, Cluster> build(StoreBuildingFactory factory) {
      return factory.<ClusterId, Cluster>newStore()
        .name(TABLE_NAME)
        .keySerializer(ClusterIdSerializer.class)
        .valueSerializer(ClusterSerializer.class)
        .versionExtractor(ClusterVersion.class)
        .build();
    }
  }

  protected static ClusterId newRandomClusterId() {
    return new ClusterId(UUID.randomUUID().toString());
  }

  @Override
  public ClusterEnriched createCluster(ClusterConfig clusterConfig) throws ProvisioningHandlingException {
    // just saves info to KVStore
    // children should do the rest
    if((clusterConfig.getClusterSpec().getMemoryMBOnHeap() + clusterConfig.getClusterSpec().getMemoryMBOffHeap()) <
    MIN_MEMORY_REQUIRED) {
      throw new ProvisioningHandlingException("Minimum memory required should be greater or equal than: " +
        MIN_MEMORY_REQUIRED + "MB");
    }

    ClusterId clusterId = newRandomClusterId();
    Cluster cluster = new Cluster();
    cluster.setId(clusterId);
    cluster.setState(ClusterState.CREATED);
    cluster.setDesiredState(ClusterState.RUNNING);
    cluster.setClusterConfig(clusterConfig);

    store.put(clusterId, cluster);

    try {
      return startCluster(clusterId);
    } catch (final Exception e) {
      store.delete(clusterId);
      throw e;
    }
  }

  @Override
  public ClusterEnriched modifyCluster(ClusterId clusterId, ClusterState desiredState, ClusterConfig clusterconfig) throws ProvisioningHandlingException {
    Preconditions.checkNotNull(clusterId, "id is required");
    final Cluster cluster = store.get(clusterId);
    if (cluster == null) {
      throw new ProvisioningHandlingException("Cluster " + clusterId + " is not found. Nothing to modify");
    }

    final Cluster modifiedCluster = toCluster(clusterconfig, desiredState, cluster);

    Action action = toAction(cluster, modifiedCluster);

    switch (action) {
      case NONE:
        return getClusterInfo(clusterId);
      case START:
        return startCluster(clusterId);
      case STOP:
        cluster.setDesiredState(ClusterState.STOPPED);
        store.put(clusterId, cluster);
        return stopCluster(clusterId);
      case DELETE:
        deleteCluster(clusterId);
        return getClusterInfo(clusterId);
      case RESIZE:
        return resizeCluster(clusterId, modifiedCluster.getClusterConfig().getClusterSpec().getContainerCount());
      case RESTART:
        if (ClusterState.RUNNING == cluster.getState() || ClusterState.STOPPING == cluster.getState()) {
          if (ClusterState.RUNNING  == modifiedCluster.getState()) {
            // modify and stop - after stop cluster will start since DESIRED state is RUNNING
            modifiedCluster.setDesiredState(ClusterState.RUNNING);
          }
          cluster.setClusterConfig(modifiedCluster.getClusterConfig());
          cluster.setDesiredState(modifiedCluster.getDesiredState());
          store.put(clusterId, cluster);
          stopCluster(clusterId);
          return getClusterInfo(clusterId);
        }
        if (ClusterState.STOPPED == cluster.getState() || ClusterState.FAILED == cluster.getState()) {
          // just modify, no need to start
          cluster.setClusterConfig(modifiedCluster.getClusterConfig());
          cluster.setDesiredState(modifiedCluster.getDesiredState());
          store.put(clusterId, cluster);
          if (ClusterState.RUNNING  == modifiedCluster.getState()) {
            // start the cluster
            startCluster(clusterId);
          }
          return getClusterInfo(clusterId);
        }
      default:
        return getClusterInfo(clusterId);
    }
  }

  @Override
  public ClusterEnriched resizeCluster(ClusterId clusterId, int newContainersCount) throws ProvisioningHandlingException {
    // get info about cluster
    // children should do the rest
    Preconditions.checkNotNull(clusterId, "id is required");
    final Cluster cluster = store.get(clusterId);
    if (cluster == null) {
      throw new ProvisioningHandlingException("Cluster " + clusterId + " is not found. Nothing to resize");
    }

    cluster.getClusterConfig().getClusterSpec().setContainerCount(newContainersCount);
    final ProvisioningServiceDelegate service = concreteServices.get(cluster.getClusterConfig().getClusterType());
    if (service == null) {
      throw new ProvisioningHandlingException("Can not find service implementation for: " + cluster.getClusterConfig().getClusterType());
    }

    if (newContainersCount <= 0) {
      logger.info("Since number of requested containers to resize == 0. Stopping cluster");
      service.stopCluster(cluster);
    } else {
      service.resizeCluster(cluster);
    }
    store.put(clusterId, cluster);
    return service.getClusterInfo(cluster);
  }

  @Override
  public ClusterEnriched stopCluster(ClusterId clusterId) throws ProvisioningHandlingException {
    // get info about cluster
    // children should do the rest
    Preconditions.checkNotNull(clusterId, "id is required");
    final Cluster cluster = store.get(clusterId);
    if (cluster == null) {
      throw new ProvisioningHandlingException("Cluster " + clusterId + " is not found. Nothing to stop");
    }

    final ProvisioningServiceDelegate service = concreteServices.get(cluster.getClusterConfig().getClusterType());
    if (service == null) {
      throw new ProvisioningHandlingException("Can not find service implementation for: " + cluster.getClusterConfig().getClusterType());
    }

    if (ClusterState.STOPPING == cluster.getState()) {
      // nothing to stop
      return new ClusterEnriched(cluster);
    }
    if (cluster.getDesiredState() == null) {
      cluster.setDesiredState(ClusterState.STOPPED);
    }
    service.stopCluster(cluster);
    store.put(clusterId, cluster);
    ClusterEnriched updatedCluster = service.getClusterInfo(cluster);
    return updatedCluster;
  }

  @Override
  public ClusterEnriched startCluster(ClusterId clusterId) throws ProvisioningHandlingException {
    // get info about cluster
    // children should do the rest
    Preconditions.checkNotNull(clusterId, "id is required");
    final Cluster cluster = store.get(clusterId);
    if (cluster == null) {
      throw new ProvisioningHandlingException("Cluster " + clusterId + " is not found. Nothing to start");
    }

    final ProvisioningServiceDelegate service = concreteServices.get(cluster.getClusterConfig().getClusterType());
    if (service == null) {
      throw new ProvisioningHandlingException("Can not find service implementation for: " + cluster.getClusterConfig().getClusterType());
    }

    final ClusterEnriched updatedCluster;
    cluster.setDesiredState(ClusterState.RUNNING);
    if (ClusterState.STOPPING == cluster.getState()) {
      updatedCluster = new ClusterEnriched(cluster);
    } else {
      updatedCluster = service.startCluster(cluster);
    }
    store.put(clusterId, updatedCluster.getCluster());
    return updatedCluster;
  }

  @Override
  public void deleteCluster(ClusterId id) throws ProvisioningHandlingException {
    // delete info from KVStore
    Preconditions.checkNotNull(id, "id is required");
    final Cluster cluster = store.get(id);
    if (cluster == null) {
      throw new ProvisioningHandlingException("Cluster " + id + " is not found. Nothing to delete");
    }
    if (ClusterState.STOPPED == cluster.getState() || ClusterState.FAILED == cluster.getState()) {
      store.delete(id);
      return;
    }
    final ProvisioningServiceDelegate service = concreteServices.get(cluster.getClusterConfig().getClusterType());
    if (service == null) {
      throw new ProvisioningHandlingException("Can not find service implementation for: " + cluster.getClusterConfig().getClusterType());
    }
    cluster.setDesiredState(ClusterState.DELETED);
    service.stopCluster(cluster);
    store.put(id, cluster);
    // stopping cluster could be async, do not delete right away
  }

  @Override
  public ClusterEnriched getClusterInfo(ClusterId id) throws ProvisioningHandlingException {
    // get info about cluster
    // children should do the rest
    Preconditions.checkNotNull(id, "id is required");
    final Cluster cluster = store.get(id);
    if (cluster == null) {
      throw new ProvisioningHandlingException("Cluster " + id + " is not found.");
    }

    final ProvisioningServiceDelegate service = concreteServices.get(cluster.getClusterConfig().getClusterType());
    if (service == null) {
      throw new ProvisioningHandlingException("Can not find service implementation for: " + cluster.getClusterConfig().getClusterType());
    }
    return service.getClusterInfo(cluster);
  }

  @Override
  public Iterable<ClusterEnriched> getClusterInfoByType(final ClusterType type) throws ProvisioningHandlingException {
    Iterable<Map.Entry<ClusterId, Cluster>> clusters = store.find();
    Predicate<Map.Entry<ClusterId, Cluster>> filter = new Predicate<Map.Entry<ClusterId, Cluster>>() {
      @Override
      public boolean apply(Map.Entry<ClusterId, Cluster> input) {
        return (input.getValue().getClusterConfig().getClusterType() == type);
      }
    };
    return FluentIterable.from(clusters).filter(filter).transform(new InfoFunctionTransformer());
  }

  @Override
  public Iterable<ClusterEnriched> getClusterInfoByTypeByState(final ClusterType type, final ClusterState state) throws ProvisioningHandlingException {
    Iterable<Map.Entry<ClusterId, Cluster>> clusters = store.find();
    Predicate<Map.Entry<ClusterId, Cluster>> filter = new Predicate<Map.Entry<ClusterId, Cluster>>() {
      @Override
      public boolean apply(Map.Entry<ClusterId, Cluster> input) {
        return (input.getValue().getClusterConfig().getClusterType() == type && input.getValue().getState() == state);
      }
    };
    return FluentIterable.from(clusters).filter(filter).transform(new InfoFunctionTransformer());
  }

  @Override
  public Iterable<ClusterEnriched> getClustersInfo() throws ProvisioningHandlingException {
    // get info about cluster
    // children should do the rest
    Iterable<Map.Entry<ClusterId, Cluster>> clusters = store.find();
    return Iterables.transform(clusters, new InfoFunctionTransformer());
  }

  @Override
  public void close() throws Exception {
    // noop
  }

  @Override
  public void started(Cluster cluster) throws ProvisioningHandlingException {
    store.put(cluster.getId(), cluster);
  }

  @Override
  public void stopped(Cluster cluster) throws ProvisioningHandlingException {
    final Cluster storedCluster = store.get(cluster.getId());
    if (storedCluster == null) {
      // should not be possible
      logger.error("Trying to mark deleted cluster {} as stopped", cluster.getId());
      store.put(cluster.getId(), cluster);
      return;
    }
    // since stopping cluster happens in a separate thread that starts when cluster starts
    // there could be quite a few transformations along the way and subsequently
    // version of original one can be different
    // set state and error from arg, let everything else be up to date
    if (storedCluster.getDesiredState() == ClusterState.DELETED) {
      store.delete(cluster.getId());
    } else {
      logger.info("Cluster {} current state is: {}", cluster.getId(), cluster.getState());
      ClusterState desiredState = storedCluster.getDesiredState();
      storedCluster.setState(cluster.getState());
      storedCluster.setError(cluster.getError());
      storedCluster.setDetailedError(cluster.getDetailedError());
      storedCluster.setRunId(cluster.getRunId());
      store.put(storedCluster.getId(), storedCluster);
      if (ClusterState.RUNNING == desiredState &&  ClusterState.FAILED != cluster.getState()) {
        // start cluster
        startCluster(storedCluster.getId());
      }
    }
  }

  @Override
  public void resized(Cluster cluster) throws ProvisioningHandlingException {
    store.put(cluster.getId(), cluster);
  }

  private class InfoFunctionTransformer implements Function<Map.Entry<ClusterId, Cluster>, ClusterEnriched> {

    @Nullable
    @Override
    public ClusterEnriched apply(@Nullable Map.Entry<ClusterId, Cluster> input) {
      try {
        return concreteServices.get(input.getValue().getClusterConfig().getClusterType()).getClusterInfo(input.getValue());
      } catch (Exception ex) {
        return new ClusterEnriched(input.getValue());
      }
    }
  }

  enum Action {
    NONE,
    START,
    STOP,
    DELETE,
    RESTART,
    RESIZE
  }

  @VisibleForTesting
  Cluster toCluster(final ClusterConfig request, ClusterState desiredState, final Cluster storedCluster) {

    final Cluster cluster = new Cluster();
    cluster.setId(storedCluster.getId());
    cluster.setState(Optional.fromNullable(desiredState).or(storedCluster.getState()));
    final ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setDistroType(Optional.fromNullable(
      storedCluster.getClusterConfig().getDistroType()).or(DistroType.OTHER));
    clusterConfig.setIsSecure(Optional.fromNullable(storedCluster.getClusterConfig().getIsSecure()).or(false));
    clusterConfig.setVersion(request.getVersion());
    if (storedCluster.getClusterConfig().getName() != null) {
      clusterConfig.setName(Optional.fromNullable(request.getName()).or(
        storedCluster.getClusterConfig().getName()));
    } else {
      clusterConfig.setName(Optional.fromNullable(request.getName()).orNull());
    }
    clusterConfig.setClusterType(Optional.fromNullable(request.getClusterType()).or(storedCluster.getClusterConfig().getClusterType()));

    // An assumption is that FE will pass full list of properties, otherwise BE does not know if any property was
    // removed
    // so if properties from FE is null it will take ones from stored cluster
    clusterConfig.setSubPropertyList(Optional.fromNullable(request.getSubPropertyList()).or(storedCluster
      .getClusterConfig().getSubPropertyList()));
    final ClusterSpec clusterSpec = new ClusterSpec();
    if (storedCluster.getClusterConfig().getClusterSpec().getQueue() != null) {
      clusterSpec.setQueue(Optional.fromNullable(request.getClusterSpec().getQueue()).or(storedCluster.getClusterConfig().getClusterSpec
        ().getQueue()));
    } else {
      clusterSpec.setQueue(Optional.fromNullable(request.getClusterSpec().getQueue()).orNull());
    }
    clusterSpec.setContainerCount(Optional.fromNullable(request.getClusterSpec().getContainerCount()).or
      (storedCluster.getClusterConfig().getClusterSpec().getContainerCount()));
    clusterSpec.setVirtualCoreCount(Optional.fromNullable(request.getClusterSpec().getVirtualCoreCount()).or
      (storedCluster.getClusterConfig().getClusterSpec().getVirtualCoreCount()));

    if (request.getClusterSpec().getMemoryMBOnHeap() == null) {
      clusterSpec.setMemoryMBOnHeap(storedCluster.getClusterConfig()
        .getClusterSpec().getMemoryMBOnHeap());
      if (request.getClusterSpec().getMemoryMBOffHeap() != null) {
        // only total memory is known
        clusterSpec.setMemoryMBOffHeap(request.getClusterSpec().getMemoryMBOffHeap() - storedCluster.getClusterConfig()
          .getClusterSpec().getMemoryMBOnHeap());
      } else {
        // means we did not really get it from FE - need to set it from what is stored
        clusterSpec.setMemoryMBOffHeap(storedCluster.getClusterConfig()
          .getClusterSpec().getMemoryMBOffHeap());
      }
    } else {
      clusterSpec.setMemoryMBOnHeap(request.getClusterSpec().getMemoryMBOnHeap());
      if (request.getClusterSpec().getMemoryMBOffHeap() != null) {
        clusterSpec.setMemoryMBOffHeap(request.getClusterSpec().getMemoryMBOffHeap());
      } else {
        clusterSpec.setMemoryMBOffHeap(storedCluster.getClusterConfig()
          .getClusterSpec().getMemoryMBOffHeap() + storedCluster.getClusterConfig()
          .getClusterSpec().getMemoryMBOnHeap() - request.getClusterSpec().getMemoryMBOnHeap());
      }
    }

    clusterConfig.setClusterSpec(clusterSpec);
    cluster.setClusterConfig(clusterConfig);

    return cluster;
  }

  @VisibleForTesting
  Action toAction(Cluster storedCluster, final Cluster modifiedCluster) throws
    ProvisioningHandlingException {

    Preconditions.checkNotNull(modifiedCluster.getClusterConfig().getVersion(), "Version in modified cluster has to be set");

    final long storedVersion = storedCluster.getClusterConfig().getVersion().longValue();
    final long incomingVersion = modifiedCluster.getClusterConfig().getVersion().longValue();
    if (storedVersion != incomingVersion) {
      throw new ConcurrentModificationException(String.format("Version of submitted Cluster does not match stored. " +
        "Stored Version: %d . Provided Version: %d . Please refetch", storedVersion, incomingVersion));
    }
    if (ClusterState.DELETED == storedCluster.getDesiredState()) {
      throw new IllegalStateException("Cluster in the process of deletion. No modification is allowed");
    }

    if (ClusterState.STOPPING == storedCluster.getState() || ClusterState.STARTING == storedCluster.getState()) {
      throw new IllegalStateException("Cluster in the process of stopping/restarting. No modification is " +
        "allowed");
    }

    if (Objects.equal(storedCluster,modifiedCluster)) {
      return Action.NONE;
    }

    // state change only
    if (Objects.equal(storedCluster.getClusterConfig(),modifiedCluster.getClusterConfig())) {
      if(storedCluster.getState() != modifiedCluster.getState()) {
        // state change
        switch (modifiedCluster.getState()) {
          case RUNNING:
            return Action.START;
          case STOPPED:
            return Action.STOP;
          case DELETED:
            return Action.DELETE;
          default:
            // nothing to do for other states
            logger.warn("Request to change to non-actionable state {}", storedCluster.getState());
            return Action.NONE;
        }
      } else {
        // looks like nothing was changed
        return Action.NONE;
      }
    }

    if (!equals(storedCluster.getClusterConfig().getSubPropertyList(), modifiedCluster.getClusterConfig()
      .getSubPropertyList())) {
      return Action.RESTART;
    }

    ClusterSpec storedClusterSpec = storedCluster.getClusterConfig().getClusterSpec();
    ClusterSpec tempClusterSpec = new ClusterSpec();
    tempClusterSpec.setQueue(storedClusterSpec.getQueue());
    tempClusterSpec.setMemoryMBOffHeap(storedClusterSpec.getMemoryMBOffHeap());
    tempClusterSpec.setMemoryMBOnHeap(storedClusterSpec.getMemoryMBOnHeap());
    tempClusterSpec.setVirtualCoreCount(storedClusterSpec.getVirtualCoreCount());
    tempClusterSpec.setContainerCount(modifiedCluster.getClusterConfig().getClusterSpec().getContainerCount());

    if (tempClusterSpec.getContainerCount() != storedClusterSpec.getContainerCount() &&
      Objects.equal(modifiedCluster.getClusterConfig().getClusterSpec(),tempClusterSpec) &&
      (ClusterState.RUNNING == storedCluster.getState() && ClusterState.RUNNING == modifiedCluster.getState())) {
      // only difference is in number of containers
      return Action.RESIZE;
    }

    // for anything else restart
    return Action.RESTART;
  }

  @VisibleForTesting
  static boolean equals(List<Property> list1, List<Property> list2) {
    if (list1.size() != list2.size()) {
      return false;
    }
    List<Property> tmpList = new ArrayList<>(list1);
    tmpList.removeAll(list2);
    if (tmpList.isEmpty()) {
      return true;
    }
    return false;
  }

  @VisibleForTesting
  Map<ClusterType, ProvisioningServiceDelegate> getConcreteServices() {
    return concreteServices;
  }

  private static final class ClusterSerializer extends Serializer<Cluster> {
    private final Serializer<Cluster> serializer = ProtostuffSerializer.of(Cluster.getSchema());

    @Override
    public byte[] convert(final Cluster v) {
      return serializer.convert(v);
    }

    @Override
    public Cluster revert(final byte[] data) {
      return serializer.revert(data);
    }

    @Override
    public String toJson(final Cluster v) throws IOException {
      return serializer.toJson(v);
    }

    @Override
    public Cluster fromJson(final String v) throws IOException {
      return serializer.fromJson(v);
    }
  }

  private static final class ClusterIdSerializer extends Serializer<ClusterId> {
    private final Serializer<ClusterId> serializer = ProtostuffSerializer.of(ClusterId.getSchema());

    @Override
    public byte[] convert(final ClusterId id) {
      return serializer.convert(id);
    }

    @Override
    public ClusterId revert(final byte[] data) {
      return serializer.revert(data);
    }

    @Override
    public String toJson(final ClusterId v) throws IOException {
      return serializer.toJson(v);
    }

    @Override
    public ClusterId fromJson(final String v) throws IOException {
      return serializer.fromJson(v);
    }

  }

  private static final class ClusterVersion implements VersionExtractor<Cluster> {

    @Override
    public Long getVersion(final Cluster value) {
      return value.getClusterConfig().getVersion();
    }

    @Override
    public Long incrementVersion(final Cluster value) {
      final Long current = value.getClusterConfig().getVersion();
      value.getClusterConfig().setVersion(Optional.fromNullable(value.getClusterConfig().getVersion()).or(-1L) + 1);
      return current;
    }

    @Override
    public void setVersion(final Cluster value, final Long version) {
      value.getClusterConfig().setVersion(version);
    }
  }
}
