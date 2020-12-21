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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.nodes.NodeProvider;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.edition.EditionProvider;
import com.dremio.options.OptionManager;
import com.dremio.provision.AwsProps;
import com.dremio.provision.AwsProps.AuthMode;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterConfig;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterId;
import com.dremio.provision.ClusterSpec;
import com.dremio.provision.ClusterState;
import com.dremio.provision.ClusterType;
import com.dremio.provision.DistroType;
import com.dremio.provision.Property;
import com.dremio.provision.resource.ProvisioningResource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
/**
 * Base implementation of Provisioning Service
 * Children will supply particular implementation for some methods
 */
public class ProvisioningServiceImpl implements ProvisioningService, ProvisioningStateListener {

  private static final Logger logger = LoggerFactory.getLogger(ProvisioningServiceImpl.class);
  public static final String TABLE_NAME = "provisioning";
  public static final int DEFAULT_HEAP_MEMORY_MB = 4096;
  public static final int LARGE_SYSTEMS_DEFAULT_HEAP_MEMORY_MB = 8192;
  public static final int MIN_MEMORY_REQUIRED_MB = 8192;
  public static final int LARGE_SYSTEMS_MIN_MEMORY_MB = 32768; // DX-10446
  private static ClusterType clusterType = null;

  private Map<ClusterType, ProvisioningServiceDelegate> concreteServices;

  private final Provider<LegacyKVStoreProvider> kvStoreProvider;
  private final CloseableThreadPool pool = new CloseableThreadPool("start-executor");
  private final Provider<Map<ClusterType, ProvisioningServiceDelegate>> delegateProvider;
  private LegacyKVStore<ClusterId, Cluster> store;

  public ProvisioningServiceImpl(
    final DremioConfig dremioConfig,
    final Provider<LegacyKVStoreProvider> kvStoreProvider,
    final NodeProvider executionNodeProvider,
    ScanResult scanResult,
    Provider<OptionManager> optionProvider,
    Provider<EditionProvider> editionProvider) {
    this.kvStoreProvider = Preconditions.checkNotNull(kvStoreProvider, "store provider is required");
    this.delegateProvider = () -> buildConcreteServices(
      scanResult,
      dremioConfig,
      executionNodeProvider,
      optionProvider,
      editionProvider,
      this
    );
  }

  public static ClusterType getType() {
    return clusterType;
  }

  @VisibleForTesting
  ProvisioningServiceImpl(
    Map<ClusterType, ProvisioningServiceDelegate> concreteServices,
    final Provider<LegacyKVStoreProvider> kvStoreProvider) {
    this.delegateProvider = () -> concreteServices;
    this.kvStoreProvider = Preconditions.checkNotNull(kvStoreProvider, "store provider is required");
  }

  private static Map<ClusterType, ProvisioningServiceDelegate> buildConcreteServices(
    ScanResult scanResult,
    DremioConfig dremioConfig,
    NodeProvider executionNodeProvider,
    Provider<OptionManager> optionProvider,
    Provider<EditionProvider> editionProvider,
    ProvisioningServiceImpl provisioningServiceImpl){
    Set<Class<? extends ProvisioningServiceDelegate>> serviceClasses =
      scanResult.getImplementations(ProvisioningServiceDelegate.class);
    Map<ClusterType, ProvisioningServiceDelegate> concreteServices = new HashMap<>();
    for (Class<? extends ProvisioningServiceDelegate> provisioningServiceClass : serviceClasses) {
      try {
        Constructor<? extends ProvisioningServiceDelegate> ctor =
          provisioningServiceClass.getConstructor(DremioConfig.class, ProvisioningStateListener.class, NodeProvider.class, OptionManager.class,
            EditionProvider.class);
        ProvisioningServiceDelegate provisioningService =
          ctor.newInstance(dremioConfig, provisioningServiceImpl, executionNodeProvider, optionProvider.get(),
          editionProvider.get());

        concreteServices.put(provisioningService.getType(), provisioningService);
      } catch (ReflectiveOperationException e) {
        logger.error("Unable to create instance of % class", provisioningServiceClass.getName(), e);
      }
    }
    return Collections.unmodifiableMap(concreteServices);
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting provisioning service");
    store = kvStoreProvider.get().getStore(ProvisioningStoreCreator.class);
    concreteServices = delegateProvider.get();
    for (ProvisioningServiceDelegate provisioningService: concreteServices.values()) {
      provisioningService.start();
      ClusterConfig defaultClusterConfig = provisioningService.defaultCluster();
      if (defaultClusterConfig != null && !exists(defaultClusterConfig.getName())) {
        Cluster cluster = initializeCluster(defaultClusterConfig);
        startCluster(cluster.getId());
      }
    }
    syncClusters();
  }

  @VisibleForTesting
  void syncClusters() {
    final List<ClusterId> orphanClusters = new ArrayList<>();
    for (Map.Entry<ClusterId, Cluster> entry : store.find()) {
      try {
        final Cluster cluster = entry.getValue();
        final ClusterType clusterType = cluster.getClusterConfig().getClusterType();
        ProvisioningServiceDelegate provisioningServiceDelegate = concreteServices.get(clusterType);
        if (provisioningServiceDelegate == null) {
          if (ClusterType.EC2 == clusterType) {
            orphanClusters.add(entry.getKey());
          }
          logger.debug("Can not find service implementation for: {}", clusterType);
        } else {
          provisioningServiceDelegate.syncCluster(cluster);
          store.put(entry.getKey(), cluster);
        }
        if (clusterType != null) {
          ProvisioningServiceImpl.clusterType = clusterType;
        }
      } catch (Exception e) {
        logger.error("Unable to sync cluster, {}", entry.getKey(), e);
      }
    }
    orphanClusters.forEach(clusterId -> store.delete(clusterId));
  }

  private boolean exists(String name) {
    return StreamSupport.stream(store.find().spliterator(), false).anyMatch(e -> name.equals(e.getValue().getClusterConfig().getName()));
  }

  /**
   * Cluster Store creator
   */
  public static class ProvisioningStoreCreator implements LegacyKVStoreCreationFunction<ClusterId, Cluster> {

    @Override
    public LegacyKVStore<ClusterId, Cluster> build(LegacyStoreBuildingFactory factory) {
      return factory.<ClusterId, Cluster>newStore()
        .name(TABLE_NAME)
        .keyFormat(Format.ofProtostuff(ClusterId.class))
        .valueFormat(Format.ofProtostuff(Cluster.class))
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
    if(clusterConfig.getClusterType() == ClusterType.YARN &&
        (clusterConfig.getClusterSpec().getMemoryMBOnHeap() + clusterConfig.getClusterSpec().getMemoryMBOffHeap()) <
    MIN_MEMORY_REQUIRED_MB) {
      throw new ProvisioningHandlingException("Minimum memory required should be greater or equal than: " +
        MIN_MEMORY_REQUIRED_MB + "MB");
    }

    Cluster cluster = initializeCluster(clusterConfig);
    ClusterId clusterId = cluster.getId();

    try {
      return startCluster(clusterId);
    } catch (final Exception e) {
      store.delete(clusterId);
      throw e;
    }
  }

  private Cluster initializeCluster(ClusterConfig clusterConfig) {
    ClusterId clusterId = newRandomClusterId();
    Cluster cluster = new Cluster();
    cluster.setId(clusterId);
    cluster.setState(ClusterState.CREATED);
    cluster.setStateChangeTime(System.currentTimeMillis());
    cluster.setDesiredState(ClusterState.RUNNING);
    cluster.setClusterConfig(clusterConfig);
    store.put(clusterId, cluster);
    return cluster;
  }

  @Override
  public void updateCluster(ClusterId clusterId) {
    Cluster cluster = store.get(clusterId);
    long ts = System.currentTimeMillis();
    logger.debug("update cluster idle time at ts: {}", ts);
    store.put(clusterId, cluster.setIdleTime(ts));
  }

  @Override
  public synchronized ClusterEnriched modifyCluster(ClusterId clusterId, ClusterState desiredState, ClusterConfig clusterconfig) throws ProvisioningHandlingException {
    logger.debug("Modifying cluster {}, desired state: {}", clusterId, desiredState);
    Preconditions.checkNotNull(clusterId, "id is required");
    final Cluster cluster = store.get(clusterId);
    if (cluster == null) {
      throw new ProvisioningHandlingException("Cluster " + clusterId + " is not found. Nothing to modify");
    }

    final Cluster modifiedCluster = toCluster(clusterconfig, desiredState, cluster);

    Action action = toAction(cluster, modifiedCluster);
    logger.debug("Action:{}", action);

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

    logger.debug("Stopping cluster {}", cluster.getId().getId());

    final ProvisioningServiceDelegate service = concreteServices.get(cluster.getClusterConfig().getClusterType());
    if (service == null) {
      throw new ProvisioningHandlingException("Can not find service implementation for: " + cluster.getClusterConfig().getClusterType());
    }

    if (ClusterState.STOPPING == cluster.getState()) {
      logger.debug("nothing to stop");
      // nothing to stop
      return new ClusterEnriched(cluster);
    }
    if (cluster.getDesiredState() == null) {
      cluster.setDesiredState(ClusterState.STOPPED);
    }
    service.stopCluster(cluster);
    logger.debug("Storing clusterId: {}, state:{}, desiredState:{}", clusterId, cluster.getState(), cluster.getDesiredState());
    store.put(clusterId, cluster);
    ClusterEnriched updatedCluster = service.getClusterInfo(cluster);
    return updatedCluster;
  }

  @Override
  public ClusterEnriched startCluster(ClusterId clusterId) throws ProvisioningHandlingException {
    logger.debug("attempting to start cluster: {}", clusterId);
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
    clusterType = cluster.getClusterConfig().getClusterType();

    final ClusterEnriched updatedCluster;
    cluster.setDesiredState(ClusterState.RUNNING);
    if (ClusterState.STOPPING == cluster.getState()) {
      updatedCluster = new ClusterEnriched(cluster);
    } else {
      long ts = System.currentTimeMillis();
      logger.debug("Starting cluster. ts: {}", ts);
      store.put(clusterId, cluster.setStartTime(ts).setIdleTime(ts));
      updatedCluster = service.startCluster(cluster);
    }
    long ts = System.currentTimeMillis();
    logger.debug("Started cluster. ts: {}", ts);
    store.put(clusterId, updatedCluster.getCluster().setStartTime(ts).setIdleTime(ts));
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
    AutoCloseables.close(Iterables.concat(Collections.singletonList(pool), concreteServices.values()));
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
      storedCluster.setStateChangeTime(cluster.getStateChangeTime());
      storedCluster.setError(cluster.getError());
      storedCluster.setDetailedError(cluster.getDetailedError());
      storedCluster.setRunId(cluster.getRunId());
      store.put(storedCluster.getId(), storedCluster);
      if (ClusterState.RUNNING == desiredState &&  ClusterState.FAILED != cluster.getState()) {
        logger.debug("Starting cluster {} because desired state is running", cluster.getId().getId());
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
    /* If the desired state of the cluster is being changed from the current state, then we want to use the current time.
     * The status change time may also be null in the event the cluster existed prior to the introduction of the
     * state change time property.
     */
    cluster.setStateChangeTime((cluster.getState() != desiredState || storedCluster.getStateChangeTime() == null) ? System.currentTimeMillis() : storedCluster.getStateChangeTime());
    final ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setAllowAutoStart(request.getAllowAutoStart());
    clusterConfig.setAllowAutoStop(request.getAllowAutoStop());
    clusterConfig.setShutdownInterval(request.getShutdownInterval());
    clusterConfig.setTag(request.getTag());
    if (storedCluster.getClusterConfig().getName() != null) {
      clusterConfig.setName(Optional.fromNullable(request.getName()).or(
        storedCluster.getClusterConfig().getName()));
    } else {
      clusterConfig.setName(Optional.fromNullable(request.getName()).orNull());
    }
    clusterConfig.setClusterType(Optional.fromNullable(request.getClusterType()).or(storedCluster.getClusterConfig().getClusterType()));

    if (clusterConfig.getClusterType() == ClusterType.YARN) {
      // An assumption is that FE will pass full list of properties, otherwise BE does not know if any property was
      // removed
      // so if properties from FE is null it will take ones from stored cluster

      clusterConfig.setDistroType(Optional.fromNullable(storedCluster.getClusterConfig().getDistroType()).or(DistroType.OTHER));
      clusterConfig.setIsSecure(Optional.fromNullable(storedCluster.getClusterConfig().getIsSecure()).or(false));

      clusterConfig.setSubPropertyList(Optional.fromNullable(request.getSubPropertyList()).or(storedCluster
        .getClusterConfig().getSubPropertyList()));
      final ClusterSpec clusterSpec = new ClusterSpec();
      if (storedCluster.getClusterConfig().getClusterSpec().getQueue() != null) {
        clusterSpec.setQueue(Optional.fromNullable(request.getClusterSpec().getQueue()).or(storedCluster.getClusterConfig().getClusterSpec
          ().getQueue()));
      } else {
        clusterSpec.setQueue(Optional.fromNullable(request.getClusterSpec().getQueue()).orNull());
      }
      clusterSpec.setContainerCount(Optional
          .fromNullable(request.getClusterSpec().getContainerCount())
          .or(storedCluster.getClusterConfig().getClusterSpec().getContainerCount()));
      clusterSpec.setVirtualCoreCount(Optional
          .fromNullable(request.getClusterSpec().getVirtualCoreCount())
          .or(storedCluster.getClusterConfig().getClusterSpec().getVirtualCoreCount()));

      if (request.getClusterSpec().getMemoryMBOnHeap() == null) {
        if (request.getClusterSpec().getMemoryMBOffHeap() != null) {
          // only total memory is known
          final int totalMemory = request.getClusterSpec().getMemoryMBOffHeap();
          final int onHeap = totalMemory < LARGE_SYSTEMS_MIN_MEMORY_MB ? DEFAULT_HEAP_MEMORY_MB : LARGE_SYSTEMS_DEFAULT_HEAP_MEMORY_MB;
          clusterSpec.setMemoryMBOnHeap(onHeap);
          clusterSpec.setMemoryMBOffHeap(totalMemory - onHeap);
        } else {
          // means we did not really get it from FE - need to set it from what is stored
          clusterSpec.setMemoryMBOnHeap(storedCluster.getClusterConfig()
            .getClusterSpec().getMemoryMBOnHeap());
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
    } else if (clusterConfig.getClusterType() == ClusterType.EC2) {
      final ClusterSpec clusterSpec = new ClusterSpec();
      clusterSpec.setContainerCount(Optional.fromNullable(request.getClusterSpec().getContainerCount()).or(storedCluster.getClusterConfig().getClusterSpec().getContainerCount()));
      clusterConfig.setClusterSpec(clusterSpec);
      AwsProps newProps = request.getAwsProps();
      clusterConfig.setAwsProps(newProps);

      AwsProps oldProps = storedCluster.getClusterConfig().getAwsProps();
      if (newProps.getConnectionProps().getAuthMode() == AuthMode.SECRET
          && ProvisioningResource.USE_EXISTING_SECRET_VALUE.equals(newProps.getConnectionProps().getSecretKey())
          && oldProps.getConnectionProps().getAuthMode() == AuthMode.SECRET
          ) {
        newProps.getConnectionProps().setSecretKey(oldProps.getConnectionProps().getSecretKey());
      }
    }

    cluster.setClusterConfig(clusterConfig);

    return cluster;
  }

  private List<Cluster> findClusters(String name) {
    List<Cluster> clusters = new ArrayList<>();

    for(Entry<ClusterId, Cluster> e : store.find()) {
      if(name.equals(e.getValue().getClusterConfig().getName())) {
        clusters.add(e.getValue());
      }
    }

    if(clusters.isEmpty()) {
      throw new ProvisioningService.NoClusterException();
    }

    return clusters;
  }

  private CompletableFuture<Void> doAction(String name, ClusterState desiredState) {
    try {
      final List<Cluster> clusters = findClusters(name);
      return CompletableFuture.allOf(
          clusters.stream()
          .map(c -> createFuture(desiredState, c, c.getClusterConfig().getName()))
          .collect(Collectors.toList())
          .toArray(new CompletableFuture[0]));
    } catch (Exception ex) {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      cf.completeExceptionally(ex);
      return cf;
    }
  }

  private CompletableFuture<Void> createFuture(ClusterState desiredState, Cluster initialCluster, String name) {
    final CompletableFuture<Void> future = new CompletableFuture<Void>();

    pool.submit(() -> {
      try {
        Cluster cluster = initialCluster;

        Boolean autoStart = cluster.getClusterConfig().getAllowAutoStart();
        if(desiredState == ClusterState.RUNNING && (autoStart == null || !autoStart)) {
          throw new ActionDisallowed();
        }

        Boolean autoStop = cluster.getClusterConfig().getAllowAutoStop();
        if(desiredState == ClusterState.STOPPED && (autoStop == null || !autoStop) ) {
          throw new ActionDisallowed();
        }

        ClusterId id = cluster.getId();

        logger.info("Applying action {} on cluster '{}'.", desiredState.name(), name);
        try {
          modifyCluster(id, desiredState, cluster.getClusterConfig());
        } catch (ConcurrentModificationException ex) {
          // could happen if multiple queries try to start the same cluster at the same time. Shouldn't cause us to fail.
          logger.debug("Autostart failed due to concurrent modification of cluster. Monitoring for desired state anyway.", ex);
        }
        int i = 0;
        while(i < 3600) {
          cluster = store.get(id);
          if(cluster.getState() == desiredState) {
            logger.info("Action {} on cluster '{}' completed.", desiredState.name(), name);
            future.complete(null);
            return;
          }

          if(future.isCancelled()) {
            break;
          }
          Thread.sleep(1000);
          i++;
        }
        logger.info("Failed to {} cluster '{}' within 1 hour.", desiredState.name(), name);
        future.complete(null);
      } catch (Throwable ex) {
        future.completeExceptionally(ex);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> autostartCluster(String name) {
    return doAction(name, ClusterState.RUNNING);
  }

  @Override
  public void stopClusters(Collection<ClusterId> clusters) {
    for (ClusterId id : clusters) {
      Cluster cluster = store.get(id);
      try {
        modifyCluster(id, ClusterState.STOPPED, cluster.getClusterConfig());
      } catch (ProvisioningHandlingException e) {
        logger.warn("Failure while stopping cluster {}", cluster.getClusterConfig().getName(), e);
      }
    }
  }

  @VisibleForTesting
  Action toAction(Cluster storedCluster, final Cluster modifiedCluster) throws
    ProvisioningHandlingException {

    Preconditions.checkNotNull(modifiedCluster.getClusterConfig().getTag(), "Version in modified cluster has to be set");

    final String storedVersion = storedCluster.getClusterConfig().getTag();
    final String incomingVersion = modifiedCluster.getClusterConfig().getTag();
    if (!incomingVersion.equals(storedVersion)) {
      throw new ConcurrentModificationException(String.format("Version of submitted Cluster does not match stored. " +
        "Stored Version: %s . Provided Version: %s . Please refetch", storedVersion, incomingVersion));
    }
    if (ClusterState.DELETED == storedCluster.getDesiredState()) {
      throw new IllegalStateException("Cluster in the process of deletion. No modification is allowed");
    }

    if (ClusterState.STARTING == storedCluster.getState()
    && ClusterState.RUNNING == modifiedCluster.getState()) {
      return Action.NONE;
    }

    if((ClusterType.YARN == modifiedCluster.getClusterConfig().getClusterType()) &&
      (ClusterState.STOPPING == storedCluster.getState())) {
      throw new IllegalStateException("YARN Cluster in the process of stopping. No modification is allowed");
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

    if(modifiedCluster.getClusterConfig().getClusterType() == ClusterType.EC2) {
      // don't check for a resize for ec2 since it doesn't support resizing.
      return Action.RESTART;
    }

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
    if((list1 == null || list1.isEmpty()) && (list2 == null || list2.isEmpty())) {
      return true;
    }
    if(list1 == null || list1.isEmpty()) {
      return false;
    }

    if(list2 == null || list2.isEmpty()) {
      return false;
    }

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

  private static final class ClusterVersion implements VersionExtractor<Cluster> {

    @Override
    public Long getVersion(final Cluster value) {
      return value.getClusterConfig().getVersion();
    }

    @Override
    public void setVersion(final Cluster value, final Long version) {
      value.getClusterConfig().setVersion(version);
    }

    @Override
    public String getTag(Cluster value) {
      return value.getClusterConfig().getTag();
    }

    @Override
    public void setTag(final Cluster value, final String tag) {
      value.getClusterConfig().setTag(tag);
    }
  }

  @Override
  public List<ClusterId> getRunningStoppableClustersByName(String name) {
    List<ClusterId> ids = new ArrayList<>();
    logger.debug("Finding clusters with name {}", name);
    for(Entry<ClusterId, Cluster> c : store.find()) {
      Cluster cluster = c.getValue();
      logger.debug("Found cluster with clusterId {}", c.getKey());
      if(!name.equals(cluster.getClusterConfig().getName())) {
        continue;
      }

      Boolean allowAutoStop = cluster.getClusterConfig().getAllowAutoStop();
      if(allowAutoStop == null || !allowAutoStop) {
        continue;
      }
      if(cluster.getState() == ClusterState.RUNNING) {
        logger.debug("Adding {} to be stopped", c.getKey());
        ids.add(c.getKey());
      }
    }
    return ids;
  }

  @Override
  public List<ClusterId> getClustersByName(String name) {
    return findClusters(name).stream().map(c -> c.getId()).collect(Collectors.toList());
  }
}
