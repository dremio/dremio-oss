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
package com.dremio.provision.yarn.service;

import static com.dremio.provision.yarn.DacDaemonYarnApplication.YARN_CLUSTER_ID;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_HOSTNAME;
import static org.apache.twill.api.Configs.Keys.HEAP_RESERVED_MIN_RATIO;
import static org.apache.twill.api.Configs.Keys.JAVA_RESERVED_MEMORY_MB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.nodes.NodeProvider;
import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.provision.Cluster;
import com.dremio.provision.ClusterEnriched;
import com.dremio.provision.ClusterState;
import com.dremio.provision.ClusterType;
import com.dremio.provision.Container;
import com.dremio.provision.Containers;
import com.dremio.provision.DistroType;
import com.dremio.provision.Property;
import com.dremio.provision.RunId;
import com.dremio.provision.service.ProvisioningDefaultsConfigurator;
import com.dremio.provision.service.ProvisioningHandlingException;
import com.dremio.provision.service.ProvisioningServiceDelegate;
import com.dremio.provision.service.ProvisioningStateListener;
import com.dremio.provision.yarn.DacDaemonYarnApplication;
import com.dremio.provision.yarn.YarnController;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * YARN Provisioning Service
 */
public class YarnService implements ProvisioningServiceDelegate {

  private static final Logger logger = LoggerFactory.getLogger(YarnService.class);

  private final YarnController yarnController;
  private final NodeProvider executionNodeProvider;
  private final ProvisioningStateListener stateListener;
  private final ProvisioningDefaultsConfigurator defaultsConfigurator;
  @VisibleForTesting
  final ConcurrentMap<RunId, OnTerminatingRunnable> terminatingThreads = new ConcurrentHashMap<>();


  private static final ImmutableSet<String> EXCLUDED =
    ImmutableSet.of(
    FS_DEFAULT_NAME_KEY,
    RM_HOSTNAME,
    DremioConfig.YARN_HEAP_SIZE
    );

  public YarnService(ProvisioningStateListener stateListener, NodeProvider executionNodeProvider) {
    this(stateListener, new YarnController(), executionNodeProvider);
  }

  @VisibleForTesting
  YarnService(ProvisioningStateListener stateListener, YarnController controller, NodeProvider executionNodeProvider) {
    this.yarnController = controller;
    this.stateListener = stateListener;
    this.executionNodeProvider = executionNodeProvider;
    this.defaultsConfigurator = new YarnDefaultsConfigurator();
  }

  @Override
  public ClusterType getType() {
    return ClusterType.YARN;
  }


  @Override
  public ClusterEnriched startCluster(Cluster cluster) throws YarnProvisioningHandlingException {
    Preconditions.checkArgument(cluster.getState() != ClusterState.RUNNING);
    return startClusterAsync(cluster);
  }

  private ClusterEnriched startClusterAsync(Cluster cluster) throws YarnProvisioningHandlingException {
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    updateYarnConfiguration(cluster, yarnConfiguration);

    List<Property> props = cluster.getClusterConfig().getSubPropertyList();
    // only to show those props on UI/API
    defaultsConfigurator.getDistroTypeDefaultsConfigurator(
      cluster.getClusterConfig().getDistroType(), cluster.getClusterConfig().getIsSecure()).mergeProperties(props);
    List<Property> cleansedProperties = new ArrayList<>();
    for (Property prop : props) {
      if (!EXCLUDED.contains(prop.getKey())) {
        cleansedProperties.add(prop);
      }
    }

    // async call - unfortunately I can not add event handlers before start of TwillController
    // which means we can miss failures
    TwillController twillController = yarnController.startCluster(yarnConfiguration, cleansedProperties);

    String runId = twillController.getRunId().getId();
    RunId dRunId = new RunId(runId);
    cluster.setState(ClusterState.STARTING);
    cluster.setRunId(dRunId);

    OnRunningRunnable onRunning = new OnRunningRunnable(cluster);

    twillController.onRunning(onRunning, Threads.SAME_THREAD_EXECUTOR);

    initOnTerminatingThread(cluster, twillController);

    return getClusterInfo(cluster);
  }

  @Override
  public void stopCluster(Cluster cluster) throws YarnProvisioningHandlingException {
    stopClusterAsync(cluster);
  }

  private void stopClusterAsync(Cluster cluster) throws YarnProvisioningHandlingException {
    TwillController controller = getTwillControllerHelper(cluster);
    if (cluster.getState() == ClusterState.STOPPED || cluster.getState() == ClusterState.FAILED) {
      // nothing to do - probably termination routine was called already
      return;
    }
    if (controller == null) {
      if (cluster.getState() != ClusterState.STOPPED && cluster.getState() != ClusterState.FAILED) {
        logger.warn("Either cluster is already stopped or YarnTwillRunnerService was not initialized yet. You may want to try again");
        cluster.setState(ClusterState.STOPPED);
      }
      return;
    }
    // async call
    cluster.setState(ClusterState.STOPPING);
    controller.terminate();
    return;
  }

  @Override
  public ClusterEnriched resizeCluster(Cluster cluster) throws YarnProvisioningHandlingException {
    if (ClusterState.RUNNING != cluster.getState()) {
      // nothing to do in resizing
      return new ClusterEnriched(cluster);
    }
    return resizeClusterAsync(cluster);
  }

  private ClusterEnriched resizeClusterAsync(Cluster cluster) throws YarnProvisioningHandlingException {
    int requestedContainerCount = cluster.getClusterConfig().getClusterSpec().getContainerCount().intValue();
    TwillController controller = getTwillControllerHelper(cluster);
    if (controller != null) {
      // async call
      controller.changeInstances(DacDaemonYarnApplication.YARN_RUNNABLE_NAME, requestedContainerCount);
      cluster.getClusterConfig().getClusterSpec().setContainerCount(requestedContainerCount);
      return getClusterInfo(cluster);
    }
    throw new YarnProvisioningHandlingException("Can not find running application. Unable to resize. May be a " +
      "temporary condition");

  }

  @Override
  public ClusterEnriched getClusterInfo(final Cluster cluster) throws YarnProvisioningHandlingException {
    // get info about cluster
    // children should do the rest
    TwillController controller = getTwillControllerHelper(cluster);
    final ClusterEnriched clusterEnriched = new ClusterEnriched(cluster);
    if (controller != null) {
      ResourceReport report = controller.getResourceReport();
      if (report == null) {
        return clusterEnriched;
      }
      Collection<TwillRunResources> runResources = report.getRunnableResources(DacDaemonYarnApplication.YARN_RUNNABLE_NAME);

      Set<String> activeContainerIds = new HashSet<>();
      for(NodeEndpoint ep : executionNodeProvider.getNodes()){
        if(ep.hasProvisionId()){
          activeContainerIds.add(ep.getProvisionId());
        }
      }

      Containers containers = new Containers();
      clusterEnriched.setRunTimeInfo(containers);

      List<Container> runningList = new ArrayList<Container>();
      List<Container> disconnectedList = new ArrayList<Container>();
      clusterEnriched.getRunTimeInfo().setRunningList(runningList);
      clusterEnriched.getRunTimeInfo().setDisconnectedList(disconnectedList);

      for (TwillRunResources runResource : runResources) {
        Container container = new Container();
        container.setContainerId(runResource.getContainerId());
        container.setContainerPropertyList(new ArrayList<Property>());
        container.getContainerPropertyList().add(new Property("host", runResource.getHost()));
        container.getContainerPropertyList().add(new Property("memoryMB", "" + runResource.getMemoryMB()));
        container.getContainerPropertyList().add(new Property("virtualCoreCount", "" + runResource.getVirtualCores()));
        if(activeContainerIds.contains(runResource.getContainerId())) {
          runningList.add(container);
        } else {
          disconnectedList.add(container);
        }
      }
      int yarnDelta = (cluster.getClusterConfig().getClusterSpec().getContainerCount() - runResources.size());
      if (yarnDelta > 0) {

        // pending
        clusterEnriched.getRunTimeInfo().setPendingCount(yarnDelta);
      } else if (yarnDelta < 0) {
        // decomissioning kind of
        clusterEnriched.getRunTimeInfo().setDecommissioningCount(Math.abs(yarnDelta));
      }

      clusterEnriched.getRunTimeInfo().setProvisioningCount(disconnectedList.size());

    }
    return clusterEnriched;
  }

  @VisibleForTesting
  protected String updateYarnConfiguration(Cluster cluster, YarnConfiguration yarnConfiguration) {
    String rmAddress = null;
    // make sure we set defaults first - before we overwrite it with props from ClusterConfig
    setYarnDefaults(cluster, yarnConfiguration);
    List<Property> keyValues = cluster.getClusterConfig().getSubPropertyList();
    if ( keyValues != null && !keyValues.isEmpty()) {
      for (Property property : keyValues) {
        yarnConfiguration.set(property.getKey(), property.getValue());
        if (RM_HOSTNAME.equalsIgnoreCase(property.getKey())) {
          rmAddress = property.getValue();
        }
      }
    }
    String queue = cluster.getClusterConfig().getClusterSpec().getQueue();
    if (queue != null && !queue.isEmpty()) {
      yarnConfiguration.set(DacDaemonYarnApplication.YARN_QUEUE_NAME, queue);
    }
    Integer memoryOnHeap = cluster.getClusterConfig().getClusterSpec().getMemoryMBOnHeap();
    Integer memoryOffHeap = cluster.getClusterConfig().getClusterSpec().getMemoryMBOffHeap();
    Preconditions.checkNotNull(memoryOnHeap, "Heap Memory is not specified");
    Preconditions.checkNotNull(memoryOffHeap, "Direct Memory is not specified");

    final int totalMemory = memoryOnHeap.intValue() + memoryOffHeap.intValue();
    // ratio between onheap and total memory. Since we need more direct memory
    // trying to make this ratio small, so heap would be as specified
    // setting this ratio based on onheap and offheap memory
    // so it will never go over the limit
    final double heapToDirectMemRatio = (double) (memoryOnHeap.intValue()) / totalMemory;

    yarnConfiguration.setDouble(HEAP_RESERVED_MIN_RATIO, Math.min(heapToDirectMemRatio, 0.1D));
    yarnConfiguration.setInt(DacDaemonYarnApplication.YARN_MEMORY_ON_HEAP, memoryOnHeap.intValue());
    yarnConfiguration.setInt(DacDaemonYarnApplication.YARN_MEMORY_OFF_HEAP, memoryOffHeap.intValue());
    yarnConfiguration.setInt(JAVA_RESERVED_MEMORY_MB, memoryOffHeap.intValue());

    Integer cpu = cluster.getClusterConfig().getClusterSpec().getVirtualCoreCount();
    if (cpu != null) {
      yarnConfiguration.setInt(DacDaemonYarnApplication.YARN_CPU, cpu.intValue());
    }
    Integer containerCount = cluster.getClusterConfig().getClusterSpec().getContainerCount();
    if (containerCount != null) {
      yarnConfiguration.setInt(DacDaemonYarnApplication.YARN_CONTAINER_COUNT, containerCount.intValue());
    }
    String clusterName = cluster.getClusterConfig().getName();
    if (clusterName != null) {
      yarnConfiguration.set(DacDaemonYarnApplication.YARN_APP_NAME, clusterName);
    }
    yarnConfiguration.set(YARN_CLUSTER_ID, cluster.getId().getId());

    return rmAddress;
  }

  private void setYarnDefaults(Cluster cluster, YarnConfiguration yarnConfiguration) {
    DistroType dType = cluster.getClusterConfig().getDistroType();
    Map<String, String> additionalProps =
      defaultsConfigurator.getDistroTypeDefaultsConfigurator(dType, cluster.getClusterConfig().getIsSecure()).getAllDefaults();
    for (Map.Entry<String,String> entry : additionalProps.entrySet()) {
      yarnConfiguration.set(entry.getKey(), entry.getValue());
    }
  }

  private TwillController getTwillControllerHelper(Cluster cluster) throws YarnProvisioningHandlingException {
    RunId runId = cluster.getRunId();
    if (runId == null) {
      return null;
    }

    TwillRunnerService twillService = getTwillService(cluster);
    if (twillService == null) {
      logger.error("YarnTwillRunnerService is null. Possibly was not instantiated yet");
      return null;
    }

    String applicationName = cluster.getClusterConfig().getName();
    if (applicationName == null) {
      applicationName = DacDaemonYarnApplication.YARN_APPLICATION_NAME_DEFAULT;
    }

    TwillController controller =  twillService.lookup(applicationName, RunIds.fromString(runId.getId()));
    initOnTerminatingThread(cluster,controller);

    return controller;
  }

  private TwillRunnerService getTwillService(Cluster cluster) {
    TwillRunnerService twillService = yarnController.getTwillService(cluster.getId());
    if (twillService == null) {
      // possibly DremioMaster restarted, start TwillRunner
      YarnConfiguration yarnConfig = new YarnConfiguration();
      updateYarnConfiguration(cluster, yarnConfig);
      yarnController.startTwillRunner(yarnConfig);
    }
    return yarnController.getTwillService(cluster.getId());
  }

  private void initOnTerminatingThread(final Cluster cluster, final TwillController controller) {
    if (controller == null || cluster.getRunId() == null) {
      // nothing we can do here
      return;
    }
    // if termRunnable is not there most likely it was never created (process restarted)
    // or we try to stop before start - unlikely though
    // create new one
    OnTerminatingRunnable onTerminating = new OnTerminatingRunnable(cluster, controller);
    OnTerminatingRunnable onTerminatingPrev = terminatingThreads.putIfAbsent(cluster.getRunId(), onTerminating);
    if (onTerminatingPrev == null) {
      logger.info("Cluster with ID: {} is adding onTerminatingThread for runId: ", cluster.getId(), cluster.getRunId());
      controller.onTerminated(onTerminating, Threads.SAME_THREAD_EXECUTOR);
    }
  }

  class OnRunningRunnable implements Runnable {

    private final Cluster cluster;

    OnRunningRunnable(Cluster cluster) {
      this.cluster = cluster;
    }
    @Override
    public void run() {
      // may be no need to do it - just a confirmation that it is indeed running
      cluster.setState(ClusterState.RUNNING);
      cluster.setError(null);
      logger.info("Cluster with ID: {} is started", cluster.getId());
      try {
        stateListener.started(cluster);
      } catch (ProvisioningHandlingException e) {
        logger.error("Exception while updating cluster state", e);
      }
    }
  }

  class OnTerminatingRunnable implements Runnable {

    private final Cluster cluster;
    private final TwillController controller;

    OnTerminatingRunnable(Cluster cluster, TwillController controller) {
      this.cluster = cluster;
      this.controller = controller;
    }

    @Override
    public void run() {
      try {
        logger.info("Cluster with ID: {} is terminating", cluster.getId());
        Future<? extends ServiceController> futureController = controller.terminate();
        futureController.get();
        ServiceController.TerminationStatus terminationStatus = controller.getTerminationStatus();
        if (terminationStatus != null) {
          logger.info("Cluster with ID: {} is terminated in YARN with status: {}", cluster.getId(), terminationStatus);
          switch (terminationStatus) {
            case SUCCEEDED:
            case KILLED:
              cluster.setState(ClusterState.STOPPED);
              break;
            case FAILED:
              cluster.setState(ClusterState.FAILED);
              break;
            default:
              logger.error("Unknown terminationStatus {}", terminationStatus);
              cluster.setState(ClusterState.STOPPED);
              break;
          }
        } else {
          cluster.setState(ClusterState.STOPPED);
          logger.info("Cluster with ID: {} is terminated in YARN", cluster.getId());
        }
      } catch (InterruptedException e) {
        // not much to do here
        logger.error("InterruptedException is thrown while waiting to terminate cluster with ID: {}", cluster.getId());
      } catch (ExecutionException e) {
        logger.error("Exception caused cluster with ID: " + cluster.getId() + " termination", e);
        cluster.setError(e.getCause().getLocalizedMessage());
        cluster.setState(ClusterState.FAILED);
      }
      try {
        if (cluster.getRunId() != null) {
          terminatingThreads.remove(cluster.getRunId());
        } else {
          logger.warn("Cluster with ID: {} does not have runId set upon termination", cluster.getId());
        }
        cluster.setRunId(null);
        yarnController.invalidateTwillService(cluster.getId());
        stateListener.stopped(cluster);
      } catch (ProvisioningHandlingException ex) {
        logger.error("Exception while updating cluster state", ex);
      }
    }
  }
}
