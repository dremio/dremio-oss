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
package com.dremio.service.scheduler;

import static com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.SET_DATA;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.PathCommand;
import static com.dremio.service.scheduler.SchedulerEvents.PerTaskRecoveryEvents;
import static com.dremio.service.scheduler.SchedulerEvents.RecoveryRejectReason;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.RegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.dremio.service.coordinator.exceptions.PathMissingException;
import com.google.common.base.Preconditions;

/**
 * Sets up recovery monitoring of task schedules that hashes into the current node given by {@code currentEndpoint}.
 * <p>
 * When nodes join and leave cluster recovery monitoring of all tasks are automatically adjusted based on
 * rehashing. In the future, we could use consistent hashing to reduce watcher load. For now, since the number of
 * tasks does not exceed the 'hundreds' range, we should be ok with regular hashing.
 * </p>
 */
final class TaskRecoveryMonitor implements NodeStatusListener, AutoCloseable {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TaskRecoveryMonitor.class);
  private static final int SET_WATCH_DELAY_SECS = 30;
  private static final Comparator<NodeEndpoint> ENDPOINT_COMPARATOR = Comparator
    .comparing(NodeEndpoint::getAddress)
    .thenComparing(NodeEndpoint::getFabricPort);
  private static final Function<Set<NodeEndpoint>, String> ENDPOINTS_AS_STRING =
    (endpoints) -> endpoints.stream()
      .map(e -> e.getAddress() + ":" + e.getFabricPort())
      .collect(Collectors.joining(" , "));

  private final ClusteredSingletonCommon schedulerCommon;
  // Cluster coordinator instance provide for this service instance
  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  // Current group membership
  private final SortedSet<NodeEndpoint> groupMembers;
  // lock to protect group membership updates
  private final ReentrantLock membershipLock;
  // all tasks that needs recovery monitoring
  private final Map<String, PerTaskRecoveryMonitor> allTasks;
  // tasks interested in death of service instances
  private final Set<PerTaskRecoveryMonitor> deathWatchSet;
  // all tasks which is currently being monitored by this instance
  private final Set<String> locallyMonitoredTasks;
  private final SchedulerEvents events;
  // Handle provided by the underlying service membership system
  private volatile RegistrationHandle registrationHandle;
  // index into the membership group for this instance, -1 if instance not in group yet
  private int currentEndPointLocation;

  TaskRecoveryMonitor(Provider<ClusterCoordinator> clusterCoordinatorProvider,
                      ClusteredSingletonCommon schedulerCommon,
                      SchedulerEvents events) {
    this.events = events;
    this.schedulerCommon = schedulerCommon;
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
    this.allTasks = new ConcurrentHashMap<>();
    this.membershipLock = new ReentrantLock();
    this.groupMembers = new TreeSet<>(ENDPOINT_COMPARATOR);
    this.locallyMonitoredTasks = new HashSet<>();
    this.currentEndPointLocation = -1;
    this.deathWatchSet = ConcurrentHashMap.newKeySet();
  }

  public void start() {
    ServiceSet s = clusterCoordinatorProvider.get().getOrCreateServiceSet(this.schedulerCommon.getBaseServiceName());
    s.addNodeStatusListener(this);
    this.registrationHandle = s.register(schedulerCommon.getThisEndpoint());
    int newSize = 0;
    membershipLock.lock();
    try {
      this.groupMembers.addAll(s.getAvailableEndpoints());
      newSize = this.groupMembers.size();
      adjustCurrentLocation();
    } finally {
      membershipLock.unlock();
      events.membershipChanged(newSize);
    }
  }

  PerTaskRecoveryMonitor addTask(PerTaskRecoveryInfo recoveryInfo, PerTaskRecoveryEvents recoveryEventSink) {
    Preconditions.checkState(registrationHandle != null, "Node %s not registered. Cannot add task %s for recovery",
      schedulerCommon.getThisEndpoint(), recoveryInfo.getTaskName());
    boolean startMonitoring = false;
    final PerTaskRecoveryMonitor recoveryMonitor = allTasks.computeIfAbsent(recoveryInfo.getTaskName(),
      (k) -> new PerTaskRecoveryMonitor(recoveryInfo, recoveryEventSink));
    membershipLock.lock();
    try {
      if (iAmOwner(recoveryMonitor)) {
        // add a watcher against task
        locallyMonitoredTasks.add(recoveryInfo.getTaskName());
        startMonitoring = true;
      }
    } finally {
      membershipLock.unlock();
    }
    if (startMonitoring) {
      recoveryMonitor.startRecoveryMonitoring(true);
    }
    return recoveryMonitor;
  }

  void removeTask(String taskName) {
    PerTaskRecoveryMonitor monitor = allTasks.remove(taskName);
    if (monitor == null) {
      LOGGER.info("Task {} already removed from monitoring", taskName);
      return;
    }
    monitor.stopRecoveryMonitoring();
    membershipLock.lock();
    try {
      locallyMonitoredTasks.remove(taskName);
    } finally {
      membershipLock.unlock();
    }
  }

  @Override
  public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
    LOGGER.info("Nodes `{}` is now removed from the cluster", ENDPOINTS_AS_STRING.apply(unregisteredNodes));
    if (unregisteredNodes.isEmpty()) {
      // nothing to unregister. spurious request
      return;
    }
    adjustMembership(false, unregisteredNodes);
    deathWatchSet.forEach(PerTaskRecoveryMonitor::runImmediateIfRecoveryOwner);
  }

  @Override
  public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
    LOGGER.info("Nodes `{}` is now added to the cluster", ENDPOINTS_AS_STRING.apply(registeredNodes));
    if (registeredNodes.isEmpty()) {
      // nothing to register. spurious request
      return;
    }
    adjustMembership(true, registeredNodes);
  }

  @Override
  public void close() {
    if (registrationHandle != null) {
      registrationHandle.close();
      registrationHandle = null;
    }
  }

  private void adjustMembership(boolean addMembers, Set<NodeEndpoint> nodes) {
    final Set<String> tasksToAdd = new HashSet<>();
    final Set<String> tasksToRemove = new HashSet<>();
    int newSize = 0;
    membershipLock.lock();
    try {
      if (addMembers) {
        groupMembers.addAll(nodes);
      } else {
        groupMembers.removeAll(nodes);
      }
      newSize = groupMembers.size();
      adjustCurrentLocation();
      rehashRecoveryMonitoring(tasksToAdd, tasksToRemove);
    } finally {
      membershipLock.unlock();
      events.membershipChanged(newSize);
    }
    if (!tasksToAdd.isEmpty()) {
      LOGGER.info("Taking over recovery monitoring for tasks `{}`", String.join(" ; ", tasksToAdd));
      events.tasksAddedToMembership(tasksToAdd.size());
      tasksToAdd.stream().map(allTasks::get).filter(Objects::nonNull).forEach((tracker -> {
        LOGGER.info("Starting recovery monitoring for {}", tracker);
        tracker.startRecoveryMonitoring(false);
      }));
    }
    if (!tasksToRemove.isEmpty()) {
      LOGGER.info("Stopping recovery monitoring for tasks `{}`", String.join(" ; ", tasksToRemove));
      events.tasksRemovedFromMembership(tasksToRemove.size());
      tasksToRemove.stream().map(allTasks::get).filter(Objects::nonNull).forEach((tracker -> {
        LOGGER.info("Stopping recovery monitoring for {}", tracker);
        tracker.stopRecoveryMonitoring();
      }));
    }
  }

  // protected under the membership lock
  private void rehashRecoveryMonitoring(Set<String> tasksToAdd, Set<String> tasksToRemove) {
    allTasks.values().forEach((tracker) -> {
      if (iAmOwner(tracker)) {
        // I am the recovery owner for this task
        if (!locallyMonitoredTasks.contains(tracker.getTaskName())) {
          locallyMonitoredTasks.add(tracker.getTaskName());
          tasksToAdd.add(tracker.getTaskName());
        }
      } else {
        if (locallyMonitoredTasks.contains(tracker.getTaskName())) {
          tasksToRemove.add(tracker.getTaskName());
          locallyMonitoredTasks.remove(tracker.getTaskName());
        }
      }
    });
  }

  // protected against the membership lock
  private boolean iAmOwner(PerTaskRecoveryMonitor tracker) {
    final int numMembers = this.groupMembers.size();
    if (numMembers == 0) {
      // this is a transient situation mostly during shutdown of all nodes. Just log for now.
      LOGGER.info("Cluster membership found to be empty while determining recovery owner for task {}",
        tracker.getTaskName());
      return false;
    }
    return currentEndPointLocation == (tracker.getTaskName().hashCode() & Integer.MAX_VALUE) % numMembers;
  }

  // protected against the membership lock
  private void adjustCurrentLocation() {
    int currentLoc = 0;
    this.currentEndPointLocation = -1;
    for (NodeEndpoint e : this.groupMembers) {
      if (ENDPOINT_COMPARATOR.compare(e, schedulerCommon.getThisEndpoint()) == 0) {
        this.currentEndPointLocation = currentLoc;
        break;
      }
      currentLoc++;
    }
  }

  interface PerTaskRecoveryInfo extends PerTaskInfo {
    RecoveryRejectReason tryRecover();

    void updateLastRun(Instant newValue);

    boolean runImmediateForce();
  }

  final class PerTaskRecoveryMonitor {
    private final PerTaskRecoveryInfo taskRecoveryInfo;
    private final PerTaskRecoveryEvents recoveryStats;
    private volatile boolean recoveryOwner;

    private PerTaskRecoveryMonitor(PerTaskRecoveryInfo recoveryInfo, PerTaskRecoveryEvents recoveryStats) {
      this.recoveryOwner = false;
      this.recoveryStats = recoveryStats;
      this.taskRecoveryInfo = recoveryInfo;
    }

    String getTaskName() {
      return taskRecoveryInfo.getTaskName();
    }

    /**
     * Starts recovery monitoring for this task, provided this service instance is not already the booking owner
     * as well.
     *
     * @param onAdd if the recovery monitoring is called during task addition
     */
    void startRecoveryMonitoring(boolean onAdd) {
      this.recoveryOwner = true;
      recoveryStats.recoveryMonitoringStarted();
      if (!onAdd) {
        setRecoveryWatch();
      }
    }

    /**
     * Stops recovery monitoring when this instance is no longer the recovery owner for this task (due to membership
     * changes).
     */
    void stopRecoveryMonitoring() {
      this.recoveryOwner = false;
      recoveryStats.recoveryMonitoringStopped();
    }

    void setRecoveryWatch() {
      // Set the recovery watch only if we do not have the booking and we are the recovery owner.
      // Recovery watch must be set through a schedule whenever we explicitly release booking
      // (except during cancellation).
      if (!taskRecoveryInfo.isBookingOwner() && recoveryOwner) {
        try {
          schedulerCommon.getTaskStore().whenDeleted(taskRecoveryInfo.getBookFqPathLocal()).thenRun(this::recover);
        } catch (PathMissingException e) {
          // the booking path does not exist. Try and grab it after a delay.
          LOGGER.info("No one has booked the task {}. Calling recovery directly after a delay", this);
          if (recoveryOwner) {
            schedulerCommon.getSchedulePool().schedule(this::recover, SET_WATCH_DELAY_SECS, TimeUnit.SECONDS);
          }
        }
      }
    }

    void addToDeathWatchLocal() {
      deathWatchSet.add(this);
      recoveryStats.addedToDeathWatch();
    }

    private void recover() {
      recoveryStats.recoveryRequested();
      if (taskRecoveryInfo.isDone()) {
        LOGGER.info("Task {} is already done. Spurious wakeup", this);
        recoveryStats.recoveryRejected(RecoveryRejectReason.TASK_DONE);
        return;
      }
      if (!recoveryOwner) {
        LOGGER.info("Recovery Ownership lost for task {}. Ignoring request to recover the schedule", this);
        recoveryStats.recoveryRejected(RecoveryRejectReason.NOT_RECOVERY_OWNER);
        return;
      }
      loadScheduleTime();
      final RecoveryRejectReason reason = taskRecoveryInfo.tryRecover();
      if (reason == null) {
        recoveryStats.recovered();
      } else {
        recoveryStats.recoveryRejected(reason);
      }
      setRecoveryWatch();
    }

    void storeScheduleTime() {
      byte[] data = new byte[8];
      ByteBuffer.wrap(data).putLong(Instant.now().toEpochMilli());
      try {
        schedulerCommon.getTaskStore().executeSingle(new PathCommand(SET_DATA,
          taskRecoveryInfo.getTaskFqPath(), data));
      } catch (Exception e) {
        // Just log and ignore as maximum issue is the possibility of a schedule missing a period
        LOGGER.warn("Unexpected exception while storing schedule at/last run time at path {}",
          taskRecoveryInfo.getTaskFqPath(), e);
      }
    }

    private void loadScheduleTime() {
      long storedTime = 0;
      final String taskFqPath = taskRecoveryInfo.getTaskFqPath();
      try {
        byte[] data = schedulerCommon.getTaskStore().getData(taskFqPath);
        if (data != null && data.length == 8) {
          storedTime = ByteBuffer.wrap(data).getLong();
        }
      } catch (Exception e) {
        // Just log and ignore as maximum issue is the possibility of a schedule missing a few cycles
        LOGGER.warn("Unexpected exception while loading schedule time at path {}", taskFqPath, e);
      }
      long currentTime = Instant.now().toEpochMilli();
      if (storedTime == 0 || storedTime > currentTime) {
        // we were not able to recover
        LOGGER.warn("Invalid stored time {} found in path {}", storedTime, taskFqPath);
        storedTime = currentTime;
      }
      taskRecoveryInfo.updateLastRun(Instant.ofEpochMilli(storedTime));
    }

    private void runImmediateIfRecoveryOwner() {
      if (recoveryOwner) {
        if (taskRecoveryInfo.runImmediateForce()) {
          recoveryStats.runOnDeath();
        } else {
          recoveryStats.failedToRunOnDeath();
        }
      }
    }
  }
}
