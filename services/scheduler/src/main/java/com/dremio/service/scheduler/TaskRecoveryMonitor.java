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
import static com.dremio.service.scheduler.ClusteredSingletonCommon.ENDPOINTS_AS_STRING;
import static com.dremio.service.scheduler.ClusteredSingletonCommon.ENDPOINT_AS_STRING;
import static com.dremio.service.scheduler.SchedulerEvents.PerTaskRecoveryEvents;
import static com.dremio.service.scheduler.SchedulerEvents.RecoveryRejectReason;

import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.RegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.dremio.service.coordinator.exceptions.PathMissingException;
import com.google.common.base.Preconditions;
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
import java.util.stream.Collectors;
import javax.inject.Provider;

/**
 * Sets up recovery monitoring of task schedules that hashes into the current node given by {@code
 * currentEndpoint}.
 *
 * <p>When nodes join and leave cluster recovery monitoring of all tasks are automatically adjusted
 * based on rehashing. In the future, we could use consistent hashing to reduce watcher load. For
 * now, since the number of tasks does not exceed the 'hundreds' range, we should be ok with regular
 * hashing.
 */
final class TaskRecoveryMonitor implements NodeStatusListener, AutoCloseable {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(TaskRecoveryMonitor.class);
  private static final int SET_WATCH_SHORT_DELAY_SECS = 10;
  private static final int SET_WATCH_LONG_DELAY_SECS = 60;
  private static final Comparator<NodeEndpoint> ENDPOINT_COMPARATOR =
      Comparator.comparing(NodeEndpoint::getAddress).thenComparing(NodeEndpoint::getFabricPort);

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
  // keep track whether nodes lost membership so that we can re-trigger run once on death schedules
  private volatile boolean nodesRemoved;
  // index into the membership group for this instance, -1 if instance not in group yet
  private int currentEndPointLocation;

  TaskRecoveryMonitor(
      Provider<ClusterCoordinator> clusterCoordinatorProvider,
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
    this.nodesRemoved = false;
  }

  public void start() {
    ServiceSet s =
        clusterCoordinatorProvider
            .get()
            .getOrCreateServiceSet(this.schedulerCommon.getBaseServiceName());
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

  void refresh() {
    membershipLock.lock();
    try {
      if (this.registrationHandle != null) {
        this.registrationHandle.close();
        this.groupMembers.clear();
        this.locallyMonitoredTasks.clear();
      }
    } finally {
      membershipLock.unlock();
    }
    start();
  }

  PerTaskRecoveryMonitor addTask(
      PerTaskRecoveryInfo recoveryInfo, PerTaskRecoveryEvents recoveryEventSink) {
    Preconditions.checkState(
        registrationHandle != null,
        "Node %s not registered. Cannot add task %s for recovery",
        schedulerCommon.getThisEndpoint(),
        recoveryInfo.getTaskName());
    boolean startMonitoring = false;
    final PerTaskRecoveryMonitor recoveryMonitor =
        allTasks.computeIfAbsent(
            recoveryInfo.getTaskName(),
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
    if (schedulerCommon.isZombie()) {
      return;
    }
    if (unregisteredNodes.isEmpty()) {
      // nothing to unregister. spurious request
      return;
    }
    LOGGER.info(
        "{}: Nodes `{}` is now removed from the cluster",
        ENDPOINT_AS_STRING.apply(schedulerCommon.getThisEndpoint()),
        ENDPOINTS_AS_STRING.apply(unregisteredNodes));
    adjustMembership(false, unregisteredNodes);
    nodesRemoved = true;
    deathWatchSet.forEach(PerTaskRecoveryMonitor::runImmediateIfRecoveryOwner);
  }

  @Override
  public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
    if (schedulerCommon.isZombie() || schedulerCommon.shouldIgnoreReconnects()) {
      return;
    }
    if (registeredNodes.isEmpty()) {
      // nothing to register. spurious request
      return;
    }
    LOGGER.info(
        "{}: Nodes `{}` is now added to the cluster",
        ENDPOINT_AS_STRING.apply(schedulerCommon.getThisEndpoint()),
        ENDPOINTS_AS_STRING.apply(registeredNodes));
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
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{}: Locally monitoring {} tasks `{}`. This node index is {}. Death set is `{}`",
            ENDPOINT_AS_STRING.apply(schedulerCommon.getThisEndpoint()),
            locallyMonitoredTasks.size(),
            String.join(" ; ", locallyMonitoredTasks),
            currentEndPointLocation,
            deathWatchSet.stream()
                .map(PerTaskRecoveryMonitor::getTaskName)
                .collect(Collectors.joining(" ; ")));
      }
      membershipLock.unlock();
      events.membershipChanged(newSize);
    }
    if (!tasksToAdd.isEmpty()) {
      final int numAdded = tasksToAdd.size();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{}: Taking over recovery monitoring for {} tasks `{}`. This node index is {}",
            ENDPOINT_AS_STRING.apply(schedulerCommon.getThisEndpoint()),
            numAdded,
            String.join(" ; ", tasksToAdd),
            currentEndPointLocation);
      }
      events.tasksAddedToMembership(numAdded);
      tasksToAdd.stream()
          .map(allTasks::get)
          .filter(Objects::nonNull)
          .forEach(
              (tracker -> {
                tracker.startRecoveryMonitoring(false);
              }));
    }
    if (!tasksToRemove.isEmpty()) {
      final int numRemoved = tasksToRemove.size();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{}: Stopping recovery monitoring for {} tasks '{}`. This node index is {}",
            ENDPOINT_AS_STRING.apply(schedulerCommon.getThisEndpoint()),
            numRemoved,
            String.join(" ; ", tasksToRemove),
            currentEndPointLocation);
      }
      events.tasksRemovedFromMembership(tasksToRemove.size());
      tasksToRemove.stream()
          .map(allTasks::get)
          .filter(Objects::nonNull)
          .forEach((PerTaskRecoveryMonitor::stopRecoveryMonitoring));
    }
  }

  // protected under the membership lock
  private void rehashRecoveryMonitoring(Set<String> tasksToAdd, Set<String> tasksToRemove) {
    allTasks
        .values()
        .forEach(
            (tracker) -> {
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
      LOGGER.info(
          "{}: Cluster membership found to be empty while determining recovery owner for task {}",
          ENDPOINT_AS_STRING.apply(schedulerCommon.getThisEndpoint()),
          tracker.getTaskName());
      return false;
    }
    return currentEndPointLocation
        == (tracker.getTaskName().hashCode() & Integer.MAX_VALUE) % numMembers;
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

    boolean runDeferredForce();
  }

  final class PerTaskRecoveryMonitor {
    private final PerTaskRecoveryInfo taskRecoveryInfo;
    private final PerTaskRecoveryEvents recoveryStats;
    private volatile boolean recoveryOwner;
    private volatile boolean deathWatchAdditionDone;

    private PerTaskRecoveryMonitor(
        PerTaskRecoveryInfo recoveryInfo, PerTaskRecoveryEvents recoveryStats) {
      this.recoveryOwner = false;
      this.recoveryStats = recoveryStats;
      this.taskRecoveryInfo = recoveryInfo;
      this.deathWatchAdditionDone = false;
    }

    String getTaskName() {
      return taskRecoveryInfo.getTaskName();
    }

    /**
     * Starts recovery monitoring for this task, provided this service instance is not already the
     * booking owner as well.
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
     * Stops recovery monitoring when this instance is no longer the recovery owner for this task
     * (due to membership changes).
     */
    void stopRecoveryMonitoring() {
      this.recoveryOwner = false;
      recoveryStats.recoveryMonitoringStopped();
    }

    void setRecoveryWatch() {
      // Set the recovery watch only if we do not have the booking and we are the recovery owner.
      // Recovery watch must be set through a schedule whenever we explicitly release booking
      // (except during cancellation).
      if (!taskRecoveryInfo.isBookingOwner() && recoveryOwner && schedulerCommon.isActive()) {
        try {
          LOGGER.debug("Setting Recovery watch for {}", this.getTaskName());
          schedulerCommon
              .getTaskStore()
              .whenDeleted(taskRecoveryInfo.getBookFqPathLocal())
              .thenRun(this::recover);
        } catch (PathMissingException e) {
          // the booking path does not exist. Try and grab it after a delay.
          LOGGER.info(
              "{}: No one has booked the task {}. Calling recovery directly after a delay",
              schedulerCommon.getThisEndpoint().getAddress(),
              getTaskName());
          if (recoveryOwner) {
            var delay =
                (taskRecoveryInfo.notInRunSet())
                    ? SET_WATCH_SHORT_DELAY_SECS
                    : SET_WATCH_LONG_DELAY_SECS;
            schedulerCommon.getSchedulePool().schedule(this::recover, delay, TimeUnit.SECONDS);
          }
        }
      }
    }

    void addToDeathWatchLocal(boolean afterRun) {
      deathWatchSet.add(this);
      recoveryStats.addedToDeathWatch();
      if (nodesRemoved && !deathWatchAdditionDone && !afterRun) {
        runImmediateIfRecoveryOwner();
      }
      deathWatchAdditionDone = true;
    }

    private void recover() {
      recoveryStats.recoveryRequested();
      if (taskRecoveryInfo.isDone()) {
        LOGGER.debug("Task {} is already done. Spurious wakeup", getTaskName());
        recoveryStats.recoveryRejected(RecoveryRejectReason.TASK_DONE);
        return;
      }
      if (!recoveryOwner || !schedulerCommon.isActive()) {
        LOGGER.debug(
            "{}: Recovery Ownership lost for task {}. Ignoring request to recover the schedule",
            ENDPOINT_AS_STRING.apply(schedulerCommon.getThisEndpoint()),
            getTaskName());
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
      if (taskRecoveryInfo.getSchedule().isLargePeriodicity()) {
        // recover schedule times only for large periodicity as for periodicity under or around a
        // minute
        // the loss will be negligible
        byte[] data = new byte[8];
        ByteBuffer.wrap(data).putLong(Instant.now().toEpochMilli());
        try {
          schedulerCommon
              .getTaskStore()
              .executeSingle(new PathCommand(SET_DATA, taskRecoveryInfo.getTaskFqPath(), data));
        } catch (Exception e) {
          // Just log and ignore as maximum issue is the possibility of a schedule missing a period
          LOGGER.warn(
              "Unexpected exception while storing schedule at/last run time at path {}",
              taskRecoveryInfo.getTaskFqPath(),
              e);
        }
      }
    }

    private void loadScheduleTime() {
      long currentTime = Instant.now().toEpochMilli();
      long storedTime = currentTime;
      if (taskRecoveryInfo.getSchedule().isLargePeriodicity()) {
        // recover schedule time only for large periodicity schedules
        storedTime = 0;
        final String taskFqPath = taskRecoveryInfo.getTaskFqPath();
        try {
          byte[] data = schedulerCommon.getTaskStore().getData(taskFqPath);
          if (data != null && data.length == 8) {
            storedTime = ByteBuffer.wrap(data).getLong();
          }
        } catch (Exception e) {
          // Just log and ignore as maximum issue is the possibility of a schedule missing a few
          // cycles
          LOGGER.warn("Unexpected exception while loading schedule time at path {}", taskFqPath, e);
        }
        if (storedTime == 0 || storedTime > currentTime) {
          // we were not able to recover
          LOGGER.warn("Invalid stored time {} found in path {}", storedTime, taskFqPath);
          storedTime = currentTime;
        }
      }
      taskRecoveryInfo.updateLastRun(Instant.ofEpochMilli(storedTime));
    }

    private void runImmediateIfRecoveryOwner() {
      if (recoveryOwner && schedulerCommon.isActive()) {
        if (taskRecoveryInfo.runDeferredForce()) {
          recoveryStats.runOnDeath();
        } else {
          recoveryStats.failedToRunOnDeath();
        }
      } else {
        LOGGER.info(
            "{}: Not recovery owner for task {}. Is shutting down {}",
            ENDPOINT_AS_STRING.apply(schedulerCommon.getThisEndpoint()),
            getTaskName(),
            !schedulerCommon.isActive());
      }
    }
  }
}
