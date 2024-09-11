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

import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.CREATE_EPHEMERAL;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.CREATE_EPHEMERAL_SEQUENTIAL;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.CREATE_PERSISTENT;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.DELETE;
import static com.dremio.service.coordinator.LinearizableHierarchicalStore.CommandType.SET_DATA;

import com.dremio.common.UncaughtExceptionHandlers;
import com.dremio.io.file.Path;
import com.dremio.service.coordinator.LinearizableHierarchicalStore;
import com.dremio.service.coordinator.LinearizableHierarchicalStore.PathCommand;
import com.dremio.service.coordinator.exceptions.PathExistsException;
import com.dremio.service.coordinator.exceptions.PathMissingException;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/** Monitors weights of weight based tasks and schedules and interacts with load controller */
final class TaskWeightTracker implements WeightBalancer {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(TaskWeightTracker.class);
  private static final long L_NODE_CHECK_PERIODICITY_MILLIS = TimeUnit.SECONDS.toMillis(10);
  private static final long REFRESH_PERIODICITY_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final long FIRST_TIME_PERIODICITY_MILLIS = TimeUnit.SECONDS.toMillis(10);

  private enum WeightRole {
    // I am the heaviest node
    L_NODE(TimeUnit.SECONDS.toMillis(60)),
    // I am the lightest node
    S_NODE(0),
    // I am neither
    NEITHER(TimeUnit.SECONDS.toMillis(30)),
    // My weight role is unknown
    UNKNOWN(TimeUnit.SECONDS.toMillis(60));

    private final long migrationsInThreshold;

    WeightRole(long thresholdMillis) {
      this.migrationsInThreshold = thresholdMillis;
    }

    long getMigrationsInThreshold() {
      return migrationsInThreshold;
    }
  }

  private static final String WEIGHT_PATH_NAME = "weights";
  private static final String WEIGHT_PREFIX = "node-";
  private static final String BALANCING_PATH_NAME = "balancing";
  private final ClusteredSingletonCommon schedulerCommon;
  // all locally owned tasks
  private final Map<String, PerTaskWeightTracker> allTasks;
  private final SchedulerEvents events;
  // weight balancing thread; monitors all weights and decides on migrating out tasks to keep the
  // balance
  private final WeightBalancingThread balancingThread;
  // weight refresher thread; refreshes self weight periodically so that other participants can see
  // weight changes
  private final WeightRefresherThread refresherThread;
  // ephemeral z-node to denote a migration is in progress to keep balance
  private final String balanceFqPath;
  // path containing weight data of all participants, shared across the cluster
  private final String weightsFqPath;
  // list containing pending migrations out; list is only used by the balancer thread
  private final List<PerTaskWeightTracker> migratingOutTasks;
  // difference in weight between the heaviest and lightest that can be tolerated
  private final int weightTolerance;
  // keeps track weight of this node (self)
  private final SelfWeight selfWeight;
  // current role of this thread
  private WeightRole currentRole;
  // session id of the balancing z-node
  private long lRoleSessionId;

  TaskWeightTracker(
      ClusteredSingletonCommon schedulerCommon,
      SchedulerEvents events,
      int weightBasedBalancingPeriodSecs,
      int weightTolerance) {
    this.schedulerCommon = schedulerCommon;
    this.allTasks = new ConcurrentHashMap<>();
    this.events = events;
    this.weightTolerance = weightTolerance;
    this.currentRole = WeightRole.UNKNOWN;
    this.migratingOutTasks = new ArrayList<>();
    this.balanceFqPath = schedulerCommon.getWeightFqPath() + Path.SEPARATOR + BALANCING_PATH_NAME;
    this.weightsFqPath = schedulerCommon.getWeightFqPath() + Path.SEPARATOR + WEIGHT_PATH_NAME;
    this.lRoleSessionId = PerTaskInfo.INVALID_SESSION_ID;
    this.selfWeight = new SelfWeight();
    this.balancingThread = new WeightBalancingThread(weightBasedBalancingPeriodSecs);
    this.balancingThread.setUncaughtExceptionHandler(UncaughtExceptionHandlers.processExit());
    this.refresherThread = new WeightRefresherThread(weightBasedBalancingPeriodSecs);
    this.refresherThread.setUncaughtExceptionHandler(UncaughtExceptionHandlers.processExit());
  }

  @Override
  public void start() {
    createWeightsRootPathIgnoreIfExists();
    refresherThread.start();
    balancingThread.start();
  }

  @Override
  public void close() throws Exception {
    this.balancingThread.close();
    this.refresherThread.close();
  }

  /**
   * Add tasks that is currently self owned.
   *
   * <p>The moment ownership is lost the caller must call {@code removeTask}.
   *
   * @param weightInfo interface used to interact with the main task tracker
   */
  @Override
  public void addTask(
      PerTaskWeightInfo weightInfo,
      boolean migratedIn,
      SchedulerEvents.PerTaskLoadEvents loadEventsSink) {
    if (weightInfo.getSchedule().getWeightProvider() == null) {
      // weight tracking is only for task schedules specifying weight tracking
      LOGGER.warn("Task {} is not enabled for weight tracking", weightInfo.getTaskName());
      return;
    }
    if (migratedIn) {
      LOGGER.info(
          "Task {} migrated in to {}",
          weightInfo.getTaskName(),
          schedulerCommon.getThisEndpoint().getAddress());
    }
    allTasks.put(
        weightInfo.getTaskName(), new PerTaskWeightTracker(weightInfo, migratedIn, loadEventsSink));
    refresherThread.localWeightChanged();
  }

  /**
   * Remove tasks when the ownership is lost locally for this task.
   *
   * @param taskName name of the task
   */
  @Override
  public void removeTask(String taskName) {
    allTasks.remove(taskName);
    refresherThread.localWeightChanged();
  }

  /**
   * To check whether this node is currently eligible for an in migration.
   *
   * <p>A node is eligible for an in migration if it is the S-node. But if a task has been in the
   * run set for far too long, other node(s) may accept in migrations.
   *
   * @param millisInRunSet amount of time in millis this task was in the runset queue waiting for
   *     migration
   * @return true, if eligible
   */
  @Override
  public boolean isEligibleForInMigrations(long millisInRunSet) {
    return (millisInRunSet >= currentRole.getMigrationsInThreshold());
  }

  private void createWeightsRootPathIgnoreIfExists() {
    try {
      schedulerCommon
          .getTaskStore()
          .executeSingle(new PathCommand(CREATE_PERSISTENT, weightsFqPath));
    } catch (PathExistsException e) {
      LOGGER.debug("Path {} already exists", weightsFqPath, e);
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to create root paths for weight tracking. Cannot Proceed", e);
    }
  }

  private boolean isBalancingInProgress() {
    try {
      schedulerCommon
          .getTaskStore()
          .whenDeleted(this.balanceFqPath)
          .thenRun(balancingThread::recompute);
      return true;
    } catch (PathMissingException e) {
      return false;
    }
  }

  /** Watch handler that handles any changes to balancing participants */
  private void dataOrMembershipChanged() {
    if (isBalancingInProgress()) {
      return;
    }
    balancingThread.recompute();
  }

  private boolean readAllWeights(List<PerNodeWeight> weightList) {
    boolean success = false;
    boolean selfNodeFound = false;
    while (!success) {
      weightList.clear();
      List<String> latestWeightPaths = null;
      CompletableFuture<Void> onChanged = new CompletableFuture<>();
      onChanged.thenRun(this::dataOrMembershipChanged);
      try {
        latestWeightPaths = schedulerCommon.getTaskStore().getChildren(weightsFqPath, onChanged);
      } catch (PathMissingException e) {
        throw new RuntimeException(e);
      }
      success = true;
      for (String weightPath : latestWeightPaths) {
        var wp = weightsFqPath + Path.SEPARATOR + weightPath;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "{} Scanning weight path {}", schedulerCommon.getThisEndpoint().getAddress(), wp);
        }
        var stats = schedulerCommon.getTaskStore().getStats(wp);
        if (stats == null) {
          LOGGER.warn("Unable to get stats for path {}", wp);
          success = false;
          break;
        }
        if (selfWeight.checkMatch(stats, wp)) {
          selfNodeFound = true;
        } else {
          byte[] data;
          try {
            data = schedulerCommon.getTaskStore().getData(wp, onChanged);
          } catch (PathMissingException e) {
            success = false;
            break;
          }
          weightList.add(new PerNodeWeight(data));
        }
      }
    }
    return selfNodeFound;
  }

  private boolean takeOverAsLNode() {
    try {
      final var cmd = new PathCommand(CREATE_EPHEMERAL, balanceFqPath);
      schedulerCommon.getTaskStore().executeSingle(cmd);
      final var stats = schedulerCommon.getTaskStore().getStats(balanceFqPath);
      Preconditions.checkState(
          stats != null, "Unexpected missing ephemeral z-node path " + balanceFqPath);
      lRoleSessionId = stats.getSessionId();
      currentRole = WeightRole.L_NODE;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{}: Taking over as L-NODE {}",
            schedulerCommon.getThisEndpoint().getAddress(),
            lRoleSessionId);
      }
      return true;
    } catch (PathExistsException | PathMissingException e) {
      return false;
    }
  }

  private boolean checkIfAllTasksMigrated() {
    if (currentRole.equals(WeightRole.L_NODE)) {
      var stats = schedulerCommon.getTaskStore().getStats(balanceFqPath);
      if (stats == null || stats.getSessionId() != lRoleSessionId) {
        currentRole = WeightRole.UNKNOWN;
        lRoleSessionId = PerTaskInfo.INVALID_SESSION_ID;
        migratingOutTasks.clear();
        return true;
      }
      // we are the genuine L-node, check if migration is over
      migratingOutTasks.removeIf(PerTaskWeightTracker::notInRunSet);
      if (migratingOutTasks.isEmpty()) {
        // fully migrated, remove balancing
        removeBalancingMarker();
        currentRole = WeightRole.UNKNOWN;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "{}: All tasks migrated. Role changed back",
              schedulerCommon.getThisEndpoint().getAddress());
        }
        return true;
      }
      return false;
    }
    return true;
  }

  private void removeBalancingMarker() {
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{}: Deleting path {}", schedulerCommon.getThisEndpoint().getAddress(), balanceFqPath);
      }
      final var cmd = new PathCommand(DELETE, balanceFqPath);
      schedulerCommon.getTaskStore().executeSingle(cmd);
    } catch (PathExistsException e) {
      throw new RuntimeException(
          "Unexpected error while trying to remove balancing marker " + balanceFqPath);
    } catch (PathMissingException e) {
      LOGGER.debug(
          "{}: Path {} does not exist",
          schedulerCommon.getThisEndpoint().getAddress(),
          balanceFqPath);
    }
  }

  private int computeSelfWeight() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "{}: Tasks {}",
          schedulerCommon.getThisEndpoint().getAddress(),
          allTasks.values().stream()
              .map((ptw) -> ptw.weightInfo.getTaskName())
              .collect(Collectors.joining(",")));
    }
    return allTasks.values().stream().mapToInt(PerTaskWeightTracker::getCurrentWeight).sum();
  }

  private List<PerTaskWeightTracker> doReBalancing(List<PerNodeWeight> allWeights) {
    int minWeightIdx = -1;
    int maxWeightIdx = -1;
    int minWeight = selfWeight.getTotalWeight();
    int maxWeight = minWeight;

    int currentIdx = 0;
    while (currentIdx < allWeights.size()) {
      var currentWeight = allWeights.get(currentIdx);
      if (currentWeight.getTotalWeight() > maxWeight) {
        maxWeight = currentWeight.getTotalWeight();
        maxWeightIdx = currentIdx;
      }
      if (currentWeight.getTotalWeight() < minWeight) {
        minWeight = currentWeight.getTotalWeight();
        minWeightIdx = currentIdx;
      }
      currentIdx++;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Rebalancing on host {} with weight {} minId:maxId:min:max {}:{}:{}:{}",
          schedulerCommon.getThisEndpoint().getAddress(),
          selfWeight.getTotalWeight(),
          minWeightIdx,
          maxWeightIdx,
          minWeight,
          maxWeight);
    }
    if (maxWeightIdx < 0) {
      var diff = maxWeight - minWeight;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "diff:tolerance is {}:{} for host {}",
            diff,
            weightTolerance,
            schedulerCommon.getThisEndpoint().getAddress());
      }
      // we could be the L-node
      if (diff > weightTolerance) {
        // we should migrate tasks
        int totalWeightToMove = diff / 2;
        List<PerTaskWeightTracker> tasksToMove = new ArrayList<>();
        pickEligibleTasksToMove(totalWeightToMove, tasksToMove);
        if (!tasksToMove.isEmpty() && takeOverAsLNode()) {
          return tasksToMove;
        }
      }
    } else if (minWeightIdx < 0) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("{}: This node is S-Node", schedulerCommon.getThisEndpoint().getAddress());
      }
      currentRole = WeightRole.S_NODE;
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "{}: This node is neither S or L node", schedulerCommon.getThisEndpoint().getAddress());
      }
      currentRole = WeightRole.NEITHER;
    }
    return Collections.emptyList();
  }

  private void pickEligibleTasksToMove(
      int totalWeightToMove, List<PerTaskWeightTracker> tasksToMove) {
    final long youngLimit =
        System.currentTimeMillis() - (refresherThread.refreshingPeriodMillis * 2);
    List<PerTaskWeightTracker> trackers =
        allTasks.values().stream()
            .filter((x) -> x.lastMigratedInTime() < youngLimit)
            .sorted(Comparator.comparing(PerTaskWeightTracker::getCachedWeight).reversed())
            .collect(Collectors.toList());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "{}: Picking eligible tasks for migration from a total of {} tasks",
          schedulerCommon.getThisEndpoint().getAddress(),
          trackers.size());
    }
    if (trackers.size() <= 1) {
      // nothing available to move
      return;
    }
    int weightToMove = totalWeightToMove;
    for (var task : trackers) {
      final var toleranceFactor =
          (task.getCachedWeight() < 10) ? ((task.getCachedWeight() < 4) ? 150 : 125) : 110;
      if (task.getCachedWeight() <= ((weightToMove * toleranceFactor) / 100)) {
        tasksToMove.add(task);
        weightToMove -= task.getCachedWeight();
      }
      if (weightToMove <= 0) {
        break;
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "{}: Picked {} eligible tasks for migration from a total of {} tasks",
          schedulerCommon.getThisEndpoint().getAddress(),
          tasksToMove.size(),
          trackers.size());
    }
  }

  static final class PerTaskWeightTracker {
    private final PerTaskWeightInfo weightInfo;
    private final SchedulerEvents.PerTaskLoadEvents loadEventsSink;
    private volatile int cachedWeight;
    private final long migratedInTime;

    PerTaskWeightTracker(
        PerTaskWeightInfo weightInfo,
        boolean migratedIn,
        SchedulerEvents.PerTaskLoadEvents loadEvents) {
      this.loadEventsSink = loadEvents;
      this.weightInfo = weightInfo;
      this.cachedWeight = weightInfo.getSchedule().getWeightProvider().getAsInt();
      this.migratedInTime = (migratedIn) ? System.currentTimeMillis() : 0;
      if (migratedIn) {
        this.loadEventsSink.weightGained(cachedWeight);
      }
    }

    boolean notInRunSet() {
      return weightInfo.notInRunSet();
    }

    long lastMigratedInTime() {
      return migratedInTime;
    }

    int getCurrentWeight() {
      cachedWeight = weightInfo.getSchedule().getWeightProvider().getAsInt();
      return cachedWeight;
    }

    int getCachedWeight() {
      if (cachedWeight <= 0) {
        cachedWeight = weightInfo.getSchedule().getWeightProvider().getAsInt();
      }
      return cachedWeight;
    }

    public boolean migrateOut() {
      this.loadEventsSink.weightShed(cachedWeight);
      return weightInfo.migrateOut();
    }
  }

  private static final class PerNodeWeight {
    private final int totalWeight;
    private final boolean stale;

    private PerNodeWeight(byte[] storedData) {
      if (storedData != null && storedData.length >= 6) {
        var buf = ByteBuffer.wrap(storedData);
        totalWeight = buf.getInt();
        stale = buf.getShort() > 0;
      } else {
        totalWeight = 0;
        stale = true;
      }
    }

    private static byte[] toBytes(int totalWeight, boolean stale) {
      byte[] data = new byte[6];
      var buf = ByteBuffer.wrap(data);
      buf.putInt(totalWeight);
      buf.putShort((short) (stale ? 1 : 0));
      return data;
    }

    private int getTotalWeight() {
      return totalWeight;
    }
  }

  private final class SelfWeight {
    private final ReentrantLock flushLock;
    private String weightPath;
    private int totalWeight;
    private boolean stale;
    private long sessionId;
    private long flushVersion;
    private long writeVersion;

    private SelfWeight() {
      this.weightPath = null;
      this.stale = true;
      this.totalWeight = 0;
      this.writeVersion = 1L;
      this.flushVersion = 0L;
      this.sessionId = PerTaskInfo.INVALID_SESSION_ID;
      this.flushLock = new ReentrantLock();
    }

    private void setTotalWeight(int newWeight) {
      flushLock.lock();
      try {
        totalWeight = newWeight;
        stale = false;
        writeVersion++;
      } finally {
        flushLock.unlock();
      }
    }

    private void markStale() {
      flushLock.lock();
      try {
        this.stale = true;
        writeVersion++;
      } finally {
        flushLock.unlock();
      }
    }

    public void ownerLost() {
      flushLock.lock();
      try {
        this.weightPath = null;
        this.sessionId = PerTaskInfo.INVALID_SESSION_ID;
        writeVersion++;
      } finally {
        flushLock.unlock();
      }
    }

    private void flush() {
      boolean shouldFlush = false;
      boolean shouldCreate = false;
      byte[] data;
      String localWeightPath = "";
      flushLock.lock();
      try {
        if (weightPath == null || sessionId == PerTaskInfo.INVALID_SESSION_ID) {
          shouldCreate = true;
          shouldFlush = true;
        }
        if (flushVersion < writeVersion) {
          localWeightPath = weightPath;
          shouldFlush = true;
          flushVersion = writeVersion;
        }
        data = PerNodeWeight.toBytes(totalWeight, stale);
      } finally {
        flushLock.unlock();
      }
      if (shouldFlush) {
        if (shouldCreate) {
          var selfWeightPath = weightsFqPath + Path.SEPARATOR + WEIGHT_PREFIX;
          try {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "{}: Creating ephemeral sequential path {}",
                  schedulerCommon.getThisEndpoint().getAddress(),
                  selfWeightPath);
            }
            var cmd = new PathCommand(CREATE_EPHEMERAL_SEQUENTIAL, selfWeightPath, data);
            schedulerCommon.getTaskStore().executeSingle(cmd);
            Preconditions.checkState(
                cmd.getReturnValue() != null,
                "Ephemeral sequential node creation unexpectedly returned null " + selfWeightPath);
            var stats = schedulerCommon.getTaskStore().getStats(cmd.getReturnValue());
            Preconditions.checkState(
                stats != null,
                "Stats call to a created ephemeral z-node unexpectedly returned null for path "
                    + cmd.getReturnValue());
            flushLock.lock();
            try {
              this.weightPath = cmd.getReturnValue();
              this.sessionId = stats.getSessionId();
            } finally {
              flushLock.unlock();
            }
          } catch (PathExistsException | PathMissingException e) {
            throw new RuntimeException(
                "Unable to create new sequential path in " + selfWeightPath, e);
          }
        } else {
          try {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "{}: setting data in path {}",
                  schedulerCommon.getThisEndpoint().getAddress(),
                  localWeightPath);
            }
            final var cmd = new PathCommand(SET_DATA, localWeightPath, data);
            schedulerCommon.getTaskStore().executeSingle(cmd);
          } catch (PathExistsException | PathMissingException e) {
            throw new RuntimeException("Unable to set data in " + localWeightPath, e);
          }
        }
      }
    }

    private boolean isStale() {
      flushLock.lock();
      try {
        return stale;
      } finally {
        flushLock.unlock();
      }
    }

    public int getTotalWeight() {
      flushLock.lock();
      try {
        return totalWeight;
      } finally {
        flushLock.unlock();
      }
    }

    public boolean checkMatch(LinearizableHierarchicalStore.Stats stats, String readPath) {
      flushLock.lock();
      try {
        return stats.getSessionId() == sessionId && readPath.equals(weightPath);
      } finally {
        flushLock.unlock();
      }
    }
  }

  /**
   * Thread that monitors the weights of all tasks of all nodes and chooses an L-node. Runs on all
   * service instances.
   *
   * <p>z-node watcher wakes up this thread whenever there is a change in the weight z-nodes.
   */
  final class WeightBalancingThread extends Thread implements AutoCloseable {
    private final long balancingPeriodMillis;
    private final ReentrantLock weightLock;
    private final Condition weightCondition;
    private boolean isClosing;
    private long nextBalancingTimeMillis;

    public WeightBalancingThread(int weightBasedBalancingPeriodSecs) {
      super("WeightTrackerThread");
      this.balancingPeriodMillis = TimeUnit.SECONDS.toMillis(weightBasedBalancingPeriodSecs);
      this.nextBalancingTimeMillis = System.currentTimeMillis() + FIRST_TIME_PERIODICITY_MILLIS;
      this.weightLock = new ReentrantLock();
      this.weightCondition = this.weightLock.newCondition();
      this.isClosing = false;
    }

    @Override
    public void run() {
      while (!isClosing) {
        try {
          var millisToSleep = nextBalancingTimeMillis - System.currentTimeMillis();
          boolean adjustTime = true;
          while (millisToSleep > 0) {
            weightLock.lock();
            try {
              long beforeMillis = System.currentTimeMillis();
              var signalled = weightCondition.await(millisToSleep, TimeUnit.MILLISECONDS);
              if (isClosing) {
                break;
              }
              if (signalled) {
                adjustTime = false;
              }
              long afterMillis = System.currentTimeMillis();
              millisToSleep -= (afterMillis - beforeMillis);
              millisToSleep = Math.min(millisToSleep, nextBalancingTimeMillis - afterMillis);
            } finally {
              weightLock.unlock();
            }
          }
          if (adjustTime) {
            weightLock.lock();
            try {
              nextBalancingTimeMillis =
                  System.currentTimeMillis()
                      + ((currentRole.equals(WeightRole.L_NODE))
                          ? L_NODE_CHECK_PERIODICITY_MILLIS
                          : balancingPeriodMillis);
            } finally {
              weightLock.unlock();
            }
          }
          if (selfWeight.isStale()) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "{}: Balancing postponed as self data is stale {}",
                  schedulerCommon.getThisEndpoint().getAddress(),
                  selfWeight);
            }
            narrowNextBalancingTime();
            continue;
          }
          if (!checkIfAllTasksMigrated()) {
            narrowNextBalancingTime();
            continue;
          }
          List<PerNodeWeight> allWeights = new ArrayList<>();
          var selfFound = readAllWeights(allWeights);
          if (!selfFound) {
            LOGGER.info(
                "{}: Entry for self not found: {}",
                schedulerCommon.getThisEndpoint().getAddress(),
                selfWeight);
            selfWeight.ownerLost();
            selfWeight.flush();
            narrowNextBalancingTime();
            continue;
          }
          if (allWeights.isEmpty()) {
            // nothing to balance
            continue;
          }
          var staleSz =
              allWeights.stream().filter((w) -> w.stale).collect(Collectors.toSet()).size();
          if (staleSz > 0) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "{}: Balancing postponed as other node data is stale {}",
                  schedulerCommon.getThisEndpoint().getAddress(),
                  staleSz);
            }
            narrowNextBalancingTime();
            continue;
          }
          List<PerTaskWeightTracker> tasksToRebalance = doReBalancing(allWeights);
          for (var task : tasksToRebalance) {
            if (task.migrateOut()) {
              LOGGER.info(
                  "Task {} is migrating out from {}",
                  task.weightInfo.getTaskName(),
                  schedulerCommon.getThisEndpoint().getAddress());
              migratingOutTasks.add(task);
            }
          }
        } catch (InterruptedException e) {
          // set the interrupt flag back again and exit thread
          isClosing = true;
          Thread.currentThread().interrupt();
        } catch (Throwable e) {
          LOGGER.warn("Unexpected exception during weight tracking. Skipping a cycle", e);
          events.hitUnexpectedError();
        }
      }
    }

    private void narrowNextBalancingTime() {
      weightLock.lock();
      try {
        nextBalancingTimeMillis =
            Math.min(
                nextBalancingTimeMillis,
                System.currentTimeMillis() + FIRST_TIME_PERIODICITY_MILLIS);
      } finally {
        weightLock.unlock();
      }
    }

    @Override
    public void close() {
      weightLock.lock();
      try {
        isClosing = true;
        weightCondition.signal();
      } finally {
        weightLock.unlock();
      }
    }

    public void recompute() {
      weightLock.lock();
      try {
        final var currentMillis = System.currentTimeMillis();
        final var nextMillis = currentMillis + L_NODE_CHECK_PERIODICITY_MILLIS;
        nextBalancingTimeMillis =
            (nextBalancingTimeMillis < currentMillis)
                ? nextMillis
                : Math.min(nextBalancingTimeMillis, nextMillis);
        weightCondition.signal();
      } finally {
        weightLock.unlock();
      }
    }
  }

  /** Refreshes self weight periodically or on signal that tasks have been added or removed. */
  final class WeightRefresherThread extends Thread implements AutoCloseable {
    private final long refreshingPeriodMillis;
    private final ReentrantLock refresherLock;
    private final Condition refresherCondition;
    private long nextRefreshTimeMillis;
    private boolean isClosing;

    public WeightRefresherThread(int refreshingPeriodSecs) {
      super("WeightRefresherThread");
      this.refreshingPeriodMillis = TimeUnit.SECONDS.toMillis(refreshingPeriodSecs);
      this.nextRefreshTimeMillis = System.currentTimeMillis() + REFRESH_PERIODICITY_MILLIS;
      this.isClosing = false;
      this.refresherLock = new ReentrantLock();
      this.refresherCondition = this.refresherLock.newCondition();
    }

    @Override
    public void run() {
      while (!isClosing) {
        try {
          var millisToSleep = nextRefreshTimeMillis - System.currentTimeMillis();
          boolean adjustTime = true;
          while (millisToSleep > 0) {
            refresherLock.lock();
            try {
              long beforeMillis = System.currentTimeMillis();
              boolean signalled = refresherCondition.await(millisToSleep, TimeUnit.MILLISECONDS);
              if (isClosing) {
                break;
              }
              if (signalled) {
                adjustTime = false;
              }
              millisToSleep -= (System.currentTimeMillis() - beforeMillis);
            } finally {
              refresherLock.unlock();
              if (!adjustTime) {
                // flush before going to sleep
                selfWeight.flush();
              }
            }
          }
          if (adjustTime) {
            refresherLock.lock();
            try {
              nextRefreshTimeMillis = System.currentTimeMillis() + refreshingPeriodMillis;
            } finally {
              refresherLock.unlock();
            }
          }
          final var allWeight = computeSelfWeight();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "{}: Self weight is now {}",
                schedulerCommon.getThisEndpoint().getAddress(),
                allWeight);
          }
          events.computedWeight(allWeight);
          selfWeight.setTotalWeight(allWeight);
          selfWeight.flush();
        } catch (InterruptedException e) {
          // set the interrupt flag back again and exit thread
          isClosing = true;
          Thread.currentThread().interrupt();
        } catch (Throwable e) {
          LOGGER.warn("Unexpected exception during refreshing weights. Skipping a cycle", e);
          events.hitUnexpectedError();
        }
      }
    }

    @Override
    public void close() {
      refresherLock.lock();
      try {
        isClosing = true;
        refresherCondition.signal();
      } finally {
        refresherLock.unlock();
      }
    }

    public void localWeightChanged() {
      selfWeight.markStale();
      refresherLock.lock();
      try {
        final var currentMillis = System.currentTimeMillis();
        final var nextMillis = currentMillis + REFRESH_PERIODICITY_MILLIS;
        nextRefreshTimeMillis =
            (nextRefreshTimeMillis < currentMillis)
                ? nextMillis
                : Math.min(nextRefreshTimeMillis, nextMillis);
        refresherCondition.signal();
      } finally {
        refresherLock.unlock();
      }
    }
  }

  static final class DummyWeightTracker implements WeightBalancer {
    @Override
    public void start() {}

    @Override
    public void close() throws Exception {}

    @Override
    public void addTask(
        PerTaskWeightInfo weightInfo,
        boolean migratedIn,
        SchedulerEvents.PerTaskLoadEvents loadEventsSink) {}

    @Override
    public void removeTask(String taskName) {}

    @Override
    public boolean isEligibleForInMigrations(long millisInRunSet) {
      return true;
    }
  }
}
