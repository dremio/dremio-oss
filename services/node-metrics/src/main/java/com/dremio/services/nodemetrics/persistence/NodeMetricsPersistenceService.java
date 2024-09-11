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
package com.dremio.services.nodemetrics.persistence;

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.plugins.nodeshistory.NodesHistoryStoreConfig;
import com.dremio.service.Service;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.nodemetrics.NodeMetricsService;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import java.time.Instant;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NodeMetricsPersistenceService runs the following scheduled tasks in order to enable the efficient
 * storage and retrieval of system metrics related to the running coordinators and executors: <br>
 * 1) Collecting the node metrics and writing them to the specified filesystem and storage path
 * given by the NodesHistoryStoreConfig, in the form of CSV files <br>
 * 2) Compacting these text files on a specified interval <br>
 * 3) Deleting these text files after a specified retention period
 */
@Options
public class NodeMetricsPersistenceService implements Service {
  private static final Logger logger = LoggerFactory.getLogger(NodeMetricsPersistenceService.class);

  /**
   * How often node metrics are persisted; frequency determines granularity of the system table
   * records
   */
  public static final TypeValidators.PositiveLongValidator NODE_HISTORY_WRITE_FREQUENCY_MINS =
      new TypeValidators.AdminPositiveLongValidator(
          "store.node_history.write.frequency.mins", Long.MAX_VALUE, 1);

  /**
   * How often node metrics files are compacted to improve query performance; all uncompacted files
   * are compacted into one file when this runs
   */
  public static final TypeValidators.PositiveLongValidator NODE_HISTORY_COMPACTION_FREQUENCY_MINS =
      new TypeValidators.AdminPositiveLongValidator(
          "store.node_history.compaction.frequency.mins", Long.MAX_VALUE, 60);

  /**
   * How often node metrics compacted files are recompacted to improve query performance; all singly
   * compacted files are compacted into one file when this runs
   */
  public static final TypeValidators.PositiveLongValidator
      NODE_HISTORY_RECOMPACTION_FREQUENCY_HOURS =
          new TypeValidators.AdminPositiveLongValidator(
              "store.node_history.recompaction.frequency.hours", Long.MAX_VALUE, 24);

  /**
   * Max age of node metrics files before they are deleted. Historical data will be retained for at
   * least this long. Min = 1 month, Max = 3 months
   */
  public static final TypeValidators.PositiveLongValidator NODE_HISTORY_RETENTION_DAYS =
      new TypeValidators.AdminPositiveLongValidator("store.node_history.retention.days", 92, 31);

  private static final String LOCAL_TASK_LEADER_NAME = "NodeMetricsPersistence";

  /** Whether metrics should only be captured for executors */
  private final boolean executorsOnly;

  private final Provider<NodeMetricsService> nodeMetricsServiceProvider;
  private final Provider<SchedulerService> schedulerServiceProvider;
  private final Provider<OptionManager> optionManager;
  private final NodeMetricsCsvFormatter csvFormatter;
  private final NodeMetricsStorage nodeMetricsStorage;
  private final Object syncObject;

  @Inject
  public NodeMetricsPersistenceService(
      boolean executorsOnly,
      Provider<NodeMetricsService> nodeMetricsServiceProvider,
      Provider<SchedulerService> schedulerServiceProvider,
      Provider<OptionManager> optionManagerProvider,
      Provider<NodesHistoryStoreConfig> nodesHistoryStoreConfigProvider) {
    this(
        executorsOnly,
        nodeMetricsServiceProvider,
        schedulerServiceProvider,
        optionManagerProvider,
        new NodeMetricsCsvFormatter(),
        new NodeMetricsStorage(nodesHistoryStoreConfigProvider));
  }

  @VisibleForTesting
  NodeMetricsPersistenceService(
      boolean executorsOnly,
      Provider<NodeMetricsService> nodeMetricsServiceProvider,
      Provider<SchedulerService> schedulerServiceProvider,
      Provider<OptionManager> optionManagerProvider,
      NodeMetricsCsvFormatter csvFormatter,
      NodeMetricsStorage nodeMetricsStorage) {
    this.executorsOnly = executorsOnly;
    this.nodeMetricsServiceProvider = nodeMetricsServiceProvider;
    this.schedulerServiceProvider = schedulerServiceProvider;
    this.optionManager = optionManagerProvider;
    this.csvFormatter = csvFormatter;
    this.nodeMetricsStorage = nodeMetricsStorage;
    this.syncObject = new Object();
  }

  @Override
  public void start() {
    if (optionManager.get().getOption(ExecConstants.NODE_HISTORY_ENABLED)) {
      logger.info("Starting node metrics scheduled tasks");

      NodeMetricsWriter writer =
          new NodeMetricsWriter(
              () -> optionManager.get().getOption(ExecConstants.NODE_HISTORY_ENABLED),
              this::getSchedule,
              optionManager.get().getOption(NODE_HISTORY_WRITE_FREQUENCY_MINS),
              executorsOnly,
              csvFormatter,
              nodeMetricsStorage,
              nodeMetricsServiceProvider.get(),
              schedulerServiceProvider.get(),
              syncObject);
      writer.start();
      logger.info("Started node metrics writer scheduled task");

      NodeMetricsCompacter compacter =
          new NodeMetricsCompacter(
              () -> optionManager.get().getOption(ExecConstants.NODE_HISTORY_ENABLED),
              this::getSchedule,
              optionManager.get().getOption(NODE_HISTORY_COMPACTION_FREQUENCY_MINS),
              CompactionType.Single,
              nodeMetricsStorage,
              schedulerServiceProvider.get(),
              syncObject);
      compacter.start();
      logger.info("Started node metrics writer compacter task");

      NodeMetricsCompacter recompacter =
          new NodeMetricsCompacter(
              () -> optionManager.get().getOption(ExecConstants.NODE_HISTORY_ENABLED),
              this::getSchedule,
              optionManager.get().getOption(NODE_HISTORY_RECOMPACTION_FREQUENCY_HOURS) * 60,
              CompactionType.Double,
              nodeMetricsStorage,
              schedulerServiceProvider.get(),
              syncObject);
      recompacter.start();
      logger.info("Started node metrics recompacter scheduled task");

      NodeMetricsCleaner cleaner =
          new NodeMetricsCleaner(
              () -> optionManager.get().getOption(ExecConstants.NODE_HISTORY_ENABLED),
              this::getSchedule,
              optionManager.get().getOption(NODE_HISTORY_RETENTION_DAYS),
              nodeMetricsStorage,
              schedulerServiceProvider.get(),
              syncObject);
      cleaner.start();
      logger.info("Started node metrics cleaner scheduled task");
    }
  }

  @Override
  public void close() throws Exception {}

  private Schedule getSchedule(Instant scheduleTime) {
    // Use single shot schedule and re-schedule after run to handle the possibility of the schedule
    // frequency option value having changed since startup.
    // This uses a clustered singleton to ensure that only a single coordinator is performing these
    // tasks.
    return Schedule.Builder.singleShotChain()
        .startingAt(scheduleTime)
        .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
        .build();
  }
}
