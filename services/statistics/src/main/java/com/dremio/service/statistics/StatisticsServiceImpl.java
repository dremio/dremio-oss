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
package com.dremio.service.statistics;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.store.sys.statistics.StatisticsListManager;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobSubmittedListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.statistics.proto.StatisticId;
import com.dremio.service.statistics.store.StatisticEntriesStore;
import com.dremio.service.statistics.store.StatisticStore;
import com.dremio.service.users.SystemUser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Statistics service
 */
public class StatisticsServiceImpl implements StatisticsService {


  private static final String TABLE_COLUMN_NAME = "TABLE_PATH";
  public static final String ROW_COUNT_IDENTIFIER = "null";
  private final Provider<JobsService> jobsService;
  private final Provider<SchedulerService> schedulerService;
  private final StatisticStore statisticStore;
  private final StatisticEntriesStore statisticEntriesStore;
  private final Provider<BufferAllocator> allocator;
  private final Provider<NamespaceService> namespaceService;
  private final Map<String, JobId> entries;

  public StatisticsServiceImpl(
    Provider<LegacyKVStoreProvider> storeProvider,
    Provider<SchedulerService> schedulerService,
    Provider<JobsService> jobsService,
    Provider<NamespaceService> namespaceService,
    Provider<BufferAllocator> allocator
  ) {
    this.schedulerService = Preconditions.checkNotNull(schedulerService, "scheduler service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service required");
    this.namespaceService = Preconditions.checkNotNull(namespaceService, "namespace service required");
    this.allocator = Preconditions.checkNotNull(allocator, "buffer allocator required");
    this.statisticStore = new StatisticStore(storeProvider);
    this.statisticEntriesStore = new StatisticEntriesStore(storeProvider);
    this.entries = new HashMap<>();
  }

  public void validateDataset(NamespaceKey key) {
    try {
      final DatasetConfig dataset = namespaceService.get().getDataset(key);
      if(dataset == null) {
        throw UserException.validationError().message("Unable to find requested dataset %s.", key).build(logger);
      } else if (!DatasetHelper.isPhysicalDataset(dataset.getType())) {
        throw UserException.validationError().message("\"%s\" is not a physical dataset", key).build(logger);
      }
    } catch(Exception e) {
      throw UserException.validationError(e).message("Unable to find requested dataset %s.", key).build(logger);
    }
  }

  @Override
  public Iterable<StatisticsListManager.StatisticsInfo> getStatisticsInfos() {
    final Map<String, Long> rowCountMap = new HashMap<>();
    return StreamSupport.stream(statisticStore.getAll().spliterator(), false)
      .map(e -> {
        Timestamp createdAt = new Timestamp(e.getValue().getCreatedAt());
        String column = e.getKey().getColumn();
        String table = e.getKey().getTablePath();
        Long ndv = e.getValue().getNdv();
        Long rowCount;
        if (rowCountMap.containsKey(table)) {
          rowCount = rowCountMap.get(table);
        } else {
          rowCount = getRowCount(table);
          rowCountMap.put(table, rowCount);
        }
        Long colRowCount = e.getValue().getColumnRowCount();
        Long nullCount = rowCount != null && colRowCount != null ? rowCount - colRowCount : null;
        String quantiles = null;

        if (e.getValue().getSerializedTdigest() != null) {
          Histogram histogram = new HistogramImpl(e.getValue().getSerializedTdigest().asReadOnlyByteBuffer());
          String quantilesBuilder = "[" +
            String.format("0: %.4f ,", histogram.quantile(0)) +
            String.format("0.25: %.4f ,", histogram.quantile(0.25)) +
            String.format("0.5: %.4f ,", histogram.quantile(0.5)) +
            String.format("0.75: %.4f ,", histogram.quantile(0.75)) +
            String.format("1: %.4f ", histogram.quantile(1)) +
            "]";
          quantiles = quantilesBuilder;
        }

        return new StatisticsListManager.StatisticsInfo(table, column, createdAt, ndv, rowCount, nullCount, quantiles);
      }).filter(StatisticsListManager.StatisticsInfo::isValid).collect(Collectors.toList());
  }

  @Override
  public List<String> deleteStatistics(List<String> columns, NamespaceKey key) {
    validateDataset(key);
    List<String> fail = new ArrayList<>();
    if (entries.containsKey(key.toString().toLowerCase())) {
      throw new ConcurrentModificationException(String.format("Cannot delete statistics for dataset, %s, as compute statistics job is currently running", key));
    }

    for (String column : columns) {
      String normalizedColumn = column.toLowerCase();
      if (!deleteStatistic(normalizedColumn, key)) {
        fail.add(normalizedColumn);
      }
    }
    return fail;
  }

  @Override
  public boolean deleteRowCountStatistics(NamespaceKey key) {
    return deleteStatistic(ROW_COUNT_IDENTIFIER, key);
  }

  @VisibleForTesting
  @Override
  public boolean deleteStatistic(String column, NamespaceKey key) {
    validateDataset(key);
    if (entries.containsKey(key.toString())) {
      throw new ConcurrentModificationException(String.format("Cannot delete statistics for dataset, %s, as compute statistics job is currently running", key));
    }
    final StatisticId statisticId = new StatisticId().setColumn(column).setTablePath(key.toString().toLowerCase());
    if (statisticStore.get(statisticId) == null) {
      return false;
    } else {
      statisticStore.delete(new StatisticId().setColumn(column).setTablePath(key.toString().toLowerCase()));
    }
    return true;
  }

  @Override
  public Long getNDV(String column, NamespaceKey key) {
    String normalizedColumn = column.toLowerCase();
    validateDataset(key);
    StatisticId statisticId = new StatisticId().setTablePath(key.toString().toLowerCase()).setColumn(normalizedColumn);
    Statistic statistic = statisticStore.get(statisticId);
    if (statistic == null) {
      return null;
    }
    return statistic.getNdv();
  }

  @Override
  public Long getRowCount(NamespaceKey key) {
    validateDataset(key);
    return getRowCount(key.toString().toLowerCase());
  }

  private Long getRowCount(String key) {
    StatisticId statisticId = new StatisticId().setTablePath(key).setColumn(ROW_COUNT_IDENTIFIER);
    Statistic statistic = statisticStore.get(statisticId);
    if (statistic == null) {
      return null;
    }
    return statistic.getRowCount();
  }

  @Override
  public Long getNullCount(String column, NamespaceKey key) {
    String normalizedColumn = column.toLowerCase();
    validateDataset(key);
    StatisticId statisticId = new StatisticId().setTablePath(key.toString()).setColumn(normalizedColumn);
    Statistic statistic = statisticStore.get(statisticId);
    Statistic rowCountStatistic = statisticStore.get(new StatisticId().setTablePath(key.toString()).setColumn(ROW_COUNT_IDENTIFIER));
    if (statistic == null || rowCountStatistic == null) {
      return null;
    }
    return rowCountStatistic.getRowCount() - statistic.getColumnRowCount();
  }

  @Override
  public Histogram getHistogram(String column, NamespaceKey key) {
    String normalizedColumn = column.toLowerCase();
    validateDataset(key);
    StatisticId statisticId = new StatisticId().setTablePath(key.toString()).setColumn(normalizedColumn);
    Statistic statistic = statisticStore.get(statisticId);
    if (statistic == null) {
      return null;
    }
    return statistic.getHistogram();
  }

  @Override
  public String requestStatistics(List<String> columns, NamespaceKey key) {
    validateDataset(key);
    final JobSubmittedListener listener = new JobSubmittedListener();
    final JobId jobId = jobsService.get().submitJob(SubmitJobRequest.newBuilder()
      .setQueryType(QueryType.UI_INTERNAL_RUN)
      .setSqlQuery(com.dremio.service.job.SqlQuery.newBuilder()
        .setSql(getSql(columns, key.toString()))
        .setUsername(SystemUser.SYSTEM_USERNAME)).build(), listener);
    statisticEntriesStore.save(key.toString().toLowerCase(), jobId);
    entries.put(key.toString().toLowerCase(), jobId);
    return jobId.getId();
  }

  @Override
  public void setNdv(String column, Long val, NamespaceKey key) {
    String normalizedColumn = column.toLowerCase();
    validateDataset(key);
    updateStatistic(key.toString(), normalizedColumn, Statistic.StatisticType.NDV, val);
  }

  @Override
  public void setRowCount(Long val, NamespaceKey key) {
    validateDataset(key);
    updateStatistic(key.toString(), ROW_COUNT_IDENTIFIER, Statistic.StatisticType.RCOUNT, val);
  }


  private String getColumnName(Statistic.StatisticType type, String name) {
    return type + "_" + name;
  }

  public String getSql(List<String> columns, String table) {
    StringBuilder stringBuilder = new StringBuilder("SELECT '");
    stringBuilder.append(table).append("' as ").append(TABLE_COLUMN_NAME).append(", ");
    populateNdvSql(stringBuilder, columns);
    if (columns.size() > 0) {
      stringBuilder.append(", ");
    }
    populateCountStarSql(stringBuilder);
    stringBuilder.append(", ");
    populateCountColumnSql(stringBuilder, columns);
    stringBuilder.append(", ");
    populateTDigestSql(stringBuilder, columns);
    stringBuilder.append("FROM ").append(table);
    return stringBuilder.toString();
  }

  private void populateNdvSql(StringBuilder stringBuilder, List<String> columns) {
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      stringBuilder.append(String.format("ndv(%s) as %s", String.format("\"%s\"", column), getColumnName(Statistic.StatisticType.NDV, column)));
      if (i == columns.size() - 1) {
        stringBuilder.append(" ");
      } else {
        stringBuilder.append(", ");
      }
    }
  }

  private void populateCountStarSql(StringBuilder stringBuilder) {
    stringBuilder.append(String.format("count(*) as %s ", getColumnName(Statistic.StatisticType.RCOUNT, ROW_COUNT_IDENTIFIER)));
  }

  private void populateCountColumnSql(StringBuilder stringBuilder, List<String> columns) {
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      stringBuilder.append(String.format("count(%s) as %s", String.format("\"%s\"", column), getColumnName(Statistic.StatisticType.COLRCOUNT, column)));
      if (i == columns.size() - 1) {
        stringBuilder.append(" ");
      } else {
        stringBuilder.append(", ");
      }
    }
  }

  private void populateTDigestSql(StringBuilder stringBuilder, List<String> columns) {
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      stringBuilder.append(String.format("tdigest(%s) as %s", String.format("\"%s\"", column), getColumnName(Statistic.StatisticType.TDIGEST, column)));
      if (i == columns.size() - 1) {
        stringBuilder.append(" ");
      } else {
        stringBuilder.append(", ");
      }
    }
  }

  @Override
  public void start() throws Exception {
    for (Map.Entry<String, JobId> entry : statisticEntriesStore.getAll()) {
      entries.put(entry.getKey(), entry.getValue());
    }
    schedulerService.get().schedule(Schedule.Builder.everySeconds(10).build(), new StatisticsUpdater());
  }

  private void updateStatistic(String table, String column, Statistic.StatisticType type, Object value) {
    String normalizedTable = table.toLowerCase();
    String normalizedColumn = column.toLowerCase();
    StatisticId statisticId = new StatisticId().setColumn(normalizedColumn).setTablePath(normalizedTable);
    Statistic statistic = (statisticStore.get(statisticId) != null) ? statisticStore.get(statisticId) : new Statistic();
    Statistic.StatisticBuilder statisticBuilder = new Statistic.StatisticBuilder(statistic);
    statisticBuilder.update(type, value);
    statisticStore.save(statisticId, statisticBuilder.build());
  }

  /**
   * StatisticsUpdater
   */
  private class StatisticsUpdater implements Runnable {
    public void run() {
      try {
        if (entries.entrySet().size() == 0) {
          return;
        }
        Set<String> tablesToRemove = new HashSet<>();
        for(Map.Entry<String, JobId> entry : entries.entrySet()) {
          try {
            final JobId id = entry.getValue();
            JobProtobuf.JobId.Builder builder = JobProtobuf.JobId.newBuilder();
            builder.setId(id.getId());
            if (id.getName() != null) {
              builder.setName(id.getName());
            }
            JobDetails jobDetails = jobsService.get().getJobDetails(JobDetailsRequest.newBuilder().setJobId(builder.build()).setUserName(SystemUser.SYSTEM_USERNAME).build());
            List<JobProtobuf.JobAttempt> attempts = jobDetails.getAttemptsList();
            JobProtobuf.JobAttempt lastAttempt = attempts.get(attempts.size() - 1);
            switch (lastAttempt.getState()) {
              case COMPLETED:
                try (final JobDataFragment data = JobDataClientUtils.getJobData(jobsService.get(), allocator.get(), id, 0, 1)) {
                  List<Field> fields = data.getSchema().getFields();
                  Preconditions.checkArgument(fields.get(0).getName().equals(TABLE_COLUMN_NAME));
                  String table = data.extractValue(fields.get(0).getName(), 0).toString();
                  StatisticsInputBuilder statisticsInputBuilder = new StatisticsInputBuilder(table.toLowerCase());
                  for (int i = 1; i < fields.size(); i++) {
                    String name = fields.get(i).getName();
                    Object value = data.extractValue(name, 0);
                    String[] names = name.split("_", 2);
                    Statistic.StatisticType type = Statistic.StatisticType.valueOf(names[0]);
                    String columnName = names[1];
                    columnName = columnName.toLowerCase();
                    statisticsInputBuilder.updateStatistic(columnName, type, value);
                  }
                  Map<StatisticId, Statistic> statisticIdStatisticHashMap = statisticsInputBuilder.build();
                  statisticIdStatisticHashMap.forEach(statisticStore::save);
                }
              case CANCELED:
              case FAILED:
              case CANCELLATION_REQUESTED:
                tablesToRemove.add(entry.getKey());
              default:
                break;
            }
          } catch (Exception ex) {
            logger.warn("Failed to handle statistics entry, {}", entry, ex);
            tablesToRemove.add(entry.getKey());
          }
        }
        tablesToRemove.forEach(key -> {
          entries.remove(key.toLowerCase());
          statisticEntriesStore.delete(key.toLowerCase());
        });
      } catch (Exception ex) {
        logger.warn("Failure while attempting to update statistics.", ex);
      }
    }
  }

  @Override
  public void close() throws Exception {
  }

  /**
   * Statistics Input Builder
   */
  public class StatisticsInputBuilder {
    private final String table;
    private final Map<StatisticId, Statistic.StatisticBuilder> builderMap;

    public StatisticsInputBuilder(String table) {
      this.builderMap = new HashMap<>();
      this.table = table;
    }

    public void updateStatistic(String column, Statistic.StatisticType type, Object value) {
      StatisticId statisticId = new StatisticId().setColumn(column).setTablePath(table);
      if (!builderMap.containsKey(statisticId)) {
        builderMap.put(statisticId, new Statistic.StatisticBuilder());
      }
      builderMap.get(statisticId).update(type, value);
    }

    public Map<StatisticId, Statistic> build() {
      Map<StatisticId, Statistic> statisticIdStatisticHashMap = new HashMap<>();
      builderMap.forEach((k, v) -> {
        statisticIdStatisticHashMap.put(k, v.build());
      });
      return statisticIdStatisticHashMap;
    }

  }


}
