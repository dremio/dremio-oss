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

import static com.dremio.service.statistics.StatisticsUtil.createRowCountStatisticId;
import static com.dremio.service.statistics.StatisticsUtil.createStatisticId;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.utils.PathUtils;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.sys.statistics.StatisticsListManager;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.options.OptionManager;
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
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.sql.type.SqlTypeName;

/** Statistics service */
public class StatisticsServiceImpl implements StatisticsService {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(StatisticsServiceImpl.class);

  private static final String TABLE_COLUMN_NAME = "TABLE_PATH";
  private static final long HEAVY_HITTERS_THRESHOLD = 3;
  public static final String ROW_COUNT_IDENTIFIER = "null";
  public static final String SAMPLE_COL_NAME = "SAMPLE";
  public static final String NON_SAMPLE_COL_PREFIX = "ORIGINAL";
  private final Provider<JobsService> jobsService;
  private final Provider<SchedulerService> schedulerService;
  private final Provider<BufferAllocator> allocator;
  private final Provider<NamespaceService> namespaceService;
  private final Provider<LegacyKVStoreProvider> storeProvider;
  private final Provider<SabotContext> sabotContext;
  private StatisticStore statisticStore;
  private StatisticEntriesStore statisticEntriesStore;
  private Map<String, JobId> entries;

  public StatisticsServiceImpl(
      Provider<LegacyKVStoreProvider> storeProvider,
      Provider<SchedulerService> schedulerService,
      Provider<JobsService> jobsService,
      Provider<NamespaceService> namespaceService,
      Provider<BufferAllocator> allocator,
      Provider<SabotContext> sabotContext) {
    this.schedulerService =
        Preconditions.checkNotNull(schedulerService, "scheduler service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service required");
    this.namespaceService =
        Preconditions.checkNotNull(namespaceService, "namespace service required");
    this.allocator = Preconditions.checkNotNull(allocator, "buffer allocator required");
    this.storeProvider = Preconditions.checkNotNull(storeProvider, "store provider required");
    this.sabotContext = Preconditions.checkNotNull(sabotContext, "sabot context required");
  }

  public void validateDataset(NamespaceKey key) {
    try {
      final DatasetConfig dataset = namespaceService.get().getDataset(key);
      if (dataset == null) {
        throw UserException.validationError()
            .message("Unable to find requested dataset %s.", key)
            .build(logger);
      } else if (!DatasetHelper.isPhysicalDataset(dataset.getType())) {
        throw UserException.validationError()
            .message("\"%s\" is not a physical dataset", key)
            .build(logger);
      }
    } catch (Exception e) {
      throw UserException.validationError(e)
          .message("Unable to find requested dataset %s.", key)
          .build(logger);
    }
  }

  private SqlTypeName getSqlTypeNameFromColumn(String column, BatchSchema schema)
      throws IllegalArgumentException {
    Optional<Field> field = schema.findFieldIgnoreCase(column);
    if (!field.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to find field, %s, from schema, %s.", field.toString(), schema.toString()));
    }

    CompleteType completeType = new CompleteType(field.get().getType(), new ArrayList<>());
    return TypeInferenceUtils.getCalciteTypeFromMinorType(completeType.toMinorType());
  }

  @Override
  public Iterable<StatisticsListManager.StatisticsInfo> getStatisticsInfos() {
    final Map<String, Long> rowCountMap = new HashMap<>();
    return StreamSupport.stream(statisticStore.getAll().spliterator(), false)
        .map(
            e -> {
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
              Long nullCount =
                  rowCount != null && colRowCount != null ? rowCount - colRowCount : null;
              String quantiles = null;
              String heavyHitters = null;
              if (e.getValue().getSerializedItemsSketch() != null) {
                try {
                  List<String> pathComponents = PathUtils.parseFullPath(table);
                  BatchSchema actualSchema =
                      BatchSchema.deserialize(
                          namespaceService
                              .get()
                              .getDataset(new NamespaceKey(pathComponents))
                              .getRecordSchema()
                              .toByteArray());
                  Histogram histogram =
                      new HistogramImpl(
                          null,
                          e.getValue()
                              .getSerializedItemsSketch()
                              .asReadOnlyByteBuffer()
                              .order(ByteOrder.nativeOrder()),
                          getSqlTypeNameFromColumn(column, actualSchema));
                  Set<Object> freq = histogram.getFrequentItems(HEAVY_HITTERS_THRESHOLD);
                  heavyHitters = freq.toString();
                } catch (Exception ex) {
                  logger.warn(
                      "ItemsSketch could not be retrieved from the store for table: {} , column: {}",
                      table,
                      column,
                      ex);
                  heavyHitters =
                      String.format(
                          "ItemsSketch could not be retrieved from the store for table: %s , column: %s: %s",
                          table, column, ex.toString());
                }
              }

              if (e.getValue().getSerializedTdigest() != null) {
                try {
                  Histogram histogram =
                      new HistogramImpl(
                          e.getValue().getSerializedTdigest().asReadOnlyByteBuffer(), null, null);
                  quantiles =
                      "["
                          + String.format("0: %.4f ,", histogram.quantile(0))
                          + String.format("0.25: %.4f ,", histogram.quantile(0.25))
                          + String.format("0.5: %.4f ,", histogram.quantile(0.5))
                          + String.format("0.75: %.4f ,", histogram.quantile(0.75))
                          + String.format("1: %.4f ", histogram.quantile(1))
                          + "]";
                } catch (Exception ex) {
                  logger.warn(
                      "T-Digest could not be retrieved from the store for table: {} , column: {}",
                      table,
                      column,
                      ex);
                  quantiles =
                      String.format(
                          "T-Digest could not be retrieved from the store for table: %s , column: %s: %s",
                          table, column, ex.toString());
                }
              }

              return new StatisticsListManager.StatisticsInfo(
                  table, column, createdAt, ndv, rowCount, nullCount, quantiles, heavyHitters);
            })
        .filter(StatisticsListManager.StatisticsInfo::isValid)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> deleteStatistics(List<String> fields, NamespaceKey key) {
    validateDataset(key);
    List<String> fail = new ArrayList<>();
    if (entries.containsKey(key.toString().toLowerCase())) {
      throw new ConcurrentModificationException(
          String.format(
              "Cannot delete statistics for dataset, %s, as compute statistics job is currently running",
              key));
    }

    for (String column : fields) {
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
    if (entries.containsKey(key.toString().toLowerCase())) {
      throw new ConcurrentModificationException(
          String.format(
              "Cannot delete statistics for dataset, %s, as compute statistics job is currently running",
              key));
    }
    final StatisticId statisticId = createStatisticId(column, key);
    if (statisticStore.get(statisticId) == null) {
      logger.trace(String.format("Statistic Not Found for column %s and dataset %s", column, key));
      return false;
    } else {
      statisticStore.delete(createStatisticId(column, key));
    }
    return true;
  }

  @Override
  public Long getNDV(String column, NamespaceKey key) {
    StatisticId statisticId = createStatisticId(column, key);
    Statistic statistic = statisticStore.get(statisticId);
    if (statistic == null) {
      logger.trace(
          String.format("NDV Statistic Not Found for column %s and dataset %s", column, key));
      return null;
    }
    return statistic.getNdv();
  }

  @Override
  public Long getRowCount(NamespaceKey key) {
    return getRowCount(key.toString());
  }

  private Long getRowCount(String key) {
    StatisticId statisticId = createRowCountStatisticId(key);
    Statistic statistic = statisticStore.get(statisticId);
    if (statistic == null) {
      logger.trace(String.format("RowCount Statistic Not Found for dataset %s", key));
      return null;
    }
    return statistic.getRowCount();
  }

  @Override
  public Long getNullCount(String column, NamespaceKey key) {
    StatisticId statisticId = createStatisticId(column, key);
    Statistic statistic = statisticStore.get(statisticId);
    Statistic rowCountStatistic = statisticStore.get(createRowCountStatisticId(key));
    if (statistic == null) {
      logger.trace(
          String.format("NullCount Statistic Not Found for column %s and dataset %s", column, key));
      return null;
    } else if (rowCountStatistic == null) {
      logger.trace(String.format("RowCount Statistic Not Found for dataset %s", key));
      return null;
    }

    return rowCountStatistic.getRowCount() - statistic.getColumnRowCount();
  }

  @Override
  public Histogram getHistogram(String column, TableMetadata tableMetaData) {
    SqlTypeName sqlTypeName = getSqlTypeNameFromColumn(column, tableMetaData.getSchema());
    StatisticId statisticId = createStatisticId(column, tableMetaData.getName().toString());
    Statistic statistic = statisticStore.get(statisticId);
    if (statistic == null) {
      logger.trace(
          String.format(
              "Histogram Statistic Not Found for column %s and dataset %s",
              column, tableMetaData.getName().toString()));
      return null;
    }
    return statistic.getHistogram(sqlTypeName);
  }

  @Override
  @VisibleForTesting
  public Histogram getHistogram(String column, NamespaceKey key, SqlTypeName sqlTypeName) {
    StatisticId statisticId = createStatisticId(column, key);
    Statistic statistic = statisticStore.get(statisticId);
    if (statistic == null) {
      logger.trace(
          String.format("Histogram Statistic Not Found for column %s and dataset %s", column, key));
      return null;
    }
    return statistic.getHistogram(sqlTypeName);
  }

  @Override
  public String requestStatistics(List<Field> fields, NamespaceKey key, Double samplingRate) {
    validateDataset(key);
    final JobSubmittedListener listener = new JobSubmittedListener();
    final JobId jobId =
        jobsService
            .get()
            .submitJob(
                SubmitJobRequest.newBuilder()
                    .setQueryType(QueryType.UI_INTERNAL_RUN)
                    .setSqlQuery(
                        com.dremio.service.job.SqlQuery.newBuilder()
                            .setSql(getSql(fields, key.toString(), samplingRate))
                            .setUsername(SystemUser.SYSTEM_USERNAME))
                    .build(),
                listener)
            .getJobId();
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

  public String getSql(List<Field> fields, String table, Double samplingRate) {
    StringBuilder stringBuilder = new StringBuilder("SELECT '");
    stringBuilder.append(table).append("' as ").append(TABLE_COLUMN_NAME);
    populateNdvSql(stringBuilder, fields);
    populateCountStarSql(stringBuilder);
    populateCountColumnSql(stringBuilder, fields);
    populateTDigestSql(stringBuilder, fields, samplingRate != null);
    populateItemsSketchSql(stringBuilder, fields);
    stringBuilder.append(getFromClause(fields, table, samplingRate));
    return stringBuilder.toString();
  }

  private String getNonSampleColName(String name) {
    return String.format("\"%s_%s\"", NON_SAMPLE_COL_PREFIX, name);
  }

  private String getFromClause(List<Field> fields, String table, Double samplingRate) {
    StringBuilder sb = new StringBuilder("FROM (Select ");
    for (int i = 0; i < fields.size(); i++) {
      sb.append(fields.get(i).getName())
          .append(" as ")
          .append(getNonSampleColName(fields.get(i).getName()));
      if (i != fields.size() - 1) {
        sb.append(", ");
      }
    }
    if (samplingRate != null) {
      sb.append(String.format(", sample(%f) %s from %s)", samplingRate, SAMPLE_COL_NAME, table));
    } else {
      sb.append("from ").append(table).append(")");
    }
    return sb.toString();
  }

  private OptionManager getOptionManager() {
    return sabotContext.get().getOptionManager();
  }

  private void populateNdvSql(StringBuilder stringBuilder, List<Field> fields) {
    if (!getOptionManager().getOption(PlannerSettings.COMPUTE_NDV_STAT)) {
      return;
    }
    for (Field field : fields) {
      String column = field.getName();
      stringBuilder.append(", ");
      stringBuilder.append(
          String.format(
              "ndv(%s) as \"%s\" ",
              getNonSampleColName(column), getColumnName(Statistic.StatisticType.NDV, column)));
    }
  }

  private void populateCountStarSql(StringBuilder stringBuilder) {
    if (!getOptionManager().getOption(PlannerSettings.COMPUTE_ROWCOUNT_STAT)) {
      return;
    }
    stringBuilder.append(", ");
    stringBuilder.append(
        String.format(
            "count(*) as \"%s\" ",
            getColumnName(Statistic.StatisticType.RCOUNT, ROW_COUNT_IDENTIFIER)));
  }

  private void populateCountColumnSql(StringBuilder stringBuilder, List<Field> fields) {
    if (!getOptionManager().getOption(PlannerSettings.COMPUTE_COUNT_COL_STAT)) {
      return;
    }
    for (Field field : fields) {
      String column = field.getName();
      stringBuilder.append(", ");
      stringBuilder.append(
          String.format(
              "count(%s) as \"%s\" ",
              getNonSampleColName(column),
              getColumnName(Statistic.StatisticType.COLRCOUNT, column)));
    }
  }

  private void populateTDigestSql(StringBuilder stringBuilder, List<Field> fields, boolean sample) {
    if (!getOptionManager().getOption(PlannerSettings.COMPUTE_TDIGEST_STAT)) {
      return;
    }
    for (Field field : fields) {
      String column = field.getName();
      if (!isSupportedTypeForTDigest(field.getFieldType())) {
        logger.warn(String.format("Could not Populate TDigest for column %s", column));
        continue;
      }
      stringBuilder.append(", ");
      if (sample) {
        stringBuilder.append(
            String.format(
                "tdigest(%s, %s) as \"%s\" ",
                getNonSampleColName(column),
                SAMPLE_COL_NAME,
                getColumnName(Statistic.StatisticType.TDIGEST, column)));
      } else {
        stringBuilder.append(
            String.format(
                "tdigest(%s, true) as \"%s\" ",
                getNonSampleColName(column),
                getColumnName(Statistic.StatisticType.TDIGEST, column)));
      }
    }
  }

  private void populateItemsSketchSql(StringBuilder stringBuilder, List<Field> fields) {
    if (!getOptionManager().getOption(PlannerSettings.COMPUTE_ITEMSSKETCH_STAT)) {
      return;
    }
    for (Field field : fields) {
      String column = field.getName();
      if (!isSupportedTypeForItemsSketch(field.getFieldType())) {
        logger.warn(String.format("Could not Populate ItemsSketch for column %s", column));
        continue;
      }
      stringBuilder.append(", ");
      stringBuilder.append(
          String.format(
              "ITEMS_SKETCH(%s) as \"%s\" ",
              getNonSampleColName(column),
              getColumnName(Statistic.StatisticType.ITEMSSKETCH, column)));
    }
  }

  private boolean isSupportedTypeForTDigest(FieldType fieldType) {
    switch (fieldType.getType().getTypeID()) {
      case Struct:
      case List:
      case LargeList:
      case FixedSizeList:
      case Union:
      case Map:
      case Interval:
      case Duration:
      case Utf8:
      case LargeBinary:
      case Binary:
      case FixedSizeBinary:
        return false;
      default:
        return true;
    }
  }

  private boolean isSupportedTypeForItemsSketch(FieldType fieldType) {
    switch (fieldType.getType().getTypeID()) {
      case Struct:
      case List:
      case LargeList:
      case FixedSizeList:
      case Union:
      case Map:
      case LargeBinary:
      case Binary:
      case FixedSizeBinary:
        return false;
      default:
        return true;
    }
  }

  @Override
  public void start() throws Exception {
    final DremioConfig dremioConfig = sabotContext.get().getDremioConfig();
    this.statisticStore =
        new StatisticStore(
            storeProvider,
            dremioConfig.getLong(DremioConfig.STATISTICS_CACHE_MAX_ENTRIES),
            dremioConfig.getLong(DremioConfig.STATISTICS_CACHE_TIMEOUT_MINUTES));
    this.statisticEntriesStore = new StatisticEntriesStore(storeProvider);
    this.entries = new ConcurrentHashMap<>();
    for (Map.Entry<String, JobId> entry : statisticEntriesStore.getAll()) {
      entries.put(entry.getKey(), entry.getValue());
    }
    schedulerService
        .get()
        .schedule(Schedule.Builder.everySeconds(10).build(), new StatisticsUpdater());
  }

  private void updateStatistic(
      String table, String column, Statistic.StatisticType type, Object value) {
    StatisticId statisticId = createStatisticId(column, table);
    Statistic statistic =
        (statisticStore.get(statisticId) != null)
            ? statisticStore.get(statisticId)
            : new Statistic();
    Statistic.StatisticBuilder statisticBuilder = new Statistic.StatisticBuilder(statistic);
    statisticBuilder.update(type, value);
    statisticStore.save(statisticId, statisticBuilder.build());
  }

  /** StatisticsUpdater */
  private final class StatisticsUpdater implements Runnable {
    @Override
    public void run() {
      try {
        if (entries.entrySet().size() == 0) {
          return;
        }
        Set<String> tablesToRemove = new HashSet<>();
        for (Map.Entry<String, JobId> entry : entries.entrySet()) {
          try {
            final JobId id = entry.getValue();
            JobProtobuf.JobId.Builder builder = JobProtobuf.JobId.newBuilder();
            builder.setId(id.getId());
            JobDetails jobDetails =
                jobsService
                    .get()
                    .getJobDetails(
                        JobDetailsRequest.newBuilder()
                            .setJobId(builder.build())
                            .setUserName(SystemUser.SYSTEM_USERNAME)
                            .build());
            List<JobProtobuf.JobAttempt> attempts = jobDetails.getAttemptsList();
            JobProtobuf.JobAttempt lastAttempt = attempts.get(attempts.size() - 1);
            switch (lastAttempt.getState()) {
              case COMPLETED:
                try (final JobDataFragment data =
                    JobDataClientUtils.getJobData(jobsService.get(), allocator.get(), id, 0, 1)) {
                  List<Field> fields = data.getSchema().getFields();
                  Preconditions.checkArgument(fields.get(0).getName().equals(TABLE_COLUMN_NAME));
                  String table = data.extractValue(fields.get(0).getName(), 0).toString();
                  StatisticsInputBuilder statisticsInputBuilder = new StatisticsInputBuilder(table);
                  for (int i = 1; i < fields.size(); i++) {
                    String name = fields.get(i).getName();
                    Object value = data.extractValue(name, 0);
                    String[] names = name.split("_", 2);
                    Statistic.StatisticType type = Statistic.StatisticType.valueOf(names[0]);
                    String columnName = names[1];
                    statisticsInputBuilder.updateStatistic(columnName, type, value);
                  }
                  Map<StatisticId, Statistic> statisticIdStatisticHashMap =
                      statisticsInputBuilder.build();
                  statisticIdStatisticHashMap.forEach(statisticStore::save);
                }
                // fall through
              case CANCELED:
              case FAILED:
              case CANCELLATION_REQUESTED:
                tablesToRemove.add(entry.getKey());
                break;
              default:
                break;
            }
          } catch (Exception ex) {
            logger.warn("Failed to handle statistics entry, {}", entry, ex);
            tablesToRemove.add(entry.getKey());
          }
        }
        tablesToRemove.forEach(
            key -> {
              entries.remove(key.toLowerCase());
              statisticEntriesStore.delete(key.toLowerCase());
            });
      } catch (Exception ex) {
        logger.warn("Failure while attempting to update statistics.", ex);
      }
    }
  }

  @Override
  public void close() throws Exception {}

  /** Statistics Input Builder */
  public class StatisticsInputBuilder {
    private final String table;
    private final Map<StatisticId, Statistic.StatisticBuilder> builderMap;

    public StatisticsInputBuilder(String table) {
      this.builderMap = new HashMap<>();
      this.table = table;
    }

    public void updateStatistic(String column, Statistic.StatisticType type, Object value) {
      StatisticId statisticId = createStatisticId(column, table);
      if (!builderMap.containsKey(statisticId)) {
        builderMap.put(statisticId, new Statistic.StatisticBuilder());
      }
      builderMap.get(statisticId).update(type, value);
    }

    public Map<StatisticId, Statistic> build() {
      Map<StatisticId, Statistic> statisticIdStatisticHashMap = new HashMap<>();
      builderMap.forEach(
          (k, v) -> {
            statisticIdStatisticHashMap.put(k, v.build());
          });
      return statisticIdStatisticHashMap;
    }
  }
}
