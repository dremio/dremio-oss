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
package com.dremio.service.jobs;

import static java.lang.String.format;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.dremio.sabot.op.aggregate.vectorized.HashAggStats;
import com.dremio.sabot.op.join.vhash.HashJoinStats;
import com.dremio.sabot.op.sort.external.ExternalSortStats;
import com.dremio.sabot.op.writer.WriterOperator;
import com.dremio.service.job.proto.CommonDatasetProfile;
import com.dremio.service.job.proto.DatasetPathUI;
import com.dremio.service.job.proto.FileSystemDatasetProfile;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobStats;
import com.dremio.service.job.proto.OperationType;
import com.dremio.service.job.proto.SpillJobDetails;
import com.dremio.service.job.proto.TableDatasetProfile;
import com.dremio.service.job.proto.TopOperation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Parse query profile into job details, stats. All incomplete fields are either marked null or with
 * -1.
 */
class QueryProfileParser {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(QueryProfileParser.class);

  private static final Splitter splitter =
      Splitter.on(CharMatcher.is(',')).trimResults().omitEmptyStrings();

  private final Map<String, String> operatorToTable;
  private final Map<String, TableDatasetProfile> tableDatasetProfileMap;
  private final Map<String, FileSystemDatasetProfile> fileSystemDatasetProfileMap;
  private final Map<OperationType, Long> topOperationsMap;
  private final JobDetails jobDetails;
  private final JobStats jobStats;
  private long queryOutputRecords = 0;
  private long queryOutputBytes = 0;
  private final ObjectMapper mapper;
  private final QueryProfile queryProfile;
  private final JobId jobId;
  private boolean queryOutputLimited = false;
  private SpillJobDetails spillJobDetails;

  public QueryProfileParser(final JobId jobId, final QueryProfile queryProfile) throws IOException {
    mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS,true);
    operatorToTable = Maps.newHashMap();
    jobDetails = new JobDetails();
    jobStats = new JobStats();
    fileSystemDatasetProfileMap = Maps.newHashMap();
    tableDatasetProfileMap = Maps.newHashMap();
    topOperationsMap = Maps.newHashMap();
    this.queryProfile = queryProfile;
    this.jobId = jobId;
    this.spillJobDetails = null;
    parse();
  }

  private void setOutputStatsForQueryResults(List<StreamProfile> streams, boolean isOutputLimited) {
    if (streams == null) {
      return;
    }
    long outputBytes = 0L;
    long outputRecords = 0L;
    for (StreamProfile stream : streams) {
      outputRecords += stream.getRecords();
      outputBytes += stream.getSize();
    }
    queryOutputBytes += outputBytes;
    queryOutputRecords += outputRecords;
    queryOutputLimited = queryOutputLimited || isOutputLimited;
  }

  private void setOutputStats(List<StreamProfile> streams) {
    if (streams == null) {
      return;
    }
    long outputBytes = 0L;
    long outputRecords = 0L;
    for (StreamProfile stream : streams) {
      outputRecords += stream.getRecords();
      outputBytes += stream.getSize();
    }
    if (jobStats.getOutputBytes() != null) {
      jobStats.setOutputBytes(jobStats.getOutputBytes() + outputBytes);
    } else {
      jobStats.setOutputBytes(outputBytes);
    }
    if (jobStats.getOutputRecords() != null) {
      jobStats.setOutputRecords(jobStats.getOutputRecords() + outputRecords);
    } else {
      jobStats.setOutputRecords(outputRecords);
    }
  }

  private void setDmlStats(OperatorProfile operatorProfile) {
    if (operatorProfile == null) {
      return;
    }
    long addedFiles =
        Optional.ofNullable(jobStats.getAddedFiles()).orElse(0L) + operatorProfile.getAddedFiles();
    jobStats.setAddedFiles(addedFiles);

    long removedFiles =
        Optional.ofNullable(jobStats.getRemovedFiles()).orElse(0L)
            + operatorProfile.getRemovedFiles();
    jobStats.setRemovedFiles(removedFiles);
  }

  private long toMillis(long nanos) {
    return TimeUnit.NANOSECONDS.toMillis(nanos);
  }

  // create operator lookup key used in physical plan.
  private String createOperatorKey(int majorFragmentId, int operatorId) {
    return format("\"%02d-%02d\"", majorFragmentId, operatorId);
  }

  // Set time spent in planning
  private void parsePlanningDetails() {
    if (queryProfile.getPlanningEnd() > 0 && queryProfile.getPlanningStart() > 0) {
      jobDetails.setTimeSpentInPlanning(
          queryProfile.getPlanningEnd() - queryProfile.getPlanningStart());
    } else {
      jobDetails.setTimeSpentInPlanning(null);
    }
    // Not yet available.
    jobDetails.setPlansConsidered(null);
  }

  private void parseMemoryDetails() {
    // Not yet available.
    // jobDetails.setPeakMemory(null);
  }

  private void checkIsAssignable(String field, Class<?> target, Class<?> expected)
      throws IOException {
    if (!expected.isAssignableFrom(target)) {
      throw new IOException(
          format("Invalid field %s, expected type %s, found %s", field, expected, target));
    }
  }

  private void setWaitInClient(long waitInClient) {
    if (jobDetails.getWaitInClient() != null) {
      jobDetails.setWaitInClient(jobDetails.getWaitInClient() + waitInClient);
    } else {
      jobDetails.setWaitInClient(waitInClient);
    }
  }

  private void setCommonDatasetProfile(
      CommonDatasetProfile datasetProfile,
      OperatorProfile operatorProfile,
      MajorFragmentProfile majorFragment,
      long inputBytes,
      long inputRecords,
      List<String> datasetPath) {

    if (datasetProfile.getBytesRead() != null) {
      datasetProfile.setBytesRead(datasetProfile.getRecordsRead() + inputBytes);
    } else {
      datasetProfile.setBytesRead(inputBytes);
    }

    if (datasetProfile.getRecordsRead() != null) {
      datasetProfile.setRecordsRead(datasetProfile.getRecordsRead() + inputRecords);
    } else {
      datasetProfile.setRecordsRead(inputRecords);
    }

    final DatasetPathUI datasetPathUI = new DatasetPathUI().setDatasetPathList(datasetPath);
    if (!datasetProfile.getDatasetPathsList().contains(datasetPathUI)) {
      datasetProfile.getDatasetPathsList().add(datasetPathUI);
    }

    if (datasetProfile.getWaitOnSource() != null) {
      datasetProfile.setWaitOnSource(
          datasetProfile.getWaitOnSource() + toMillis(operatorProfile.getWaitNanos()));
    } else {
      datasetProfile.setWaitOnSource(toMillis(operatorProfile.getWaitNanos()));
    }

    if (datasetProfile.getParallelism() != null) {
      datasetProfile.setParallelism(datasetProfile.getParallelism() + 1);
    } else {
      datasetProfile.setParallelism(1);
    }
  }

  private void setScanStats(
      CoreOperatorType operatorType,
      OperatorProfile operatorProfile,
      MajorFragmentProfile majorFragment) {
    final String operatorId =
        createOperatorKey(majorFragment.getMajorFragmentId(), operatorProfile.getOperatorId());
    long inputBytes = 0L;
    long inputRecords = 0L;
    if (operatorProfile.getInputProfileList() != null) {
      for (StreamProfile stream : operatorProfile.getInputProfileList()) {
        inputRecords += stream.getRecords();
        inputBytes += stream.getSize();
      }
      addInputBytesAndRecords(inputBytes, inputRecords);
    }

    if (operatorToTable.containsKey(operatorId)) {
      // TODO check if its fs based or table based using operatorType
      final String tableName = operatorToTable.get(operatorId);
      TableDatasetProfile tableDatasetProfile = tableDatasetProfileMap.get(tableName);
      if (tableDatasetProfile == null) {
        tableDatasetProfile = new TableDatasetProfile();
        tableDatasetProfile.setDatasetProfile(
            new CommonDatasetProfile().setDatasetPathsList(Lists.<DatasetPathUI>newArrayList()));
        tableDatasetProfileMap.put(tableName, tableDatasetProfile);
      }
      setCommonDatasetProfile(
          tableDatasetProfile.getDatasetProfile(),
          operatorProfile,
          majorFragment,
          inputBytes,
          inputRecords,
          PathUtils.parseFullPath(tableName));
      tableDatasetProfile.setPushdownQuery(null);
    }
  }

  private void setAggSpillInfo(CoreOperatorType operatorType, OperatorProfile operatorProfile) {
    initSpillJobDetails();
    final int operatorNumber = operatorType.getNumber();
    Preconditions.checkState(operatorNumber == CoreOperatorType.HASH_AGGREGATE_VALUE);
    final List<UserBitShared.MetricValue> metricValues = operatorProfile.getMetricList();
    for (UserBitShared.MetricValue metricValue : metricValues) {
      final int metricId = metricValue.getMetricId();
      if (metricId == HashAggStats.Metric.TOTAL_SPILLED_DATA_SIZE.ordinal()
          && metricValue.hasLongValue()) {
        spillJobDetails.setTotalBytesSpilledByHashAgg(
            spillJobDetails.getTotalBytesSpilledByHashAgg() + metricValue.getLongValue());
      }
    }
  }

  private void setJoinSpillInfo(CoreOperatorType operatorType, OperatorProfile operatorProfile) {
    initSpillJobDetails();
    final int operatorNumber = operatorType.getNumber();
    Preconditions.checkState(operatorNumber == CoreOperatorType.HASH_JOIN_VALUE);
    final List<UserBitShared.MetricValue> metricValues = operatorProfile.getMetricList();
    for (UserBitShared.MetricValue metricValue : metricValues) {
      final int metricId = metricValue.getMetricId();
      if ((metricId == HashJoinStats.Metric.SPILL_WR_BUILD_BYTES.ordinal()
              || metricId == HashJoinStats.Metric.SPILL_WR_PROBE_BYTES.ordinal())
          && metricValue.hasLongValue()) {
        spillJobDetails.setTotalBytesSpilledByHashJoin(
            spillJobDetails.getTotalBytesSpilledByHashJoin() + metricValue.getLongValue());
      }
    }
  }

  private void initSpillJobDetails() {
    if (spillJobDetails == null) {
      this.spillJobDetails = new SpillJobDetails();
      spillJobDetails.setTotalBytesSpilledByHashAgg((long) 0);
      spillJobDetails.setTotalBytesSpilledByHashJoin((long) 0);
      spillJobDetails.setTotalBytesSpilledBySort((long) 0);
    }
  }

  private void setSortSpillInfo(CoreOperatorType operatorType, OperatorProfile operatorProfile) {
    initSpillJobDetails();
    final int operatorNumber = operatorType.getNumber();
    Preconditions.checkState(operatorNumber == CoreOperatorType.EXTERNAL_SORT_VALUE);
    final List<UserBitShared.MetricValue> metricValues = operatorProfile.getMetricList();
    for (UserBitShared.MetricValue metricValue : metricValues) {
      final int metricId = metricValue.getMetricId();
      if (metricId == ExternalSortStats.Metric.TOTAL_SPILLED_DATA_SIZE.ordinal()
          && metricValue.hasLongValue()) {
        spillJobDetails.setTotalBytesSpilledBySort(
            spillJobDetails.getTotalBytesSpilledBySort() + metricValue.getLongValue());
      }
    }
  }

  /**
   * Get the spill info for the the job after parsing the query profile
   *
   * @return null if the query never spilled, non-null if some operator spilled as of now, we only
   *     consider external sort and hashagg operators.
   */
  SpillJobDetails getSpillDetails() {
    if (spillJobDetails != null
        && (spillJobDetails.getTotalBytesSpilledByHashAgg() > 0
            || spillJobDetails.getTotalBytesSpilledBySort() > 0
            || spillJobDetails.getTotalBytesSpilledByHashJoin() > 0)) {
      return spillJobDetails;
    }
    return null;
  }

  private void addInputBytesAndRecords(long inputBytes, long inputRecords) {
    if (jobStats.getInputBytes() != null) {
      jobStats.setInputBytes(jobStats.getInputBytes() + inputBytes);
    } else {
      jobStats.setInputBytes(inputBytes);
    }
    if (jobStats.getInputRecords() != null) {
      jobStats.setInputRecords(jobStats.getInputRecords() + inputRecords);
    } else {
      jobStats.setInputRecords(inputRecords);
    }
  }

  private void setOperationStats(OperationType operationType, long cpuUsed) {
    if (topOperationsMap.containsKey(operationType)) {
      cpuUsed += topOperationsMap.get(operationType);
    }
    topOperationsMap.put(operationType, cpuUsed);
  }

  @SuppressWarnings("unchecked")
  private void parsePhysicalPlan() throws IOException {
    if (queryProfile.getJsonPlan() == null || queryProfile.getJsonPlan().isEmpty()) {
      return;
    }
    // Parse the plan and map tables to major fragment and operator ids.
    final Map<String, Object> plan = mapper.readValue(queryProfile.getJsonPlan(), Map.class);
    for (Map.Entry<String, Object> entry : plan.entrySet()) {
      checkIsAssignable(entry.getKey(), entry.getValue().getClass(), Map.class);
      final Map<String, Object> operatorInfo = (Map) entry.getValue();
      final String operator = (String) operatorInfo.get("\"op\"");
      if (operator != null
          && (operator.contains("Scan") || operator.contains("TableFunction"))
          && operatorInfo.containsKey("\"values\"")) {
        // Get table name
        checkIsAssignable(
            entry.getKey() + ": values", operatorInfo.get("\"values\"").getClass(), Map.class);
        final Map<String, Object> values = (Map) operatorInfo.get("\"values\"");
        if (values.containsKey("\"table\"")) {
          // TODO (Amit H) remove this after we clean up code.
          final String tokens = ((String) values.get("\"table\"")).replaceAll("^\\[|\\]$", "");
          final String tablePath = PathUtils.constructFullPath(splitter.splitToList(tokens));
          operatorToTable.put(entry.getKey(), tablePath);
        }
      }
    }
  }

  /**
   * Parse query profile into JobDetails and JobStats. Avoid going over same data twice.
   *
   * @return JobDetails
   * @throws IOException
   */
  private void parse() throws IOException {
    int totalMajorFragments = 0;
    long totalTimeInMillis = 0L;

    // Parse physical plan from jsonPlan field. Can throw exception
    try {
      parsePhysicalPlan();
    } catch (IOException ioe) {
      logger.error(
          "Failed to parse physical plan for query {}, plan {}",
          jobId.getId(),
          queryProfile.getJsonPlan(),
          ioe);
    }

    parseMemoryDetails();
    parsePlanningDetails();
    if (queryProfile.getFragmentProfileList() != null) {
      totalMajorFragments = queryProfile.getFragmentProfileList().size();
    }
    if (totalMajorFragments == 0) {
      return;
    }

    boolean outputLimited = false;
    final ListMultimap<CoreOperatorType, OperatorProfile> operators = ArrayListMultimap.create();
    for (MajorFragmentProfile majorFragment : queryProfile.getFragmentProfileList()) {
      if (majorFragment.getMinorFragmentProfileList() == null) {
        continue;
      }
      for (MinorFragmentProfile minorFragmentProfile :
          majorFragment.getMinorFragmentProfileList()) {
        if (minorFragmentProfile.getOperatorProfileList() == null) {
          continue;
        }
        for (OperatorProfile operatorProfile : minorFragmentProfile.getOperatorProfileList()) {
          totalTimeInMillis +=
              toMillis(
                  operatorProfile.getProcessNanos()
                      + operatorProfile.getWaitNanos()
                      + operatorProfile.getSetupNanos());
          final CoreOperatorType operatorType =
              CoreOperatorType.valueOf(operatorProfile.getOperatorType());

          // TODO (Amit H) undefined operators, TRACE, unknown.
          if (operatorType == null) {
            logger.error(
                "QueryProfile has unknown operator type " + operatorProfile.getOperatorType());
            setOperationStats(OperationType.Misc, toMillis(operatorProfile.getProcessNanos()));
            continue;
          }

          operators.put(operatorType, operatorProfile);

          switch (operatorType) {
            case SCREEN:
              setWaitInClient(toMillis(operatorProfile.getWaitNanos()));
              // Time spent in client side processing and wait
              setOperationStats(
                  OperationType.Client,
                  toMillis(
                      operatorProfile.getProcessNanos()
                          + operatorProfile.getWaitNanos()
                          + operatorProfile.getSetupNanos()));
              break;

            case AVRO_SUB_SCAN:
            case DIRECT_SUB_SCAN:
            case HBASE_SUB_SCAN:
            case HIVE_SUB_SCAN:
            case INFO_SCHEMA_SUB_SCAN:
            case JSON_SUB_SCAN:
            case MOCK_SUB_SCAN:
            case PARQUET_ROW_GROUP_SCAN:
            case SYSTEM_TABLE_SCAN:
            case TEXT_SUB_SCAN:
            case ELASTICSEARCH_AGGREGATOR_SUB_SCAN:
            case ELASTICSEARCH_SUB_SCAN:
            case MONGO_SUB_SCAN:
            case JDBC_SUB_SCAN:
            case FLIGHT_SUB_SCAN:
              setScanStats(operatorType, operatorProfile, majorFragment);
              // wait time in scan is shown per table.
              setOperationStats(
                  OperationType.Reading,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case VALUES_READER:
              addInputBytesAndRecords(0, 0); // 0 bytes/records read from disk
              setOperationStats(
                  OperationType.Reading,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case EXTERNAL_SORT:
              setSortSpillInfo(operatorType, operatorProfile);
              setOperationStats(
                  OperationType.Sort,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case OLD_SORT:
            case TOP_N_SORT:
              setOperationStats(
                  OperationType.Sort,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case HASH_AGGREGATE:
              setAggSpillInfo(operatorType, operatorProfile);
              setOperationStats(
                  OperationType.Aggregate,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;
            case STREAMING_AGGREGATE:
              setOperationStats(
                  OperationType.Aggregate,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case PROJECT:
              setOperationStats(
                  OperationType.Project,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case HASH_JOIN:
              setJoinSpillInfo(operatorType, operatorProfile);
              // fall through
            case MERGE_JOIN:
            case NESTED_LOOP_JOIN:
              setOperationStats(
                  OperationType.Join,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case FILTER:
            case SELECTION_VECTOR_REMOVER:
              setOperationStats(
                  OperationType.Filter,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case UNION:
              setOperationStats(
                  OperationType.Union,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case WINDOW:
              setOperationStats(
                  OperationType.Window,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case HASH_PARTITION_SENDER:
            case BROADCAST_SENDER:
            case ORDERED_PARTITION_SENDER:
            case RANGE_SENDER:
            case SINGLE_SENDER:
              // for senders include processing, setup and wait time
              setOperationStats(
                  OperationType.Data_exchange,
                  toMillis(
                      operatorProfile.getSetupNanos()
                          + operatorProfile.getProcessNanos()
                          + operatorProfile.getWaitNanos()));
              break;

            case MERGING_RECEIVER:
            case UNORDERED_RECEIVER:
              // for receivers include processing and setup time.
              setOperationStats(
                  OperationType.Data_exchange,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

              // job output writer
            case ARROW_WRITER:
              setOperationStats(
                  OperationType.Writing,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              if (isArrowWriterOutputLimited(operatorProfile)) {
                outputLimited = true;
              }
              break;

              // CTAS writers
            case PARQUET_WRITER:
            case TEXT_WRITER:
            case JSON_WRITER:
              setOutputStats(operatorProfile.getInputProfileList());
              setOperationStats(
                  OperationType.Writing,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case LIMIT:
              setOperationStats(
                  OperationType.Limit,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case COMPLEX_TO_JSON:
              setOperationStats(
                  OperationType.Complext_to_JSON,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;

            case PRODUCER_CONSUMER:
              setOperationStats(
                  OperationType.Producer_consumer,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;
            case FLATTEN:
              setOperationStats(
                  OperationType.Flatten,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;
            case TABLE_FUNCTION:
              if (operatorProfile.getOperatorSubtype()
                      == TableFunctionConfig.FunctionType.DATA_FILE_SCAN.ordinal()
                  || operatorProfile.getOperatorSubtype()
                      == TableFunctionConfig.FunctionType.EASY_DATA_FILE_SCAN.ordinal()
                  || operatorProfile.getOperatorSubtype()
                      == TableFunctionConfig.FunctionType.TRIGGER_PIPE_EASY_DATA_SCAN.ordinal()) {
                setScanStats(operatorType, operatorProfile, majorFragment);
              }
              break;
            case DELTALAKE_SUB_SCAN:
              setOperationStats(
                  OperationType.Reading,
                  toMillis(operatorProfile.getProcessNanos() + operatorProfile.getSetupNanos()));
              break;
            case WRITER_COMMITTER:
              setDmlStats(operatorProfile);
              break;
            default:
              break;
          }
        } // end of for loop for operator profiles
      } // end of for loop for minor fragments
      setJobPhasesData(majorFragment);
    } // end of for loop for major fragments

    // finalize output stats

    // Get query output stats
    // Try first ARROW_WRITER operators (UI) or SCREEN otherwise (for external clients)
    for (CoreOperatorType operatorType :
        Arrays.asList(CoreOperatorType.ARROW_WRITER, CoreOperatorType.SCREEN)) {
      List<OperatorProfile> profiles = operators.get(operatorType);
      if (profiles.isEmpty()) {
        continue;
      }

      for (OperatorProfile profile : profiles) {
        List<StreamProfile> streams = profile.getInputProfileList();
        setOutputStatsForQueryResults(streams, outputLimited);
      }
      // No need to try other operators
      break;
    }

    if (jobStats.getOutputRecords() == null) {
      jobStats.setOutputRecords(queryOutputRecords);
    }
    if (jobStats.getOutputBytes() == null) {
      jobStats.setOutputBytes(queryOutputBytes);
    }
    if (jobStats.getIsOutputLimited() == null) {
      jobStats.setIsOutputLimited(queryOutputLimited);
    }
    jobDetails.setDataVolume(jobStats.getOutputBytes());
    jobDetails.setFsDatasetProfilesList(new ArrayList<>(fileSystemDatasetProfileMap.values()));
    jobDetails.setTableDatasetProfilesList(new ArrayList<>(tableDatasetProfileMap.values()));
    final List<TopOperation> topOperations = Lists.newArrayList();
    if (totalTimeInMillis > 0) {
      float factor = 100.0f / totalTimeInMillis;
      for (Map.Entry<OperationType, Long> entry : topOperationsMap.entrySet()) {
        topOperations.add(new TopOperation(entry.getKey(), entry.getValue() * factor));
      }
    }
    Collections.sort(
        topOperations,
        new Comparator<TopOperation>() {
          @Override
          public int compare(TopOperation o1, TopOperation o2) {
            // descending order
            return Float.compare(o2.getTimeConsumed(), o1.getTimeConsumed());
          }
        });
    jobDetails.setTopOperationsList(topOperations);
  }

  private void setJobPhasesData(MajorFragmentProfile majorFragment) {
    Long totalMemory = 0L;

    if (jobDetails.getTotalMemory() != null) {
      if (jobDetails.getTotalMemory() > 0) {
        totalMemory = jobDetails.getTotalMemory();
      }
    }
    totalMemory +=
        majorFragment.getNodePhaseProfileList().stream()
            .collect(Collectors.summarizingLong(memory -> memory.getMaxMemoryUsed()))
            .getSum();
    Long peakMemory =
        majorFragment.getNodePhaseProfileList().stream()
            .collect(Collectors.summarizingLong(memory -> memory.getMaxMemoryUsed()))
            .getMax();
    if (jobDetails.getPeakMemory() != null) {
      if (jobDetails.getPeakMemory() > peakMemory) {
        peakMemory = jobDetails.getPeakMemory();
      }
    }
    jobDetails.setPeakMemory(peakMemory);
    jobDetails.setTotalMemory(totalMemory);
    jobDetails.setCpuUsed(topOperationsMap.values().stream().mapToLong(value -> value).sum());
  }

  private boolean isArrowWriterOutputLimited(OperatorProfile operatorProfile) {
    for (int i = 0; i < operatorProfile.getMetricCount(); i++) {
      UserBitShared.MetricValue metricValue = operatorProfile.getMetric(i);
      if (metricValue.getMetricId() == WriterOperator.Metric.OUTPUT_LIMITED.ordinal()) {
        return metricValue.getLongValue() > 0;
      }
    }
    return false;
  }

  public JobStats getJobStats() {
    return jobStats;
  }

  public JobDetails getJobDetails() {
    return jobDetails;
  }
}
