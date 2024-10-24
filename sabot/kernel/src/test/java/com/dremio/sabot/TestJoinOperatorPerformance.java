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
package com.dremio.sabot;

import static com.dremio.sabot.PerformanceTestsHelper.getFieldInfos;
import static com.dremio.sabot.PerformanceTestsHelper.getFieldNames;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.JoinCondition;
import com.dremio.common.util.TestTools;
import com.dremio.sabot.FieldInfo.SortOrder;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.join.hash.TestHashJoin;
import com.dremio.sabot.op.join.vhash.HashJoinStats.Metric;
import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestJoinOperatorPerformance extends TestHashJoin {

  private static final String FILE_PATH = "../";
  private List<FieldInfo> buildFieldInfos;
  private List<FieldInfo> probeFieldInfos;
  private int numberOfBuildRows;
  private int numberOfProbeRows;
  private int batchSize;
  private int numOfBuildCarryOvers;
  private int numOfProbeCarryOvers;
  private int numOfFixedJoinKeys;
  private int numOfVarJoinKeys;
  private int varcharLen;
  List<JoinCondition> joinConditions;
  JoinRelType joinType;
  LogicalExpression extraCondition;
  private int maxRandomInt;
  private boolean isSpillingJoin;
  private String fileName;

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(Duration.ofMinutes(60));

  public TestJoinOperatorPerformance(
      List<FieldInfo> buildFieldInfos,
      List<FieldInfo> probeFieldInfos,
      int numberOfBuildRows,
      int numberOfProbeRows,
      int batchSize,
      int numOfBuildCarryOvers,
      int numOfProbeCarryOvers,
      int numOfFixedJoinKeys,
      int numOfVarJoinKeys,
      int varcharLen,
      List<JoinCondition> joinConditions,
      JoinRelType joinType,
      LogicalExpression extraCondition,
      int maxRandomInt,
      boolean isSpillingJoin,
      String fileName) {
    this.buildFieldInfos = buildFieldInfos;
    this.probeFieldInfos = probeFieldInfos;
    this.numberOfBuildRows = numberOfBuildRows;
    this.numberOfProbeRows = numberOfProbeRows;
    this.batchSize = batchSize;
    this.numOfBuildCarryOvers = numOfBuildCarryOvers;
    this.numOfProbeCarryOvers = numOfProbeCarryOvers;
    this.numOfFixedJoinKeys = numOfFixedJoinKeys;
    this.numOfVarJoinKeys = numOfVarJoinKeys;
    this.varcharLen = varcharLen;
    this.joinConditions = joinConditions;
    this.joinType = joinType;
    this.extraCondition = extraCondition;
    this.maxRandomInt = maxRandomInt;
    this.isSpillingJoin = isSpillingJoin;
    this.fileName = fileName;
  }

  /*
  Each method called from this function returns an Object[] where each Object is a value for TestJoinOperatorPerformance members in same order as they
  are declared.
  Right now each Object[] returned would contain -
  List<FieldInfo> buildFieldInfos;
  List<FieldInfo> probeFieldInfos;
  int numberOfBuildRows;
  int numberOfProbeRows;
  int batchSize;
  int numOfBuildCarryOvers;
  int numOfProbeCarryOvers;
  int numOfFixedJoinKeys;
  int numOfVarJoinKeys;
  int varcharLen;
  List<JoinCondition> joinConditions;
  JoinRelType joinType;
  LogicalExpression extraCondition;
  int maxRandomInt;
  boolean isSpillingJoin;
  String fileName;

    If more parameters are to be added in the future, they should be added as class members and test values for them would accordingly be added in
    following array.

    https://github.com/junit-team/junit4/wiki/Parameterized-tests
     */
  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {
    List<Object[]> parameters = new ArrayList<>();
    parameters.addAll(scaleBatchSizes(2));
    /*parameters.addAll(scaleBatchSizes(900));
    parameters.addAll(scaleBuildKeyChains());
    parameters.addAll(scaleNoOfBatches(JoinRelType.LEFT, 400_000));
    parameters.addAll(scaleNoOfBatches(JoinRelType.LEFT, 40_000_000));
    parameters.addAll(scaleNoOfCarryOvers());
    parameters.addAll(scaleNoOfBatches(JoinRelType.RIGHT, 400_000));
    parameters.addAll(scaleNoOfBatches(JoinRelType.INNER, 400_000));
    parameters.addAll(scaleIntJoinKeysLongChains());*/
    return parameters;
  }

  @Ignore
  @Test
  public void testJoin() throws Exception {
    try (BatchDataGenerator buildGenerator =
            new BatchDataGenerator(
                buildFieldInfos,
                getTestAllocator(),
                numberOfBuildRows,
                batchSize,
                varcharLen,
                maxRandomInt);
        BatchDataGenerator probeGenerator =
            new BatchDataGenerator(
                probeFieldInfos,
                getTestAllocator(),
                numberOfProbeRows,
                batchSize,
                varcharLen,
                maxRandomInt)) {
      JoinInfo info = null;
      if (isSpillingJoin) {
        info =
            getSpillingJoinInfo(
                joinConditions, extraCondition, joinType, 1_000_000, Integer.MAX_VALUE);
      } else {
        info =
            getNoSpillJoinInfo(
                joinConditions, extraCondition, joinType, 1_000_000, Integer.MAX_VALUE);
      }
      OperatorStats stats =
          validateDual(
              info.operator, info.clazz, probeGenerator, buildGenerator, batchSize, null, true);

      writeResultsCSV(
          stats,
          batchSize,
          varcharLen,
          numberOfBuildRows,
          numberOfProbeRows,
          numOfBuildCarryOvers,
          numOfProbeCarryOvers,
          numOfFixedJoinKeys,
          numOfVarJoinKeys,
          joinType,
          maxRandomInt,
          "",
          "");
    } catch (Exception e) {
      writeResultsCSV(
          null,
          batchSize,
          varcharLen,
          numberOfBuildRows,
          numberOfProbeRows,
          numOfBuildCarryOvers,
          numOfProbeCarryOvers,
          numOfFixedJoinKeys,
          numOfVarJoinKeys,
          joinType,
          maxRandomInt,
          e.toString(),
          Arrays.toString(e.getStackTrace()));
    }
  }

  public static Collection<Object[]> scaleBatchSizes(int varcharSize) throws Exception {
    List<Object[]> parameters = new ArrayList<>();
    String fileName = "SHJScaleBatchSizesSmallString";
    writeFileHeader(fileName);
    int numOfVarJoinKeyPerSide = 2;
    int numberOfCarryOvers = 16;
    int dataSize = 1100000000;
    for (int batchSize = 3500; batchSize <= 50_000; batchSize += 200) {
      int numOfBatches = (dataSize / batchSize / (varcharSize * 18));
      parameters.add(
          getParameterList(
              0,
              numOfVarJoinKeyPerSide,
              batchSize,
              numOfBatches,
              numOfBatches,
              numberOfCarryOvers,
              numberOfCarryOvers,
              varcharSize,
              JoinRelType.INNER,
              null,
              10_000 * batchSize,
              true,
              batchSize,
              fileName));
    }
    return parameters;
  }

  public static Collection<Object[]> scaleNoOfCarryOvers() throws Exception {
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "SHJscaleNoOfCarryOvers";
    writeFileHeader(fileName);
    int batchSize = 4000;
    int numOfFixedJoinKeysPerSide = 1;
    int numOfBatches = 500;
    for (int numberOfCarryOvers = 2; numberOfCarryOvers < 500; numberOfCarryOvers++) {
      parameters.add(
          getParameterList(
              numOfFixedJoinKeysPerSide,
              0,
              batchSize,
              numOfBatches,
              numOfBatches,
              numberOfCarryOvers,
              2,
              15,
              JoinRelType.LEFT,
              null,
              10_000 * batchSize,
              true,
              batchSize,
              fileName));
    }
    return parameters;
  }

  private static Object[] getParameterList(
      int numOfFixedJoinKeysPerSide,
      int numOfVarJoinKeysPerSide,
      int batchSize,
      int numOfBuildBatches,
      int numOfProbeBatches,
      int numOfBuildCarryOvers,
      int numOfProbeCarryOvers,
      int varcharLen,
      JoinRelType joinType,
      LogicalExpression extraCondition,
      int maxRandomInt,
      boolean isSpillingJoin,
      int numOfUniqueValuesInBuildBatch,
      String fileName) {
    List<FieldInfo> buildFieldInfos = new ArrayList<>();
    List<FieldInfo> probeFieldInfos = new ArrayList<>();

    List<FieldInfo> buildKeyFieldInfos = new ArrayList<>();
    List<FieldInfo> probeKeyFieldInfos = new ArrayList<>();

    if (numOfFixedJoinKeysPerSide > 0) {
      buildKeyFieldInfos.addAll(
          getFieldInfos(
              new ArrowType.Int(32, true),
              numOfUniqueValuesInBuildBatch,
              SortOrder.RANDOM,
              numOfFixedJoinKeysPerSide));
      probeKeyFieldInfos.addAll(
          getFieldInfos(
              new ArrowType.Int(32, true), batchSize, SortOrder.RANDOM, numOfFixedJoinKeysPerSide));
    }

    if (numOfVarJoinKeysPerSide > 0) {
      buildKeyFieldInfos.addAll(
          getFieldInfos(
              new ArrowType.Utf8(),
              numOfUniqueValuesInBuildBatch,
              SortOrder.RANDOM,
              numOfVarJoinKeysPerSide));
      probeKeyFieldInfos.addAll(
          getFieldInfos(
              new ArrowType.Utf8(), batchSize, SortOrder.RANDOM, numOfVarJoinKeysPerSide));
    }
    buildFieldInfos.addAll(buildKeyFieldInfos);
    List<String> buildKeyNames = getFieldNames(buildKeyFieldInfos);
    probeFieldInfos.addAll(probeKeyFieldInfos);
    List<String> probeKeyNames = getFieldNames(probeKeyFieldInfos);

    if (numOfBuildCarryOvers > 0) {
      buildFieldInfos.addAll(
          getFieldInfos(
              new ArrowType.Int(32, true), batchSize, SortOrder.RANDOM, numOfBuildCarryOvers));
    }

    if (numOfProbeCarryOvers > 0) {
      probeFieldInfos.addAll(
          getFieldInfos(
              new ArrowType.Int(32, true), batchSize, SortOrder.RANDOM, numOfProbeCarryOvers));
    }

    List<JoinCondition> joinConditions = new ArrayList<>();
    for (int index = 0; index < numOfFixedJoinKeysPerSide + numOfVarJoinKeysPerSide; index++) {
      joinConditions.add(
          new JoinCondition("EQUALS", f(probeKeyNames.get(index)), f(buildKeyNames.get(index))));
    }

    return new Object[] {
      buildFieldInfos,
      probeFieldInfos,
      batchSize * numOfBuildBatches,
      batchSize * numOfProbeBatches,
      batchSize,
      numOfBuildCarryOvers,
      numOfProbeCarryOvers,
      numOfFixedJoinKeysPerSide,
      numOfVarJoinKeysPerSide,
      varcharLen,
      joinConditions,
      joinType,
      extraCondition,
      maxRandomInt,
      isSpillingJoin,
      fileName
    };
  }

  public static Collection<Object[]> scaleNoOfBatches(JoinRelType joinType, int maxIntKeyValue)
      throws Exception {
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "scaleNoOfBatchesLeftJoinSHJ";
    writeFileHeader(fileName);
    int batchSize = 4000;
    int numberOfCarryOvers = 2;
    int numOfFixedJoinKeysPerSide = 1;
    for (int numOfBatches = 50; numOfBatches < 5_000; numOfBatches += 50) {
      parameters.add(
          getParameterList(
              numOfFixedJoinKeysPerSide,
              0,
              batchSize,
              numOfBatches,
              numOfBatches,
              numberOfCarryOvers,
              numberOfCarryOvers,
              15,
              joinType,
              null,
              maxIntKeyValue,
              true,
              batchSize,
              fileName));
    }
    return parameters;
  }

  public static Collection<Object[]> scaleBuildKeyChains() throws Exception {
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "scaleBuildKeyChainsNoSpillJoin";
    writeFileHeader(fileName);
    int batchSize = 4000;
    int numOfBatches = 500;
    int numberOfCarryOvers = 2;
    int numOfFixedJoinKeysPerSide = 1;
    for (int maxKeyVal = 16_000; maxKeyVal >= 1; maxKeyVal -= 100) {
      parameters.add(
          getParameterList(
              numOfFixedJoinKeysPerSide,
              0,
              batchSize,
              numOfBatches,
              numOfBatches,
              numberOfCarryOvers,
              numberOfCarryOvers,
              15,
              JoinRelType.INNER,
              null,
              maxKeyVal,
              false,
              batchSize,
              fileName));
    }
    return parameters;
  }

  public static Collection<Object[]> scaleIntJoinKeysLongChains() throws Exception {
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "scaleIntJoinKeysLongChainsMoreBatches";
    writeFileHeader(fileName);
    int batchSize = 4000;
    int numOfBatches = 500;
    int numberOfCarryOvers = 2;
    for (int numOfFixedJoinKeysPerSide = 1;
        numOfFixedJoinKeysPerSide <= 100;
        numOfFixedJoinKeysPerSide++) {
      parameters.add(
          getParameterList(
              numOfFixedJoinKeysPerSide,
              0,
              batchSize,
              numOfBatches,
              numOfBatches,
              numberOfCarryOvers,
              numberOfCarryOvers,
              15,
              JoinRelType.INNER,
              null,
              batchSize,
              true,
              batchSize / 10,
              fileName));
    }
    return parameters;
  }

  private static void writeFileHeader(String fileName) throws IOException {
    String filePath = FILE_PATH + fileName + ".csv";
    System.out.println("The results will be saved to: " + filePath);
    try (CSVWriter writer = new CSVWriter(new FileWriter(filePath, true))) {
      List<String[]> header = new ArrayList<>();
      header.add(
          new String[] {
            "Batch Size",
            "String Col Len",
            "numberOfBuildRows",
            "numberOfProbeRows",
            "numOfBuildCarryOvers",
            "numOfProbeCarryOvers",
            "numOfFixedJoinKeys",
            "numOfVarJoinKeys",
            "joinType",
            "Max Int Key Value",
            "outputCount",
            "unmatchedBuildKeyCount",
            "unmatchedProbeCount",
            "Setup Time",
            "Pivot Time",
            "Insert Time",
            "hashComputeTime",
            "slicerTime",
            "linkTime",
            "spillTime",
            "pivotProbeTime",
            "probeListTime",
            "probeCopyWatch",
            "buildCopyTime",
            "probeFindTime",
            "projectBuildNonMatchesTime",
            "totalTime",
            "Exception",
            "Exception Stack Trace"
          });
      writer.writeAll(header);
    }
  }

  private void writeResultsCSV(
      OperatorStats stats,
      int batchSize,
      int varcharLen,
      int numberOfBuildRows,
      int numberOfProbeRows,
      int numOfBuildCarryOvers,
      int numOfProbeCarryOvers,
      int numOfFixedJoinKeys,
      int numOfVarJoinKeys,
      JoinRelType joinType,
      int maxRandomInt,
      String exception,
      String stackTrace)
      throws IOException {

    String filePath = FILE_PATH + fileName + ".csv";
    System.out.println("The results will be saved to: " + filePath);
    try (CSVWriter writer = new CSVWriter(new FileWriter(filePath, true))) {
      List<String[]> metrics = new ArrayList<>();
      long setUpTime = stats != null ? stats.getLongStat(Metric.SETUP_TIME) / (1024 * 1024) : -1;
      long pivotTime =
          stats != null ? stats.getLongStat(Metric.PIVOT_TIME_NANOS) / (1024 * 1024) : -1;
      long hashComputeTime =
          stats != null ? stats.getLongStat(Metric.HASHCOMPUTATION_TIME_NANOS) / (1024 * 1024) : -1;
      long insertTime =
          stats != null ? stats.getLongStat(Metric.INSERT_TIME_NANOS) / (1024 * 1024) : -1;
      long slicerTime =
          stats != null ? stats.getLongStat(Metric.BUILD_CARRYOVER_COPY_NANOS) / (1024 * 1024) : -1;
      long linkTime =
          stats != null ? stats.getLongStat(Metric.LINK_TIME_NANOS) / (1024 * 1024) : -1;
      long spillTime = stats != null ? stats.getLongStat(Metric.SPILL_NANOS) / (1024 * 1024) : -1;
      long pivotProbeTime =
          stats != null ? stats.getLongStat(Metric.PROBE_PIVOT_NANOS) / (1024 * 1024) : -1;
      long probeListTime =
          stats != null ? stats.getLongStat(Metric.PROBE_LIST_NANOS) / (1024 * 1024) : -1;
      long probeCopyWatch =
          stats != null ? stats.getLongStat(Metric.PROBE_COPY_NANOS) / (1024 * 1024) : -1;
      long buildCopyTime =
          stats != null ? stats.getLongStat(Metric.BUILD_COPY_NANOS) / (1024 * 1024) : -1;
      long probeFindTime =
          stats != null ? stats.getLongStat(Metric.PROBE_FIND_NANOS) / (1024 * 1024) : -1;
      long projectBuildNonMatchesTime =
          stats != null ? stats.getLongStat(Metric.BUILD_COPY_NOMATCH_NANOS) / (1024 * 1024) : -1;
      long unmatchedBuildKeyCount =
          stats != null ? stats.getLongStat(Metric.UNMATCHED_BUILD_KEY_COUNT) : -1;
      long unmatchedProbeCount =
          stats != null ? stats.getLongStat(Metric.UNMATCHED_PROBE_COUNT) : -1;
      long outputCount = stats != null ? stats.getLongStat(Metric.OUTPUT_RECORDS) : -1;

      long totalTime =
          setUpTime
              + pivotProbeTime
              + pivotTime
              + hashComputeTime
              + insertTime
              + slicerTime
              + linkTime
              + spillTime
              + probeListTime
              + probeCopyWatch
              + buildCopyTime
              + probeFindTime
              + projectBuildNonMatchesTime;

      metrics.add(
          new String[] {
            String.valueOf(batchSize),
            String.valueOf(varcharLen),
            String.valueOf(numberOfBuildRows),
            String.valueOf(numberOfProbeRows),
            String.valueOf(numOfBuildCarryOvers),
            String.valueOf(numOfProbeCarryOvers),
            String.valueOf(numOfFixedJoinKeys),
            String.valueOf(numOfVarJoinKeys),
            String.valueOf(joinType.toString()),
            String.valueOf(maxRandomInt),
            String.valueOf(outputCount),
            String.valueOf(unmatchedBuildKeyCount),
            String.valueOf(unmatchedProbeCount),
            String.valueOf(setUpTime),
            String.valueOf(pivotTime),
            String.valueOf(insertTime),
            String.valueOf(hashComputeTime),
            String.valueOf(slicerTime),
            String.valueOf(linkTime),
            String.valueOf(spillTime),
            String.valueOf(pivotProbeTime),
            String.valueOf(probeListTime),
            String.valueOf(probeCopyWatch),
            String.valueOf(buildCopyTime),
            String.valueOf(probeFindTime),
            String.valueOf(projectBuildNonMatchesTime),
            String.valueOf(totalTime),
            exception,
            stackTrace
          });

      writer.writeAll(metrics);
    }
  }
}
