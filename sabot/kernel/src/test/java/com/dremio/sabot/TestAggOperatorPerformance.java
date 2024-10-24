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

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.util.TestTools;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.sabot.FieldInfo.SortOrder;
import com.dremio.sabot.exec.context.HeapAllocatedMXBeanWrapper;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.aggregate.vectorized.HashAggStats;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.sort.external.ExternalSortStats;
import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestAggOperatorPerformance extends BaseTestOperator {

  private static final String FILE_PATH = "../";
  private List<FieldInfo> fieldInfos;
  private int numberOfRows;
  private int batchSize;
  private int varcharLen;
  private int numOfFixedKeyCols;
  private int numOfVarKeyCols;
  private List<NamedExpression> dim;
  private List<NamedExpression> measure;
  private int fixedSumCount;
  private int varSumCount;
  private int fixedAvgCount;
  private int varAvgCount;
  private int fixedMaxCount;
  private int varMaxCount;
  private int fixedMinCount;
  private int varMinCount;
  private int fixedCntCount;
  private int varCntCount;
  private int numOfUniqueDimensions;
  private int maxRandomInt = Integer.MAX_VALUE;
  private String fileName;

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(Duration.ofMinutes(60));

  public TestAggOperatorPerformance(
      List<FieldInfo> fieldInfos,
      int numberOfRows,
      int batchSize,
      int varcharLen,
      int numOfFixedKeyCols,
      int numOfVarKeyCols,
      List<NamedExpression> dim,
      List<NamedExpression> measure,
      int fixedSumCount,
      int varSumCount,
      int fixedAvgCount,
      int varAvgCount,
      int fixedMaxCount,
      int varMaxCount,
      int fixedMinCount,
      int varMinCount,
      int fixedCntCount,
      int varCntCount,
      int numOfUniqueDimensions,
      String fileName) {
    this.fieldInfos = fieldInfos;
    this.numberOfRows = numberOfRows;
    this.batchSize = batchSize;
    this.varcharLen = varcharLen;
    this.numOfFixedKeyCols = numOfFixedKeyCols;
    this.numOfVarKeyCols = numOfVarKeyCols;
    this.dim = dim;
    this.measure = measure;
    this.fixedSumCount = fixedSumCount;
    this.varSumCount = varSumCount;
    this.fixedAvgCount = fixedAvgCount;
    this.varAvgCount = varAvgCount;
    this.fixedMaxCount = fixedMaxCount;
    this.varMaxCount = varMaxCount;
    this.fixedMinCount = fixedMinCount;
    this.varMinCount = varMinCount;
    this.fixedCntCount = fixedCntCount;
    this.varCntCount = varCntCount;
    this.numOfUniqueDimensions = numOfUniqueDimensions;
    this.fileName = fileName;
  }

  /*
  Each method called from this function returns an Object[] where each Object is a value for TestAggOperatorPerformance members in same order as they
  are declared.
  Right now each Object[] returned would contain -
  List<FieldInfo> fieldInfos,
  int numberOfRows;
  int batchSize;
  int varcharLen;
  int numOfCarryOvers;
  int numOfFixedKeyCols;
  int numOfVarKeyCols;
  List<NamedExpression> dim;
  List<NamedExpression> measure;
  int fixedSumCount;
  int varSumCount;
  int fixedAvgCount;
  int varAvgCount;
  int fixedMaxCount;
  int varMaxCount;
  int fixedMinCount;
  int varMinCount;
  int fixedCntCount;
  int varCntCount;
  int numOfUniqueDimensions;
  int maxRandomInt = Integer.MAX_VALUE;
  String fileName;

  If more parameters are to be added in the future, they should be added as class members and test values for them would accordingly be added in
  following array.

  https://github.com/junit-team/junit4/wiki/Parameterized-tests
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {
    List<Object[]> parameters = new ArrayList<>();
    parameters.addAll(scaleNumberOfMeasures(false, true, true, false));
    /* parameters.addAll(scaleNumberOfMeasures(false, true, false, true));
    parameters.addAll(scaleNumberOfDimensions(true, false));
    parameters.addAll(scaleBatchSizesWithVariousStringKeySizes());
    parameters.addAll(scaleBatchSizesForFixedDataSize());
    parameters.addAll(scaleUniqueDimensions());
    parameters.addAll(scaleNumberOfMeasures(true, false, true, false));
    parameters.addAll(scaleNumberOfDimensions(false, true));*/
    return parameters;
  }

  @Ignore
  @Test
  public void testAgg() throws Exception {
    try (BatchDataGenerator sdg =
        new BatchDataGenerator(
            fieldInfos, allocator, numberOfRows, batchSize, varcharLen, maxRandomInt)) {

      OpProps props = PROPS.cloneWithNewReserve(1_000_000).cloneWithMemoryExpensive(true);

      final HashAggregate agg = new HashAggregate(props, null, dim, measure, true, true, 1f);

      HeapAllocatedMXBeanWrapper.setFeatureSupported(true);
      OperatorStats stats =
          validateSingle(agg, VectorizedHashAggOperator.class, sdg, null, batchSize);

      writeResultsCSV(
          stats,
          fileName,
          varcharLen,
          numberOfRows / batchSize,
          batchSize,
          numOfFixedKeyCols,
          numOfVarKeyCols,
          fixedSumCount,
          varSumCount,
          fixedAvgCount,
          varAvgCount,
          fixedMaxCount,
          varMaxCount,
          fixedMinCount,
          varMinCount,
          fixedCntCount,
          varCntCount,
          numOfUniqueDimensions,
          "",
          "");

    } catch (Exception e) {
      writeResultsCSV(
          null,
          fileName,
          varcharLen,
          numberOfRows / batchSize,
          batchSize,
          numOfFixedKeyCols,
          numOfVarKeyCols,
          fixedSumCount,
          varSumCount,
          fixedAvgCount,
          varAvgCount,
          fixedMaxCount,
          varMaxCount,
          fixedMinCount,
          varMinCount,
          fixedCntCount,
          varCntCount,
          numOfUniqueDimensions,
          e.toString(),
          Arrays.toString(e.getStackTrace()));
    }
  }

  public static Collection<Object[]> scaleNumberOfDimensions(
      boolean scaleIntDimensions, boolean scaleStringDimensions) throws Exception {
    int batchSize = 4000;
    int numOfBatches = 1000;
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "AggScaleStringDimensions";
    writeFileHeader(fileName);
    for (int numOfColumns = 1; numOfColumns <= 500; numOfColumns += 3) {
      parameters.add(
          getParameterList(
              batchSize * numOfBatches,
              batchSize,
              15,
              scaleIntDimensions ? numOfColumns : 0,
              scaleStringDimensions ? numOfColumns : 0,
              1,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              3900,
              fileName));
    }
    return parameters;
  }

  public static Collection<Object[]> scaleUniqueDimensions() throws Exception {
    int batchSize = 4000;
    int numOfBatches = 26000;
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "AggScaleUniqueDimensions";
    writeFileHeader(fileName);

    for (int numOfUniqueValues = 1; numOfUniqueValues <= batchSize; numOfUniqueValues++) {
      parameters.add(
          getParameterList(
              batchSize * numOfBatches,
              batchSize,
              15,
              1,
              0,
              2,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              numOfUniqueValues,
              fileName));
    }
    return parameters;
  }

  private static Object[] getParameterList(
      int numberOfRows,
      int batchSize,
      int varcharLen,
      int numOfFixedKeyCols,
      int numOfVarKeyCols,
      int fixedSumCount,
      int varSumCount,
      int fixedAvgCount,
      int varAvgCount,
      int fixedMaxCount,
      int varMaxCount,
      int fixedMinCount,
      int varMinCount,
      int fixedCntCount,
      int varCntCount,
      int numOfUniqueDimensions,
      String fileName) {
    List<FieldInfo> fieldInfos = new ArrayList<>();
    List<NamedExpression> measures = new ArrayList<>();
    if (fixedSumCount > 0) {
      List<FieldInfo> fixedSumFields =
          getFieldInfos(new ArrowType.Int(32, true), batchSize, SortOrder.RANDOM, fixedSumCount);
      fieldInfos.addAll(fixedSumFields);
      List<String> sumMeasureNames = getFieldNames(fixedSumFields);
      measures.addAll(getMeasureExpressions(sumMeasureNames, "sum"));
    }
    if (fixedAvgCount > 0) {
      List<FieldInfo> fixedAvgFields =
          getFieldInfos(new ArrowType.Int(32, true), batchSize, SortOrder.RANDOM, fixedAvgCount);
      fieldInfos.addAll(fixedAvgFields);
      List<String> avgMeasureNames = getFieldNames(fixedAvgFields);
      measures.addAll(getMeasureExpressions(avgMeasureNames, "avg"));
    }
    if (fixedMaxCount > 0) {
      List<FieldInfo> fixedMaxFields =
          getFieldInfos(new ArrowType.Int(32, true), batchSize, SortOrder.RANDOM, fixedMaxCount);
      fieldInfos.addAll(fixedMaxFields);
      List<String> maxMeasureNames = getFieldNames(fixedMaxFields);
      measures.addAll(getMeasureExpressions(maxMeasureNames, "max"));
    }
    if (fixedMinCount > 0) {
      List<FieldInfo> fixedMinFields =
          getFieldInfos(new ArrowType.Int(32, true), batchSize, SortOrder.RANDOM, fixedMinCount);
      fieldInfos.addAll(fixedMinFields);
      List<String> minMeasureNames = getFieldNames(fixedMinFields);
      measures.addAll(getMeasureExpressions(minMeasureNames, "min"));
    }
    if (fixedCntCount > 0) {
      List<FieldInfo> fixedCntFields =
          getFieldInfos(new ArrowType.Int(32, true), batchSize, SortOrder.RANDOM, fixedCntCount);
      fieldInfos.addAll(fixedCntFields);
      List<String> cntMeasureNames = getFieldNames(fixedCntFields);
      measures.addAll(getMeasureExpressions(cntMeasureNames, "count"));
    }
    if (varMaxCount > 0) {
      List<FieldInfo> varMaxFields =
          getFieldInfos(new Utf8(), batchSize, SortOrder.RANDOM, varMaxCount);
      fieldInfos.addAll(varMaxFields);
      List<String> varMaxMeasureNames = getFieldNames(varMaxFields);
      measures.addAll(getMeasureExpressions(varMaxMeasureNames, "max"));
    }
    if (varMinCount > 0) {
      List<FieldInfo> varMinFields =
          getFieldInfos(new Utf8(), batchSize, SortOrder.RANDOM, varMinCount);
      fieldInfos.addAll(varMinFields);
      List<String> varMinMeasureNames = getFieldNames(varMinFields);
      measures.addAll(getMeasureExpressions(varMinMeasureNames, "min"));
    }
    if (varCntCount > 0) {
      List<FieldInfo> varCntFields =
          getFieldInfos(new Utf8(), batchSize, SortOrder.RANDOM, varCntCount);
      fieldInfos.addAll(varCntFields);
      List<String> varCntMeasureNames = getFieldNames(varCntFields);
      measures.addAll(getMeasureExpressions(varCntMeasureNames, "count"));
    }

    List<FieldInfo> dimensionFields = new ArrayList<>();
    if (numOfFixedKeyCols > 0) {
      dimensionFields.addAll(
          getFieldInfos(
              new ArrowType.Int(32, true),
              numOfUniqueDimensions,
              SortOrder.ASCENDING,
              numOfFixedKeyCols));
    }
    if (numOfVarKeyCols > 0) {
      dimensionFields.addAll(
          getFieldInfos(
              new ArrowType.Utf8(), numOfUniqueDimensions, SortOrder.ASCENDING, numOfVarKeyCols));
    }

    List<String> dimensionNames = getFieldNames(dimensionFields);
    List<NamedExpression> dimensions = getDimensionExpressions(dimensionNames);
    fieldInfos.addAll(dimensionFields);

    return new Object[] {
      fieldInfos,
      numberOfRows,
      batchSize,
      varcharLen,
      numOfFixedKeyCols,
      numOfVarKeyCols,
      dimensions,
      measures,
      fixedSumCount,
      varSumCount,
      fixedAvgCount,
      varAvgCount,
      fixedMaxCount,
      varMaxCount,
      fixedMinCount,
      varMinCount,
      fixedCntCount,
      varCntCount,
      numOfUniqueDimensions,
      fileName
    };
  }

  public static Collection<Object[]> scaleBatchSizesForFixedDataSize() throws Exception {
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "scaleBatchSizesForFixedDataSize";
    writeFileHeader(fileName);
    for (int batchSize = 9200; batchSize <= 10_000; batchSize += 200) {

      long dataSize = 2880000000L;
      int numOfBatches = (int) (dataSize / (16 * batchSize));

      parameters.add(
          getParameterList(
              batchSize * numOfBatches,
              batchSize,
              15,
              2,
              0,
              2,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              0,
              batchSize,
              fileName));
    }
    return parameters;
  }

  public static Collection<Object[]> scaleNumberOfMeasures(
      boolean scaleStringMeasures,
      boolean scaleIntMeasures,
      boolean scaleMinMeasures,
      boolean scaleMaxMeasures)
      throws Exception {
    int batchSize = 4000;
    int numOfBatches = 1000;
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "AggScaleIntMeasuresMin";
    writeFileHeader(fileName);
    for (int numOfColumns = 1; numOfColumns <= 1000; numOfColumns += 3) {
      parameters.add(
          getParameterList(
              batchSize * numOfBatches,
              batchSize,
              15,
              1,
              0,
              0,
              0,
              0,
              0,
              scaleMaxMeasures && scaleIntMeasures ? numOfColumns : 0,
              scaleMaxMeasures && scaleStringMeasures ? numOfColumns : 0,
              scaleMinMeasures && scaleIntMeasures ? numOfColumns : 0,
              scaleStringMeasures && scaleMinMeasures ? numOfColumns : 0,
              0,
              0,
              batchSize / 10,
              fileName));
    }
    return parameters;
  }

  public static Collection<Object[]> scaleBatchSizesWithVariousStringKeySizes() throws Exception {
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "AggScaleBatchSizes";
    writeFileHeader(fileName);
    int dataSize = 320000000;
    for (int varcharSize = 10; varcharSize <= 5000; varcharSize += 50) {
      for (int batchSize = 100; batchSize <= 8000; batchSize += 100) {
        int numOfBatches = dataSize / batchSize / (varcharSize + 12);
        parameters.add(
            getParameterList(
                batchSize * numOfBatches,
                batchSize,
                varcharSize,
                1,
                1,
                0,
                0,
                0,
                0,
                2,
                0,
                0,
                0,
                0,
                0,
                batchSize / 2,
                fileName));
      }
    }
    return parameters;
  }

  private void writeResultsCSV(
      OperatorStats stats,
      String fileName,
      int varcharLen,
      int numOfBatches,
      int batchSize,
      int numOfFixedKeyColumns,
      int numOfVarKeyColumns,
      int fixedSumCount,
      int varSumCount,
      int fixedAvgCount,
      int varAvgCount,
      int fixedMaxCount,
      int varMaxCount,
      int fixedMinCount,
      int varMinCount,
      int fixedCntCount,
      int varCntCount,
      int numOfUniqueDimensions,
      String exception,
      String stackTrace)
      throws IOException {
    String filePath = FILE_PATH + fileName + ".csv";
    System.out.println("The results will be saved to: " + filePath);
    try (CSVWriter writer = new CSVWriter(new FileWriter(filePath, true))) {
      List<String[]> metrics = new ArrayList<>();

      long setUpTime = stats != null ? stats.getLongStat(HashAggStats.Metric.SETUP_MILLIS) : -1;
      long pivotTime = stats != null ? stats.getLongStat(HashAggStats.Metric.PIVOT_TIME) : -1;
      long insertTime = stats != null ? stats.getLongStat(HashAggStats.Metric.INSERT_TIME) : -1;
      long accumTime = stats != null ? stats.getLongStat(HashAggStats.Metric.ACCUMULATE_TIME) : -1;
      long resizingTime = stats != null ? stats.getLongStat(HashAggStats.Metric.RESIZING_TIME) : -1;
      long spliceTime = stats != null ? stats.getLongStat(HashAggStats.Metric.SPLICE_TIME_NS) : -1;
      long forceAccumTime =
          stats != null ? stats.getLongStat(HashAggStats.Metric.FORCE_ACCUM_TIME_NS) : -1;
      long accumCompactTime =
          stats != null ? stats.getLongStat(HashAggStats.Metric.ACCUM_COMPACTS_TIME_NS) : -1;
      long unpivotTime = stats != null ? stats.getLongStat(HashAggStats.Metric.UNPIVOT_TIME) : -1;
      long spillTime = stats != null ? stats.getLongStat(HashAggStats.Metric.SPILL_TIME) : -1;

      long outputTime =
          stats != null ? stats.getLongStat(ExternalSortStats.Metric.CAN_PRODUCE_MILLIS) : -1;
      long averageHeapAllocated = stats != null ? stats.getAverageHeapAllocated() : -1;
      long peakHeapAllocated = stats != null ? stats.getPeakHeapAllocated() : -1;

      long spilledDataSize =
          stats != null ? stats.getLongStat(HashAggStats.Metric.TOTAL_SPILLED_DATA_SIZE) : -1;

      metrics.add(
          new String[] {
            String.valueOf(batchSize),
            String.valueOf(varcharLen),
            String.valueOf(numOfFixedKeyColumns),
            String.valueOf(numOfVarKeyColumns),
            String.valueOf(numOfBatches),
            String.valueOf(fixedSumCount),
            String.valueOf(varSumCount),
            String.valueOf(fixedAvgCount),
            String.valueOf(varAvgCount),
            String.valueOf(fixedMaxCount),
            String.valueOf(varMaxCount),
            String.valueOf(fixedMinCount),
            String.valueOf(varMinCount),
            String.valueOf(fixedCntCount),
            String.valueOf(varCntCount),
            String.valueOf(setUpTime),
            String.valueOf(pivotTime),
            String.valueOf(insertTime),
            String.valueOf(accumTime),
            String.valueOf(accumCompactTime),
            String.valueOf(resizingTime),
            String.valueOf(spliceTime),
            String.valueOf(forceAccumTime),
            String.valueOf(outputTime),
            String.valueOf(unpivotTime),
            String.valueOf(setUpTime + pivotTime + insertTime + accumTime + outputTime),
            String.valueOf(averageHeapAllocated),
            String.valueOf(peakHeapAllocated),
            String.valueOf(spillTime),
            String.valueOf(spilledDataSize),
            String.valueOf(numOfUniqueDimensions),
            exception,
            stackTrace
          });

      writer.writeAll(metrics);
    }
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
            "No. of Fixed Key Cols",
            "No. of Var Key Cols",
            "No. of Batches",
            "Fixed Sum Accumulators",
            "Var Sum Accumulators",
            "Fixed Avg Accumulators",
            "Var Avg Accumulators",
            "Fixed Max Accumulators",
            "Var Max Accumulators",
            "Fixed Min Accumulators",
            "Var Min Accumulators",
            "Fixed Cnt Accumulators",
            "Var Cnt Accumulators",
            "Setup Time",
            "Pivot Time",
            "Insert Time",
            "Accum Time",
            "Accum Compact Time",
            "Resize Time",
            "Splice Time",
            "Force Accum Time",
            "Output Time",
            "Unpivot Time",
            "Total Time",
            "Avg Heap Allocation",
            "Peak Heap Allocation",
            "Spill Time Nanos",
            "Spilled Data Size",
            "No. of Unique Dimensions",
            "Exception",
            "Exception Stack Trace"
          });
      writer.writeAll(header);
    }
  }

  private static List<NamedExpression> getMeasureExpressions(
      List<String> fieldNames, String aggregate) {
    List<NamedExpression> expressions = new ArrayList<>();
    for (String fieldName : fieldNames) {
      expressions.add(n(aggregate + "(" + fieldName + ")", aggregate + "_" + fieldName));
    }
    return expressions;
  }

  private static List<NamedExpression> getDimensionExpressions(List<String> fieldNames) {
    List<NamedExpression> expressions = new ArrayList<>();
    for (String fieldName : fieldNames) {
      expressions.add(n(fieldName));
    }
    return expressions;
  }
}
