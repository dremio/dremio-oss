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
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST;

import com.dremio.common.logical.data.Order;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.sabot.FieldInfo.SortOrder;
import com.dremio.sabot.exec.context.HeapAllocatedMXBeanWrapper;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.sort.external.ExternalSortOperator;
import com.dremio.sabot.op.sort.external.ExternalSortStats;
import com.dremio.sabot.op.sort.external.ExternalSortStats.Metric;
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
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSortOperatorPerformance extends BaseTestOperator {
  private static final String FILE_PATH = "../";
  private final int numOfUniqueKeysPerBatch;
  private List<FieldInfo> fieldInfos;
  private int numberOfRows;
  private int batchSize;
  private int varcharLen;
  private List<Ordering> orderings;
  private boolean reverse;
  private boolean enableSplaySort;
  private int numOfCarryOvers;
  private int numOfFixedKeyCols;
  private int numOfVarKeyCols;
  private String sortType;
  private int maxRandomInt;
  private String fileName;

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(Duration.ofMinutes(100));

  @Rule public Timeout globalTimeout = Timeout.seconds(60_000); // Increase the timeout as needed

  public TestSortOperatorPerformance(
      List<FieldInfo> fieldInfos,
      int numberOfRows,
      int batchSize,
      int varcharLen,
      List<Ordering> orderings,
      boolean reverse,
      boolean enableSplaySort,
      int numOfCarryOvers,
      int numOfFixedKeyCols,
      int numOfVarKeyCols,
      String sortType,
      int maxRandomInt,
      int numOfUniqueKeysPerBatch,
      String fileName) {
    this.fieldInfos = fieldInfos;
    this.numberOfRows = numberOfRows;
    this.batchSize = batchSize;
    this.varcharLen = varcharLen;
    this.orderings = orderings;
    this.reverse = reverse;
    this.enableSplaySort = enableSplaySort;
    this.numOfCarryOvers = numOfCarryOvers;
    this.numOfFixedKeyCols = numOfFixedKeyCols;
    this.numOfVarKeyCols = numOfVarKeyCols;
    this.sortType = sortType;
    this.maxRandomInt = maxRandomInt;
    this.numOfUniqueKeysPerBatch = numOfUniqueKeysPerBatch;
    this.fileName = fileName;
  }

  @Ignore
  @Test(timeout = 1800000)
  public void testSort() throws Exception {
    try (BatchDataGenerator sdg =
        new BatchDataGenerator(
            fieldInfos, getTestAllocator(), numberOfRows, batchSize, varcharLen, maxRandomInt)) {
      if (enableSplaySort) {
        testContext
            .getOptions()
            .setOption(
                OptionValue.createBoolean(
                    OptionType.SYSTEM,
                    ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT.getOptionName(),
                    true));
      }

      ExternalSort sort =
          new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, orderings, reverse);
      HeapAllocatedMXBeanWrapper.setFeatureSupported(true);
      OperatorStats stats = validateSingle(sort, ExternalSortOperator.class, sdg, null, batchSize);

      writeResultsCSV(
          stats,
          fileName,
          varcharLen,
          numberOfRows / batchSize,
          batchSize,
          numOfFixedKeyCols,
          numOfVarKeyCols,
          numOfCarryOvers,
          sortType,
          numOfUniqueKeysPerBatch,
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
          numOfCarryOvers,
          sortType,
          numOfUniqueKeysPerBatch,
          e.toString(),
          Arrays.toString(e.getStackTrace()));
    }
  }

  /*
  Each method called from this function returns an Object[] where each Object is a value for TestSortOperatorPerformance members in same order as they
  are declared.
  Right now each Object[] returned would contain -
  final int numOfUniqueKeysPerBatch;
  List<FieldInfo> fieldInfos;
  int numberOfRows;
  int batchSize;
  int varcharLen;
  List<Ordering> orderings;
  boolean reverse;
  boolean enableSplaySort;
  int numOfCarryOvers;
  int numOfFixedKeyCols;
  int numOfVarKeyCols;
  String sortType;
  int maxRandomInt;
  String fileName;

  If more parameters are to be added in the future, they should be added as class members and test values for them would accordingly be added in
  following array.

  https://github.com/junit-team/junit4/wiki/Parameterized-tests
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {
    List<Object[]> parameters = new ArrayList<>();
    parameters.addAll(scaleVarcharLenAndNumOfBatches());
    /*parameters.addAll(scaleNumberOfKeyColumns(true, false));
    parameters.addAll(scaleNumberOfKeyColumns(false, true));
    parameters.addAll(scaleStringColLenAndBatchSizes());
    parameters.addAll(scaleBatchSizesWithIntCols());
    parameters.addAll(scaleUniqueKeyValues(false));
    parameters.addAll(scaleNumberOfIntCarryOvers());
    parameters.addAll(scaleUniqueKeyValues(true));*/
    return parameters;
  }

  public static Collection<Object[]> scaleNumberOfIntCarryOvers() throws Exception {
    int batchSize = 4000;
    int numOfBatches = 100;
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "SortscaleIntCarryOvers";
    writeFileHeader(fileName);
    for (int numOfColumns = 822; numOfColumns <= 1000; numOfColumns += 10) {

      parameters.add(
          getParameterList(
              batchSize,
              batchSize * numOfBatches,
              batchSize,
              15,
              false,
              false,
              numOfColumns,
              0,
              2,
              0,
              "Quick",
              2000,
              fileName,
              ASCENDING));
    }
    return parameters;
  }

  public static Collection<Object[]> scaleNumberOfKeyColumns(
      boolean scaleIntKeyCols, boolean scaleStringKeyCols) throws Exception {
    int batchSize = 4000;
    int numOfBatches = 100;
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "scaleIntKeyColumns";
    writeFileHeader(fileName);
    for (int numOfColumns = 4900; numOfColumns <= 10_000; numOfColumns += 100) {
      parameters.add(
          getParameterList(
              batchSize,
              batchSize * numOfBatches,
              batchSize,
              15,
              false,
              false,
              1,
              0,
              scaleIntKeyCols ? numOfColumns : 0,
              scaleStringKeyCols ? numOfColumns : 0,
              "Quick",
              Integer.MAX_VALUE,
              fileName,
              ASCENDING));
    }
    return parameters;
  }

  private static Object[] getParameterList(
      int numOfUniqueKeysPerBatch,
      int numberOfRows,
      int batchSize,
      int varcharLen,
      boolean reverse,
      boolean enableSplaySort,
      int numOfFixedCarryOvers,
      int numOfVarCarryOvers,
      int numOfFixedKeyCols,
      int numOfVarKeyCols,
      String sortType,
      int maxRandomInt,
      String fileName,
      RelFieldCollation.Direction direction) {
    List<FieldInfo> fieldInfos = new ArrayList<>();

    if (numOfFixedKeyCols > 0) {
      fieldInfos.addAll(
          getFieldInfos(
              new ArrowType.Int(32, true),
              numOfUniqueKeysPerBatch,
              SortOrder.RANDOM,
              numOfFixedKeyCols));
    }
    if (numOfVarKeyCols > 0) {
      fieldInfos.addAll(getFieldInfos(new Utf8(), batchSize, SortOrder.RANDOM, numOfVarKeyCols));
    }
    List<String> fieldNames = getFieldNames(fieldInfos);
    if (numOfFixedCarryOvers > 0) {
      fieldInfos.addAll(
          getFieldInfos(
              new ArrowType.Int(32, true),
              numOfUniqueKeysPerBatch,
              SortOrder.RANDOM,
              numOfFixedCarryOvers));
    }
    if (numOfVarKeyCols > 0) {
      fieldInfos.addAll(getFieldInfos(new Utf8(), batchSize, SortOrder.RANDOM, numOfVarCarryOvers));
    }
    List<Ordering> orderings = getOrderings(direction, FIRST, fieldNames);

    return new Object[] {
      fieldInfos,
      numberOfRows,
      batchSize,
      varcharLen,
      orderings,
      reverse,
      enableSplaySort,
      numOfFixedCarryOvers + numOfVarCarryOvers,
      numOfFixedKeyCols,
      numOfVarKeyCols,
      sortType,
      maxRandomInt,
      numOfUniqueKeysPerBatch,
      fileName
    };
  }

  // done
  public static Collection<Object[]> scaleBatchSizesWithIntCols() throws Exception {
    int numOfVarKeyColumns = 0;
    int numOfCarryOvers = 2;
    int varcharSize = 15;
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "scaleBatchSizesWithIntKeys";
    writeFileHeader(fileName);
    int dataSize = 320000000;

    for (int numOfFixedKeyColumns = 1; numOfFixedKeyColumns < 100; numOfFixedKeyColumns += 5) {
      for (int batchSize = 100; batchSize <= 8000; batchSize += 100) {
        int numOfBatches = dataSize / (batchSize * (numOfFixedKeyColumns + numOfCarryOvers) * 8);
        parameters.add(
            getParameterList(
                batchSize / 2,
                batchSize * numOfBatches,
                batchSize,
                15,
                false,
                false,
                2,
                0,
                numOfFixedKeyColumns,
                0,
                "Quick",
                Integer.MAX_VALUE,
                fileName,
                ASCENDING));
      }
    }
    return parameters;
  }

  public static Collection<Object[]> scaleStringColLenAndBatchSizes() throws Exception {
    int numOfVarKeyColumns = 1;
    int numOfFixedKeyColumns = 1;
    int numOfCarryOvers = 2;
    List<Object[]> parameters = new ArrayList<>();

    String fileName = "scaleBatchSizes";
    writeFileHeader(fileName);
    int dataSize = Integer.MAX_VALUE;
    for (int varcharSize = 600; varcharSize <= 5000; varcharSize += 100) {
      for (int batchSize = 100; batchSize <= 8000; batchSize += 100) {
        int numOfBatches = dataSize / batchSize / (varcharSize + 12);

        parameters.add(
            getParameterList(
                batchSize / 2,
                batchSize * numOfBatches,
                batchSize,
                varcharSize,
                false,
                false,
                numOfCarryOvers,
                0,
                numOfFixedKeyColumns,
                numOfVarKeyColumns,
                "Quick",
                Integer.MAX_VALUE,
                fileName,
                ASCENDING));
      }
    }
    return parameters;
  }

  public static Collection<Object[]> scaleVarcharLenAndNumOfBatches() throws Exception {
    int batchSize = 4000;
    int numOfVarKeyColumns = 1;
    int numOfCarryOvers = 1;

    List<Object[]> parameters = new ArrayList<>();

    String fileName = "scaleVarchar";

    for (int varcharSize = 100; varcharSize <= 5_000; varcharSize += 100) {
      for (int numOfBatches = 1000; numOfBatches <= 5000; numOfBatches += 100) {
        parameters.add(
            getParameterList(
                batchSize,
                numOfBatches * batchSize,
                batchSize,
                varcharSize,
                false,
                false,
                numOfCarryOvers,
                0,
                0,
                numOfVarKeyColumns,
                "Quick",
                Integer.MAX_VALUE,
                fileName,
                ASCENDING));
      }
    }
    return parameters;
  }

  public static Collection<Object[]> scaleNumberOfBatchesRandomKeys() throws Exception {
    int batchSize = 4000;
    String fileName = "ScaleNumberOfBatchesRandomKeys";

    List<Object[]> parameters = new ArrayList<>();

    for (int numOfBatches = 11600; numOfBatches <= 15_000; numOfBatches += 500) {
      parameters.add(
          getParameterList(
              batchSize,
              batchSize * numOfBatches,
              batchSize,
              15,
              false,
              false,
              1,
              1,
              1,
              1,
              "Quick",
              4000,
              fileName,
              ASCENDING));
    }
    return parameters;
  }

  public static Collection<Object[]> scaleNumberOfBatchesAscKeys() throws Exception {
    int batchSize = 4000;
    String fileName = "ScaleNumberOfBatchesAscKeys";

    List<Object[]> parameters = new ArrayList<>();
    writeFileHeader(fileName);

    for (int numOfBatches = 9100; numOfBatches <= 15_000; numOfBatches += 500) {
      /*
      int numOfUniqueKeysPerBatch,
      int numberOfRows,
      int batchSize,
      int varcharLen,
      boolean reverse,
      boolean enableSplaySort,
      int numOfFixedCarryOvers,
      int numOfVarCarryOvers,
      int numOfFixedKeyCols,
      int numOfVarKeyCols,
      String sortType,
      int maxRandomInt,
      String fileName,
      RelFieldCollation.Direction direction
       */
      parameters.add(
          getParameterList(
              batchSize,
              batchSize * numOfBatches,
              batchSize,
              15,
              false,
              false,
              1,
              1,
              1,
              1,
              "Quick",
              4000,
              fileName,
              ASCENDING));
    }
    return parameters;
  }

  public static Collection<Object[]> scaleUniqueKeyValues(boolean isQuickSort) throws Exception {
    int batchSize = 4000;
    int numOfBatches = 2000;
    String fileName = "scaleUniqueKeyValuesSplaySortSmallRange";
    int numOfIntKeyCols = 2;
    writeFileHeader(fileName);

    List<Object[]> parameters = new ArrayList<>();
    for (int numOfUniqueValues = 1; numOfUniqueValues < 100; numOfUniqueValues++) {
      List<FieldInfo> fieldInfos = new ArrayList<>();

      parameters.add(
          getParameterList(
              numOfUniqueValues,
              batchSize * numOfBatches,
              batchSize,
              15,
              false,
              true,
              1,
              0,
              numOfIntKeyCols,
              0,
              isQuickSort ? "Quick" : "Splay",
              Integer.MAX_VALUE,
              fileName,
              ASCENDING));
    }
    return parameters;
  }

  private static List<Ordering> getOrderings(
      RelFieldCollation.Direction direction,
      RelFieldCollation.NullDirection nullDirection,
      List<String> fieldNames) {
    List<Ordering> orderings = new ArrayList<>();
    for (String fieldName : fieldNames) {
      orderings.add(new Order.Ordering(direction, parseExpr(fieldName), nullDirection));
    }
    return orderings;
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
            "No. of Carry Overs",
            "No. of Fixed Key Cols",
            "No. of Var Key Cols",
            "No. of Batches",
            "Sort Type",
            "No of Unique Sort Keys",
            "Setup Time",
            "Consume Time",
            "No More To Consume Time",
            "Output Time",
            "Total Time",
            "Avg Heap Allocation",
            "Peak Heap Allocation",
            "No. of Batches Spilled",
            "Spill Time Nanos",
            "Merge Time Nanos",
            "Spilled Data Size",
            "Spill Copy Nanos",
            "Exception",
            "Exception Stack Trace"
          });
      writer.writeAll(header);
    }
  }

  private void writeResultsCSV(
      OperatorStats stats,
      String fileName,
      int varcharLen,
      int numOfBatches,
      int batchSize,
      int numOfFixedKeyColumns,
      int numOfVarKeyColumns,
      int numOfCarryOverColumns,
      String sortType,
      int numOfUniqueKeysPerBatch,
      String exception,
      String stackTrace)
      throws IOException {
    String filePath = FILE_PATH + fileName + ".csv";
    System.out.println("The results will be saved to: " + filePath);
    try (CSVWriter writer = new CSVWriter(new FileWriter(filePath, true))) {
      List<String[]> metrics = new ArrayList<>();

      long setUpTime =
          stats != null ? stats.getLongStat(ExternalSortStats.Metric.SETUP_MILLIS) : -1;
      long consumeTime =
          stats != null ? stats.getLongStat(ExternalSortStats.Metric.CAN_CONSUME_MILLIS) : -1;
      long noMoreToConsumeTime =
          stats != null ? stats.getLongStat(Metric.NO_MORE_TO_CONSUME_MILLIS) : -1;
      long outputTime =
          stats != null ? stats.getLongStat(ExternalSortStats.Metric.CAN_PRODUCE_MILLIS) : -1;
      long averageHeapAllocated = stats != null ? stats.getAverageHeapAllocated() : -1;
      long peakHeapAllocated = stats != null ? stats.getPeakHeapAllocated() : -1;

      long numOfBatchesSpilled = stats != null ? stats.getLongStat(Metric.BATCHES_SPILLED) : -1;

      long spillTimeNanos = stats != null ? stats.getLongStat(Metric.SPILL_TIME_NANOS) : -1;
      long mergeTimeNanos = stats != null ? stats.getLongStat(Metric.MERGE_TIME_NANOS) : -1;
      long spilledDataSize = stats != null ? stats.getLongStat(Metric.TOTAL_SPILLED_DATA_SIZE) : -1;
      long spillCopyNanos = stats != null ? stats.getLongStat(Metric.SPILL_COPY_NANOS) : -1;

      metrics.add(
          new String[] {
            String.valueOf(batchSize),
            String.valueOf(varcharLen),
            String.valueOf(numOfCarryOverColumns),
            String.valueOf(numOfFixedKeyColumns),
            String.valueOf(numOfVarKeyColumns),
            String.valueOf(numOfBatches),
            sortType,
            String.valueOf(numOfUniqueKeysPerBatch),
            String.valueOf(setUpTime),
            String.valueOf(consumeTime),
            String.valueOf(noMoreToConsumeTime),
            String.valueOf(outputTime),
            String.valueOf(setUpTime + consumeTime + noMoreToConsumeTime + outputTime),
            String.valueOf(averageHeapAllocated),
            String.valueOf(peakHeapAllocated),
            String.valueOf(numOfBatchesSpilled),
            String.valueOf(spillTimeNanos),
            String.valueOf(mergeTimeNanos),
            String.valueOf(spilledDataSize),
            String.valueOf(spillCopyNanos),
            exception,
            stackTrace
          });

      writer.writeAll(metrics);
    }
  }
}
