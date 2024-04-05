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
package com.dremio;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;
import static com.dremio.exec.expr.TypeHelper.getValueVectorClass;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.util.DremioGetObject;
import com.dremio.exec.HyperVectorValueIterator;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.HyperVectorWrapper;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.joda.time.Period;
import org.junit.Assert;

/**
 * An object to encapsulate the options for a Dremio unit test, as well as the execution methods to
 * perform the tests and validation of results.
 *
 * <p>To construct an instance easily, look at the TestBuilder class. From an implementation of the
 * BaseTestQuery class, and instance of the builder is accessible through the testBuilder() method.
 */
public class DremioTestWrapper {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioTestWrapper.class);

  // TODO - when in JSON, read baseline in all text mode to avoid precision loss for decimal values

  // This flag will enable all of the values that are validated to be logged. For large validations
  // this is time consuming
  // so this is not exposed in a way that it can be enabled for an individual test. It can be
  // changed here while debugging
  // a test to see all of the output, but as this framework is doing full validation, there is no
  // reason to keep it on as
  // it will only make the test slower.
  private static boolean VERBOSE_DEBUG = false;

  // Unit test doesn't expect any specific batch count
  public static final int EXPECTED_BATCH_COUNT_NOT_SET = -1;

  public static final int MAX_SAMPLE_RECORDS_TO_PRINT_ON_FAILURE = 20;

  // The motivation behind the TestBuilder was to provide a clean API for test writers. The model is
  // mostly designed to
  // prepare all of the components necessary for running the tests, before the TestWrapper is
  // initialized. There is however
  // one case where the setup for the baseline is driven by the test query results, and this is
  // implicit type enforcement
  // for the baseline data. In this case there needs to be a call back into the TestBuilder once we
  // know the type information
  // from the test query.
  private TestBuilder testBuilder;

  /** Test query to run. Type of object depends on the {@link #queryType} */
  private Object query;

  // The type of query provided
  private UserBitShared.QueryType queryType;
  // The type of query provided for the baseline
  private UserBitShared.QueryType baselineQueryType;
  // should ordering be enforced in the baseline check
  private boolean ordered;
  private BufferAllocator allocator;
  // queries to run before the baseline or test queries, can be used to set options
  private String baselineOptionSettingQueries;
  private String testOptionSettingQueries;
  // two different methods are available for comparing ordered results, the default reads all of the
  // records
  // into giant lists of objects, like one giant on-heap batch of 'vectors'
  // this flag enables the other approach which iterates through a hyper batch for the test query
  // results and baseline
  // while this does work faster and use less memory, it can be harder to debug as all of the
  // elements are not in a
  // single list
  private boolean highPerformanceComparison;
  // if the baseline is a single option test writers can provide the baseline values and columns
  // without creating a file, these are provided to the builder in the baselineValues() and
  // baselineColumns() methods
  // and translated into a map in the builder
  private List<Map<String, Object>> baselineRecords;

  private static Map<String, BaselineValuesForTDigest> baselineValuesForTDigestMap;
  private static Map<String, BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap;

  private int expectedNumBatches;

  private TestResult latestResult;

  public DremioTestWrapper(
      TestBuilder testBuilder,
      BufferAllocator allocator,
      Object query,
      QueryType queryType,
      String baselineOptionSettingQueries,
      String testOptionSettingQueries,
      QueryType baselineQueryType,
      boolean ordered,
      boolean highPerformanceComparison,
      List<Map<String, Object>> baselineRecords,
      int expectedNumBatches,
      Map<String, BaselineValuesForTDigest> baselineValuesForTDigestMap,
      Map<String, BaselineValuesForItemsSketch> baselineValuesForItemsSketchMap) {
    this.testBuilder = testBuilder;
    this.allocator = allocator;
    this.query = query;
    this.queryType = queryType;
    this.baselineQueryType = baselineQueryType;
    this.ordered = ordered;
    this.baselineOptionSettingQueries = baselineOptionSettingQueries;
    this.testOptionSettingQueries = testOptionSettingQueries;
    this.highPerformanceComparison = highPerformanceComparison;
    this.baselineRecords = baselineRecords;
    this.expectedNumBatches = expectedNumBatches;
    this.baselineValuesForTDigestMap = baselineValuesForTDigestMap;
    this.baselineValuesForItemsSketchMap = baselineValuesForItemsSketchMap;
  }

  public TestResult run() throws Exception {
    if (testBuilder.getExpectedSchema() != null) {
      compareSchemaOnly();
    } else {
      if (ordered) {
        compareOrderedResults();
      } else {
        compareUnorderedResults();
      }
    }
    return latestResult;
  }

  private BufferAllocator getAllocator() {
    return allocator;
  }

  private void compareHyperVectors(
      Map<String, HyperVectorValueIterator> expectedRecords,
      Map<String, HyperVectorValueIterator> actualRecords)
      throws Exception {
    for (String s : expectedRecords.keySet()) {
      assertNotNull("Expected column '" + s + "' not found.", actualRecords.get(s));
      assertEquals(
          expectedRecords.get(s).getTotalRecords(), actualRecords.get(s).getTotalRecords());
      HyperVectorValueIterator expectedValues = expectedRecords.get(s);
      HyperVectorValueIterator actualValues = actualRecords.get(s);
      int i = 0;
      while (expectedValues.hasNext()) {
        compareValuesErrorOnMismatch(expectedValues.next(), actualValues.next(), i, s);
        i++;
      }
    }
    cleanupHyperValueIterators(expectedRecords.values());
    cleanupHyperValueIterators(actualRecords.values());
  }

  private void cleanupHyperValueIterators(Collection<HyperVectorValueIterator> hyperBatches) {
    for (HyperVectorValueIterator hvi : hyperBatches) {
      for (ValueVector vv : hvi.getHyperVector().getValueVectors()) {
        vv.clear();
      }
    }
  }

  public static void compareMergedVectors(
      Map<String, List<Object>> expectedRecords, Map<String, List<Object>> actualRecords)
      throws Exception {
    validateColumnSets(expectedRecords, actualRecords);
    for (String s : actualRecords.keySet()) {
      List<?> expectedValues = expectedRecords.get(s);
      List<?> actualValues = actualRecords.get(s);
      assertEquals(
          String.format(
              "Incorrect number of rows returned by query.\nquery: %s\nexpected: %s\nactual: %s",
              s, expectedValues, actualValues),
          expectedValues.size(),
          actualValues.size());

      for (int i = 0; i < expectedValues.size(); i++) {
        try {
          compareValuesErrorOnMismatch(expectedValues.get(i), actualValues.get(i), i, s);
        } catch (Exception ex) {
          throw new Exception(
              ex.getMessage() + "\n\n" + printNearbyRecords(expectedRecords, actualRecords, i), ex);
        }
      }
    }
    if (actualRecords.size() < expectedRecords.size()) {
      throw new Exception(findMissingColumns(expectedRecords.keySet(), actualRecords.keySet()));
    }
  }

  private static void validateColumnSets(
      Map<String, List<Object>> expectedRecords, Map<String, List<Object>> actualRecords) {
    Set<String> expectedKeys = expectedRecords.keySet();
    Set<String> actualKeys = actualRecords.keySet();
    if (!expectedKeys.equals(actualKeys)) {
      fail(
          String.format(
              "Incorrect keys, expected:(%s) actual:(%s)",
              String.join(",", expectedKeys), String.join(",", actualKeys)));
    }
  }

  private static String printNearbyRecords(
      Map<String, List<Object>> expectedRecords,
      Map<String, List<Object>> actualRecords,
      int offset) {
    StringBuilder expected = new StringBuilder();
    StringBuilder actual = new StringBuilder();
    expected.append("Expected Records near verification failure:\n");
    actual.append("Actual Records near verification failure:\n");
    int firstRecordToPrint = Math.max(0, offset - 5);
    List<?> expectedValuesInFirstColumn =
        expectedRecords.get(expectedRecords.keySet().iterator().next());
    List<?> actualValuesInFirstColumn =
        expectedRecords.get(expectedRecords.keySet().iterator().next());
    int numberOfRecordsToPrint =
        Math.min(
            Math.min(20, expectedValuesInFirstColumn.size()), actualValuesInFirstColumn.size());
    for (int i = firstRecordToPrint; i < numberOfRecordsToPrint; i++) {
      expected.append("Record Number: ").append(i).append(" { ");
      actual.append("Record Number: ").append(i).append(" { ");
      for (String s : actualRecords.keySet()) {
        List<?> actualValues = actualRecords.get(s);
        actual.append(s).append(" : ").append(actualValues.get(i)).append(",");
      }
      for (String s : expectedRecords.keySet()) {
        List<?> expectedValues = expectedRecords.get(s);
        expected.append(s).append(" : ").append(expectedValues.get(i)).append(",");
      }
      expected.append(" }\n");
      actual.append(" }\n");
    }

    return expected.append("\n\n").append(actual).toString();
  }

  private Map<String, HyperVectorValueIterator> addToHyperVectorMap(
      final List<QueryDataBatch> records, final RecordBatchLoader loader)
      throws SchemaChangeException, UnsupportedEncodingException {
    // TODO - this does not handle schema changes
    Map<String, HyperVectorValueIterator> combinedVectors = new TreeMap<>();

    long totalRecords = 0;
    QueryDataBatch batch;
    int size = records.size();
    for (int i = 0; i < size; i++) {
      batch = records.get(i);
      loader.load(batch.getHeader().getDef(), batch.getData());
      logger.debug(
          "reading batch with "
              + loader.getRecordCount()
              + " rows, total read so far "
              + totalRecords);
      totalRecords += loader.getRecordCount();
      for (VectorWrapper<?> w : loader) {
        String field = SchemaPath.getSimplePath(w.getField().getName()).toExpr();
        if (!combinedVectors.containsKey(field)) {
          ValueVector[] vvList =
              (ValueVector[]) Array.newInstance(getValueVectorClass(w.getField()), 1);
          vvList[0] = w.getValueVector();
          combinedVectors.put(
              field,
              new HyperVectorValueIterator(
                  w.getField(), new HyperVectorWrapper<>(w.getField(), vvList)));
        } else {
          combinedVectors.get(field).getHyperVector().addVector(w.getValueVector());
        }
      }
    }
    for (HyperVectorValueIterator hvi : combinedVectors.values()) {
      hvi.determineTotalSize();
    }
    return combinedVectors;
  }

  private static class BatchIterator implements Iterable<VectorAccessible>, AutoCloseable {
    private final List<QueryDataBatch> dataBatches;
    private final RecordBatchLoader batchLoader;

    public BatchIterator(List<QueryDataBatch> dataBatches, RecordBatchLoader batchLoader) {
      this.dataBatches = dataBatches;
      this.batchLoader = batchLoader;
    }

    @Override
    public Iterator<VectorAccessible> iterator() {
      return new Iterator<VectorAccessible>() {

        int index = -1;

        @Override
        public boolean hasNext() {
          return index < dataBatches.size() - 1;
        }

        @Override
        public VectorAccessible next() {
          index++;
          if (index == dataBatches.size()) {
            throw new RuntimeException("Tried to call next when iterator had no more items.");
          }
          batchLoader.clear();
          QueryDataBatch batch = dataBatches.get(index);
          try {
            batchLoader.load(batch.getHeader().getDef(), batch.getData());
          } catch (SchemaChangeException e) {
            throw new RuntimeException(e);
          }
          return batchLoader;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Removing is not supported");
        }
      };
    }

    @Override
    public void close() throws Exception {
      batchLoader.clear();
    }
  }

  private static Object getVectorObject(ValueVector vector, int index) {
    return DremioGetObject.getObject(vector, index);
  }

  /**
   * @param batches
   * @return
   * @throws SchemaChangeException
   * @throws UnsupportedEncodingException
   */
  public static Map<String, List<Object>> addToCombinedVectorResults(
      Iterable<VectorAccessible> batches)
      throws SchemaChangeException, UnsupportedEncodingException {
    // TODO - this does not handle schema changes
    Map<String, List<Object>> combinedVectors = new TreeMap<>();

    long totalRecords = 0;
    BatchSchema schema = null;
    for (VectorAccessible loader : batches) {
      // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
      // SchemaChangeException, so check/clean throws clause above.
      if (schema == null) {
        schema = loader.getSchema();
        for (Field mf : schema) {
          combinedVectors.put(SchemaPath.getSimplePath(mf.getName()).toExpr(), new ArrayList<>());
        }
      } else {
        // TODO - actually handle schema changes, this is just to get access to the
        // SelectionVectorMode
        // of the current batch, the check for a null schema is used to only mutate the schema once
        // need to add new vectors and null fill for previous batches? distinction between null and
        // non-existence important?
        schema = loader.getSchema();
      }
      logger.debug(
          "reading batch with "
              + loader.getRecordCount()
              + " rows, total read so far "
              + totalRecords);
      totalRecords += loader.getRecordCount();
      for (VectorWrapper<?> w : loader) {
        String field = SchemaPath.getSimplePath(w.getField().getName()).toExpr();
        ValueVector[] vectors;
        if (w.isHyper()) {
          vectors = w.getValueVectors();
        } else {
          vectors = new ValueVector[] {w.getValueVector()};
        }
        SelectionVector2 sv2 = null;
        SelectionVector4 sv4 = null;
        switch (schema.getSelectionVectorMode()) {
          case TWO_BYTE:
            sv2 = loader.getSelectionVector2();
            break;
          case FOUR_BYTE:
            sv4 = loader.getSelectionVector4();
            break;
        }
        if (sv4 != null) {
          for (int j = 0; j < sv4.getCount(); j++) {
            int complexIndex = sv4.get(j);
            int batchIndex = complexIndex >> 16;
            int recordIndexInBatch = complexIndex & 65535;
            Object obj = getVectorObject(vectors[batchIndex], recordIndexInBatch);
            if (obj != null) {
              if (obj instanceof Text) {
                obj = obj.toString();
              }
            }
            combinedVectors.get(field).add(obj);
          }
        } else {
          for (ValueVector vv : vectors) {
            for (int j = 0; j < loader.getRecordCount(); j++) {
              int index;
              if (sv2 != null) {
                index = sv2.getIndex(j);
              } else {
                index = j;
              }
              Object obj = getVectorObject(vv, index);
              if (obj != null) {
                if (obj instanceof Text) {
                  obj = obj.toString();
                }
              }
              combinedVectors.get(field).add(obj);
            }
          }
        }
      }
    }
    return combinedVectors;
  }

  protected void compareSchemaOnly() throws Exception {
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    List<QueryDataBatch> actual = null;
    try {
      actual = runQueryAndGetResults();
      QueryDataBatch batch = actual.get(0);
      loader.load(batch.getHeader().getDef(), batch.getData());

      final BatchSchema schema = loader.getSchema();
      final List<Pair<SchemaPath, MajorType>> expectedSchema = testBuilder.getExpectedSchema();
      if (schema.getFieldCount() != expectedSchema.size()) {
        throw new Exception(
            String.format(
                "Expected and actual numbers of columns do not match. Expected: %s, Actual: %s.",
                expectedSchema, schema));
      }

      for (int i = 0; i < schema.getFieldCount(); ++i) {
        final String actualSchemaPath = schema.getColumn(i).getName();
        final MinorType actualMinorType = getMinorTypeForArrowType(schema.getColumn(i).getType());

        final String expectedSchemaPath = expectedSchema.get(i).getLeft().getAsUnescapedPath();
        final MinorType expectedMinorType =
            getArrowMinorType(expectedSchema.get(i).getValue().getMinorType());

        if (!actualSchemaPath.equals(expectedSchemaPath)
            || !actualMinorType.equals(expectedMinorType)) {
          throw new Exception(
              String.format(
                  "Schema path or type mismatch for column #%d:\n"
                      + "Expected schema path: %s\nActual   schema path: %s\nExpected type: %s\nActual   type: %s",
                  i, expectedSchemaPath, actualSchemaPath, expectedMinorType, actualMinorType));
        }
      }

    } finally {
      if (actual != null) {
        for (QueryDataBatch batch : actual) {
          try {
            batch.release();
          } catch (final Exception e) {
            logger.error("Failed to release query output batch");
          }
        }
      }
      loader.clear();
    }
  }

  /**
   * Use this method only if necessary to validate one query against another. If you are just
   * validating against a baseline file use one of the simpler interfaces that will write the
   * validation query for you.
   *
   * @throws Exception
   */
  protected void compareUnorderedResults() throws Exception {
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());

    List<QueryDataBatch> actual = Collections.emptyList();
    List<QueryDataBatch> expected = Collections.emptyList();
    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    List<Map<String, Object>> actualRecords = new ArrayList<>();

    try {
      actual = runQueryAndGetResults();
      checkNumBatches(actual);

      addTypeInfoIfMissing(actual.get(0), testBuilder);
      addToMaterializedResults(actualRecords, actual, loader);

      // If baseline data was not provided to the test builder directly, we must run a query for the
      // baseline, this includes
      // the cases where the baseline is stored in a file.
      if (baselineRecords == null) {
        BaseTestQuery.test(baselineOptionSettingQueries);
        expected =
            BaseTestQuery.testRunAndReturn(baselineQueryType, testBuilder.getValidationQuery());
        addToMaterializedResults(expectedRecords, expected, loader);
      } else {
        expectedRecords = baselineRecords;
      }

      compareResults(expectedRecords, actualRecords);
    } finally {
      cleanupBatches(actual, expected);
    }
  }

  /**
   * Use this method only if necessary to validate one query against another. If you are just
   * validating against a baseline file use one of the simpler interfaces that will write the
   * validation query for you.
   *
   * @throws Exception
   */
  protected void compareOrderedResults() throws Exception {
    if (highPerformanceComparison) {
      if (baselineQueryType == null) {
        throw new Exception(
            "Cannot do a high performance comparison without using a baseline file");
      }
      compareResultsHyperVector();
    } else {
      compareMergedOnHeapVectors();
    }
  }

  private List<QueryDataBatch> runQueryAndGetResults() throws Exception {
    BaseTestQuery.test(testOptionSettingQueries);
    List<QueryDataBatch> actual = BaseTestQuery.testRunAndReturn(queryType, query);
    latestResult = new TestResult(actual.get(0).getHeader());
    return actual;
  }

  public void compareMergedOnHeapVectors() throws Exception {
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    BatchSchema schema = null;

    List<QueryDataBatch> actual = Collections.emptyList();
    List<QueryDataBatch> expected = Collections.emptyList();
    Map<String, List<Object>> actualSuperVectors;
    Map<String, List<Object>> expectedSuperVectors;

    try {
      actual = runQueryAndGetResults();
      checkNumBatches(actual);

      // To avoid extra work for test writers, types can optionally be inferred from the test query
      addTypeInfoIfMissing(actual.get(0), testBuilder);

      BatchIterator batchIter = new BatchIterator(actual, loader);
      actualSuperVectors = addToCombinedVectorResults(batchIter);
      batchIter.close();

      // If baseline data was not provided to the test builder directly, we must run a query for the
      // baseline, this includes
      // the cases where the baseline is stored in a file.
      if (baselineRecords == null) {
        BaseTestQuery.test(baselineOptionSettingQueries);
        expected =
            BaseTestQuery.testRunAndReturn(baselineQueryType, testBuilder.getValidationQuery());
        BatchIterator exBatchIter = new BatchIterator(expected, loader);
        expectedSuperVectors = addToCombinedVectorResults(exBatchIter);
        exBatchIter.close();
      } else {
        // data is built in the TestBuilder in a row major format as it is provided by the user
        // translate it here to vectorized, the representation expected by the ordered comparison
        expectedSuperVectors = translateRecordListToHeapVectors(baselineRecords);
      }

      compareMergedVectors(expectedSuperVectors, actualSuperVectors);
    } catch (Exception e) {
      throw new Exception(e.getMessage() + "\nFor query: " + query, e);
    } finally {
      cleanupBatches(expected, actual);
    }
  }

  public static Map<String, List<Object>> translateRecordListToHeapVectors(
      List<Map<String, Object>> records) {
    Map<String, List<Object>> ret = new TreeMap<>();
    for (String s : records.get(0).keySet()) {
      ret.put(s, new ArrayList<>());
    }
    for (Map<String, Object> m : records) {
      for (String s : m.keySet()) {
        ret.get(s).add(m.get(s));
      }
    }
    return ret;
  }

  public void compareResultsHyperVector() throws Exception {
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());

    List<QueryDataBatch> results = runQueryAndGetResults();

    checkNumBatches(results);

    // To avoid extra work for test writers, types can optionally be inferred from the test query
    addTypeInfoIfMissing(results.get(0), testBuilder);

    Map<String, HyperVectorValueIterator> actualSuperVectors = addToHyperVectorMap(results, loader);

    BaseTestQuery.test(baselineOptionSettingQueries);
    List<QueryDataBatch> expected =
        BaseTestQuery.testRunAndReturn(baselineQueryType, testBuilder.getValidationQuery());

    Map<String, HyperVectorValueIterator> expectedSuperVectors =
        addToHyperVectorMap(expected, loader);

    compareHyperVectors(expectedSuperVectors, actualSuperVectors);
    cleanupBatches(results, expected);
  }

  private void checkNumBatches(final List<QueryDataBatch> results) {
    if (expectedNumBatches != EXPECTED_BATCH_COUNT_NOT_SET) {
      final int actualNumBatches = results.size();
      assertEquals(
          String.format(
              "Expected %d batches but query returned %d non empty batch(es)%n",
              expectedNumBatches, actualNumBatches),
          expectedNumBatches,
          actualNumBatches);
    } else {
      if (results.isEmpty() && !baselineRecords.isEmpty()) {
        Assert.fail("No records returned.");
      }
    }
  }

  private void addTypeInfoIfMissing(QueryDataBatch batch, TestBuilder testBuilder) {
    if (!testBuilder.typeInfoSet()) {
      Map<SchemaPath, MajorType> typeMap = getTypeMapFromBatch(batch);
      testBuilder.baselineTypes(typeMap);
    }
  }

  private Map<SchemaPath, MajorType> getTypeMapFromBatch(QueryDataBatch batch) {
    Map<SchemaPath, MajorType> typeMap = new TreeMap<>();
    for (int i = 0; i < batch.getHeader().getDef().getFieldCount(); i++) {
      typeMap.put(
          SchemaPath.getSimplePath(
              SerializedFieldHelper.create(batch.getHeader().getDef().getField(i)).getName()),
          batch.getHeader().getDef().getField(i).getMajorType());
    }
    return typeMap;
  }

  @SafeVarargs
  private final void cleanupBatches(List<QueryDataBatch>... results) {
    for (List<QueryDataBatch> resultList : results) {
      for (QueryDataBatch result : resultList) {
        result.release();
      }
    }
  }

  public static void addToMaterializedResults(
      List<Map<String, Object>> materializedRecords,
      List<QueryDataBatch> records,
      RecordBatchLoader loader)
      throws SchemaChangeException, UnsupportedEncodingException {
    long totalRecords = 0;
    QueryDataBatch batch;
    int size = records.size();
    for (int i = 0; i < size; i++) {
      batch = records.get(0);
      loader.load(batch.getHeader().getDef(), batch.getData());
      // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
      // SchemaChangeException, so check/clean throws clause above.
      logger.debug(
          "reading batch with "
              + loader.getRecordCount()
              + " rows, total read so far "
              + totalRecords);
      totalRecords += loader.getRecordCount();
      for (int j = 0; j < loader.getRecordCount(); j++) {
        Map<String, Object> record = new LinkedHashMap<>();
        for (VectorWrapper<?> w : loader) {
          Object obj = getVectorObject(w.getValueVector(), j);
          if (obj != null) {
            if (obj instanceof Text) {
              obj = obj.toString();
            }
            record.put(SchemaPath.getSimplePath(w.getField().getName()).toExpr(), obj);
          }
          record.put(SchemaPath.getSimplePath(w.getField().getName()).toExpr(), obj);
        }
        materializedRecords.add(record);
      }
      records.remove(0);
      batch.release();
      loader.clear();
    }
  }

  public static boolean compareValuesErrorOnMismatch(
      Object expected, Object actual, int counter, String column) throws Exception {

    if (compareValues(expected, actual, counter, column)) {
      return true;
    }
    if (expected == null) {
      throw new Exception(
          "at position "
              + counter
              + " column '"
              + column
              + "' mismatched values, expected: null "
              + "but received "
              + actual
              + "("
              + actual.getClass().getSimpleName()
              + ")");
    }
    if (actual == null) {
      throw new Exception(
          "unexpected null at position "
              + counter
              + " column '"
              + column
              + "' should have been:  "
              + expected);
    }
    if (actual instanceof byte[]) {
      throw new Exception(
          "at position "
              + counter
              + " column '"
              + column
              + "' mismatched values, expected: "
              + new String((byte[]) expected, UTF_8)
              + " but received "
              + new String((byte[]) actual, UTF_8));
    }
    if (!expected.equals(actual)) {
      throw new Exception(
          String.format(
              "at position %d column '%s' mismatched values, \nexpected (%s):\n\n%s\n\nbut received (%s):\n\n%s\n\n"
                  + "Hints:"
                  + "\n (1) Results are actually wrong"
                  + "\n (2) If results 'look right', then check if integer type is 'long' and not 'int' (so 1 is 1L)"
                  + "\n",
              counter,
              column,
              expected.getClass().getSimpleName(),
              expected,
              actual.getClass().getSimpleName(),
              actual));
    }
    return true;
  }

  public static boolean compareValues(Object expected, Object actual, int counter, String column)
      throws Exception {
    if (expected == null) {
      if (actual == null) {
        if (VERBOSE_DEBUG) {
          logger.debug(
              "(1) at position "
                  + counter
                  + " column '"
                  + column
                  + "' matched value:  "
                  + expected);
        }
        return true;
      } else {
        return false;
      }
    }
    if (actual == null) {
      return false;
    }
    if (baselineValuesForTDigestMap != null && baselineValuesForTDigestMap.get(column) != null) {
      if (!approximatelyEqualFromTDigest(
          expected, actual, baselineValuesForTDigestMap.get(column))) {
        return false;
      } else {
        if (VERBOSE_DEBUG) {
          logger.debug(
              "at position " + counter + " column '" + column + "' matched value:  " + expected);
        }
      }
      return true;
    }
    if (baselineValuesForItemsSketchMap != null
        && baselineValuesForItemsSketchMap.get(column) != null) {
      if (!verifyItemsSketchValues(actual, baselineValuesForItemsSketchMap.get(column))) {
        return false;
      } else {
        if (VERBOSE_DEBUG) {
          logger.debug(
              "at position " + counter + " column '" + column + "' matched value:  " + expected);
        }
      }
      return true;
    }
    if (actual instanceof byte[]) {
      if (!Arrays.equals((byte[]) expected, (byte[]) actual)) {
        return false;
      } else {
        if (VERBOSE_DEBUG) {
          logger.debug(
              "at position "
                  + counter
                  + " column '"
                  + column
                  + "' matched value "
                  + new String((byte[]) expected, UTF_8));
        }
        return true;
      }
    }
    if (actual instanceof Double && expected instanceof Double) {
      double actualValue = ((Double) actual).doubleValue();
      double expectedValue = ((Double) expected).doubleValue();
      return actual.equals(expected)
          || (actualValue == 0
              ? Math.abs(actualValue - expectedValue) < 0.0000001
              : Math.abs((actualValue - expectedValue) / actualValue) < 0.0000001);
    }
    if (actual instanceof Float && expected instanceof Float) {
      float actualValue = ((Float) actual).floatValue();
      float expectedValue = ((Float) expected).floatValue();
      return actual.equals(expected)
          || (actualValue == 0
              ? Math.abs(actualValue - expectedValue) < 0.0000001
              : Math.abs((double) (actualValue - expectedValue) / actualValue) < 0.0000001);
    }
    if (actual instanceof Period && expected instanceof Period) {
      // joda Period should be compared after normalized
      Period actualValue = ((Period) actual).normalizedStandard();
      Period expectedValue = ((Period) expected).normalizedStandard();
      return actualValue.equals(expectedValue);
    }
    if (actual instanceof BigDecimal && expected instanceof BigDecimal) {
      // equals returns false for 2.0 and 2.00.
      // hard to test for values with differing number of digits in the input file.
      return ((BigDecimal) actual).compareTo((BigDecimal) expected) == 0;
    }
    if (!expected.equals(actual)) {
      return false;
    } else {
      if (VERBOSE_DEBUG) {
        logger.debug(
            "at position " + counter + " column '" + column + "' matched value:  " + expected);
      }
    }

    return true;
  }

  private static boolean approximatelyEqualFromTDigest(
      Object expected, Object actual, BaselineValuesForTDigest value) {
    if (expected instanceof Double) {

      if (!(actual instanceof byte[])) {
        return false;
      }
      ByteBuffer buffer = ByteBuffer.wrap((byte[]) actual);
      com.tdunning.math.stats.TDigest tDigest =
          com.tdunning.math.stats.MergingDigest.fromBytes(buffer);
      double out = tDigest.quantile(value.quartile);
      return Math.abs(((Double) expected - (Double) out) / (Double) expected) <= value.tolerance;
    }
    return false;
  }

  private static boolean verifyItemsSketchValues(
      Object actual, BaselineValuesForItemsSketch value) {
    if (!(actual instanceof byte[])) {
      return false;
    }

    final ItemsSketch sketch = ItemsSketch.getInstance(Memory.wrap((byte[]) actual), value.serde);
    if (value.heavyHitters != null) {
      int size = value.heavyHitters.size();
      ItemsSketch.Row[] rows1 = sketch.getFrequentItems(5, ErrorType.NO_FALSE_NEGATIVES);
      for (int i = 0; i < rows1.length; i++) {
        if (!rows1[i].getItem().equals(value.heavyHitters.get(i))) {
          return false;
        }
      }
      ItemsSketch.Row[] rows2 = sketch.getFrequentItems(size, ErrorType.NO_FALSE_NEGATIVES);
      for (int i = 0; i < rows2.length; i++) {
        if (!rows2[i].getItem().equals(value.heavyHitters.get(i))) {
          return false;
        }
      }
    }
    if (value.counts != null) {
      for (Pair<Object, Long> count : value.counts) {
        Object key = count.getLeft();
        Long val = count.getRight();
        return ((double) Math.abs(sketch.getEstimate(key) - val)) / ((double) val) <= 0.01;
      }
    }
    return true;
  }

  /**
   * Compare two result sets, ignoring ordering.
   *
   * @param expectedRecords - list of records from baseline
   * @param actualRecords - list of records from test query, WARNING - this list is destroyed in
   *     this method
   * @throws Exception
   */
  public static void compareResults(
      List<Map<String, Object>> expectedRecords, List<Map<String, Object>> actualRecords)
      throws Exception {

    if (expectedRecords.size() != actualRecords.size()) {

      String expectedRecordExamples = serializeRecordExamplesToString(expectedRecords);
      String actualRecordExamples = serializeRecordExamplesToString(actualRecords);
      throw new AssertionError(
          String.format(
              "Different number of records returned - expected:<%d> but was:<%d>\n\n"
                  + "Some examples of expected records:\n%s\n\n Some examples of records returned by the test query:\n%s",
              expectedRecords.size(),
              actualRecords.size(),
              expectedRecordExamples,
              actualRecordExamples));
    }

    int i;
    int counter = 0;
    boolean found;
    for (Map<String, Object> expectedRecord : expectedRecords) {
      i = 0;
      found = false;
      findMatch:
      for (Map<String, Object> actualRecord : actualRecords) {
        for (String s : actualRecord.keySet()) {
          if (!expectedRecord.containsKey(s)) {
            throw new AssertionError(
                "Unexpected column '"
                    + s
                    + "' returned by query.\n"
                    + "got: "
                    + actualRecord.keySet()
                    + "\n"
                    + "expected: "
                    + expectedRecord.keySet());
          }
          if (!compareValues(expectedRecord.get(s), actualRecord.get(s), counter, s)) {
            i++;
            continue findMatch;
          }
        }
        if (actualRecord.size() < expectedRecord.size()) {
          throw new Exception(findMissingColumns(expectedRecord.keySet(), actualRecord.keySet()));
        }
        found = true;
        break;
      }
      if (!found) {
        String expectedRecordExamples = serializeRecordExamplesToString(expectedRecords);
        String actualRecordExamples = serializeRecordExamplesToString(actualRecords);
        throw new Exception(
            String.format(
                "After matching %d records, did not find expected record in result set:\n %s\n\n"
                    + "Some examples of expected records:\n%s\n\n Some examples of records returned by the test query:\n%s",
                counter,
                printRecord(expectedRecord),
                expectedRecordExamples,
                actualRecordExamples));
      } else {
        actualRecords.remove(i);
        counter++;
      }
    }
    assertEquals(0, actualRecords.size());
  }

  private static String serializeRecordExamplesToString(List<Map<String, Object>> records) {
    StringBuilder sb = new StringBuilder();
    for (int recordDisplayCount = 0;
        recordDisplayCount < MAX_SAMPLE_RECORDS_TO_PRINT_ON_FAILURE
            && recordDisplayCount < records.size();
        recordDisplayCount++) {
      sb.append(printRecord(records.get(recordDisplayCount)));
    }
    return sb.toString();
  }

  private static String findMissingColumns(Set<String> expected, Set<String> actual) {
    String missingCols = "";
    for (String colName : expected) {
      if (!actual.contains(colName)) {
        missingCols += colName + ", ";
      }
    }
    return "Expected column(s) " + missingCols + " not found in result set: " + actual + ".";
  }

  private static String printRecord(Map<String, ?> record) {
    String ret = "";
    for (String s : record.keySet()) {
      ret += s + " : " + record.get(s) + ", ";
    }
    return ret + "\n";
  }

  public static class BaselineValuesForTDigest {
    public double tolerance;
    public double quartile;

    public BaselineValuesForTDigest(double tolerance, double quartile) {
      this.tolerance = tolerance;
      this.quartile = quartile;
    }
  }

  public static class BaselineValuesForItemsSketch {
    public List<Pair<Object, Long>> counts = null;
    public List<Object> heavyHitters = null;
    public ArrayOfItemsSerDe serde;

    public BaselineValuesForItemsSketch(
        List<Pair<Object, Long>> counts, List<Object> heavyHitters, ArrayOfItemsSerDe serde) {
      this.counts = counts;
      this.heavyHitters = heavyHitters;
      this.serde = serde;
    }
  }
}
