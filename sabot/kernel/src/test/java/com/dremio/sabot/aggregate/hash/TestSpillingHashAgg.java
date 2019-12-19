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
package com.dremio.sabot.aggregate.hash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.CustomHashAggDataGenerator;
import com.dremio.sabot.CustomHashAggDataGeneratorDecimal;
import com.dremio.sabot.CustomHashAggDataGeneratorLargeAccum;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggSpillStats;
import com.dremio.test.AllocatorRule;
import com.dremio.test.UserExceptionMatcher;

public class TestSpillingHashAgg extends BaseTestOperator {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(1000, TimeUnit.SECONDS);

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  private HashAggregate getHashAggregate(long reserve, long max, int hashTableBatchSize) {
    OpProps props = PROPS.cloneWithNewReserve(reserve).cloneWithMemoryExpensive(true);
    props.setMemLimit(max);
    return new HashAggregate(props, null,
                             Arrays.asList(n("INT_KEY"), n("BIGINT_KEY"), n("VARCHAR_KEY"),
                                           n("FLOAT_KEY"), n("DOUBLE_KEY"), n("BOOLEAN_KEY"), n("DECIMAL_KEY")),
                             Arrays.asList(n("sum(INT_MEASURE)", "SUM_INT"),
                                           n("min(INT_MEASURE)", "MIN_INT"),
                                           n("max(INT_MEASURE)", "MAX_INT"),
                                           n("sum(BIGINT_MEASURE)", "SUM_BIGINT"),
                                           n("min(BIGINT_MEASURE)", "MIN_BIGINT"),
                                           n("max(BIGINT_MEASURE)", "MAX_BIGINT"),
                                           n("sum(FLOAT_MEASURE)", "SUM_FLOAT"),
                                           n("min(FLOAT_MEASURE)", "MIN_FLOAT"),
                                           n("max(FLOAT_MEASURE)", "MAX_FLOAT"),
                                           n("sum(DOUBLE_MEASURE)", "SUM_DOUBLE"),
                                           n("min(DOUBLE_MEASURE)", "MIN_DOUBLE"),
                                           n("max(DOUBLE_MEASURE)", "MAX_DOUBLE"),
                                           n("sum(DECIMAL_MEASURE)", "SUM_DECIMAL"),
                                           n("min(DECIMAL_MEASURE)", "MIN_DECIMAL"),
                                           n("max(DECIMAL_MEASURE)", "MAX_DECIMAL")
                             ),
                             true,
                             true,
                             1f,
                              hashTableBatchSize);
  }

  private HashAggregate getHashAggregateWithLargeAccum(long reserve, long max, int hashTableBatchSize, int numAccum) {
    OpProps props = PROPS.cloneWithNewReserve(reserve).cloneWithMemoryExpensive(true);
    props.setMemLimit(max);
    List<NamedExpression> aggExpr = new ArrayList<>();
    for (int i = 0; i < numAccum; ++i) {
      aggExpr.add(n("sum(INT_MEASURE_" + i + " )", "SUM_INT"));
    }

    return new HashAggregate(props, null,
      Arrays.asList(n("INT_KEY"), n("BIGINT_KEY"), n("VARCHAR_KEY"),
        n("FLOAT_KEY"), n("DOUBLE_KEY"), n("BOOLEAN_KEY"), n("DECIMAL_KEY")),
      aggExpr,
      true,
      true,
      1f,
      hashTableBatchSize);
  }

  private HashAggregate getHashAggregate(long reserve, long max) {
    return getHashAggregate(reserve, max, 3968);
  }

  private HashAggregate getHashAggregateDecimal(long reserve, long max, int hashTableBatchSize) {
    OpProps props = PROPS.cloneWithNewReserve(reserve).cloneWithMemoryExpensive(true);
    props.setMemLimit(max);
    return new HashAggregate(props, null,
      Arrays.asList(n("DECIMAL_KEY")),
      Arrays.asList(n("sum(DECIMAL_MEASURE)", "SUM_DECIMAL"),
        n("min(DECIMAL_MEASURE)", "MIN_DECIMAL"),
        n("max(DECIMAL_MEASURE)", "MAX_DECIMAL"),
        n("$sum0(DECIMAL_MEASURE)", "SUM0_DECIMAL")
      ),
      true,
      true,
      1f,
      hashTableBatchSize);
  }

  /**
   * Test no spilling
   * @throws Exception
   */
  @Test
  public void testNoSpill() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 12_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(0, stats.getSpills());
        assertEquals(0, stats.getOoms());
        assertEquals(1, stats.getIterations());
        assertEquals(0, stats.getRecursionDepth());
      }
    }
  }

  /**
   * Tests with varchar key of length > 32k
   * @throws Exception
   */
  @Test
  public void testVeryLargeVarcharKey() throws Exception {
    HashAggregate agg = getHashAggregate(1_000_000, 12_000_000);

    boolean exceptionThrown = false;


    final int shortLen = (120 * 1024);
    final int largeLen = (128 * 1024);

    //shortLen size must not fail, any subsequent largeLen inserts would fail
    boolean shortLenSuccess = false;
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1000, getTestAllocator(), shortLen)) {
        try (CustomHashAggDataGenerator generator1 = new CustomHashAggDataGenerator(1000, getTestAllocator(), largeLen)) {
          Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
          validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 1000);

          //shortLen key size should have worked. must reach here!
          shortLenSuccess = true;

          //add some largeLen keys
          Fixtures.Table table1 = generator1.getExpectedGroupsAndAggregations();
          validateSingle(agg, VectorizedHashAggOperator.class, generator1, table1, 1000);

          //must not reach here
          Assert.assertEquals(0, 1);
        }
      }
    } catch (StringIndexOutOfBoundsException userExp) {
      exceptionThrown = true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      Assert.assertEquals(true, shortLenSuccess);
      Assert.assertEquals(true, exceptionThrown);
    }

    //fails to pivot with largeLen key size
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1000, getTestAllocator(), largeLen)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 1000);

        //must not reach here
        Assert.assertEquals(0, 1);
      }
    } catch (StringIndexOutOfBoundsException userExp) {
      exceptionThrown = true;
    } finally {
      Assert.assertEquals(true, exceptionThrown);
    }

    //passes largeLen key size with increased batch size.
    agg = getHashAggregate(1_000_000, 24_000_000, 3968 * 2);
    exceptionThrown = false;
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
        try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1000, getTestAllocator(), largeLen);
             AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES, 2048 * 1024);
             AutoCloseable options2 = with(ExecConstants.TARGET_BATCH_RECORDS_MAX, 8192)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 1000);
      }
    } catch (Exception e) {
      System.err.println("Exception message: " + e.getMessage());
      e.printStackTrace();
      exceptionThrown = true;
    } finally {
      Assert.assertEquals(false, exceptionThrown);
    }

    //should fail with 1MB key size
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1000, getTestAllocator(), (1024 * 1024));
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES, 2048 * 1024);
           AutoCloseable options2 = with(ExecConstants.TARGET_BATCH_RECORDS_MAX, 8192)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 1000);
      }
    } catch (StringIndexOutOfBoundsException userExp) {
      exceptionThrown = true;
    } finally {
      Assert.assertEquals(true, exceptionThrown);
    }
  }

  /**
   * Test failure during operator setup when provided memory constraints
   * are lower than the memory required for preallocating data structures.
   * @throws Exception
   */
  @Test
  public void testSetupFailureForHashTableInit() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 2_100_000);
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
                                                    "Query was cancelled because it exceeded the memory limits set by the administrator.",
                                                    VectorizedHashAggOperator.PREALLOC_FAILURE_PARTITIONS));
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(0, stats.getSpills());
        assertEquals(0, stats.getOoms());
        assertEquals(1, stats.getIterations());
        assertEquals(0, stats.getRecursionDepth());
      }
    }
  }

  @Test
  public void testSetupFailureForPreallocation() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 5_000_000);
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
                                                    "Query was cancelled because it exceeded the memory limits set by the administrator.",
                                                    VectorizedHashAggOperator.PREALLOC_FAILURE_PARTITIONS));
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(0, stats.getSpills());
        assertEquals(0, stats.getOoms());
        assertEquals(1, stats.getIterations());
        assertEquals(0, stats.getRecursionDepth());
      }
    }
  }

  @Test
  public void testSetupFailureForExtraPartition() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 8_800_000);
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
                                                    "Query was cancelled because it exceeded the memory limits set by the administrator.",
                                                    VectorizedHashAggOperator.PREALLOC_FAILURE_LOADING_PARTITION));
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(0, stats.getSpills());
        assertEquals(0, stats.getOoms());
        assertEquals(1, stats.getIterations());
        assertEquals(0, stats.getRecursionDepth());
      }
    }
  }

  @Test
  public void testSetupFailureForAuxStructures() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 9_900_000);
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.OUT_OF_MEMORY,
                                                    "Query was cancelled because it exceeded the memory limits set by the administrator.",
                                                    VectorizedHashAggOperator.PREALLOC_FAILURE_AUX_STRUCTURES));
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(0, stats.getSpills());
        assertEquals(0, stats.getOoms());
        assertEquals(1, stats.getIterations());
        assertEquals(0, stats.getRecursionDepth());
      }
    }
  }

  /*
   * Note on the usage of ExecConstants.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS
   * The algorithm that picks up victim partitions to spill first targets the set of spilled
   * partitions to see if there is a suitable candidate. This helps in keeping the total
   * number of unique partitions spilled to minimum. In other words, it helps to prevent
   * situations where all partitions are spilled and we are not left with anything in
   * memory. This also means that we may end up choosing a spilled partition as the next
   * victim partition where there is another non-spilled partition having the potential
   * to release more memory. Thus there are chances of hitting slightly higher OOMs
   * with this approach.
   *
   * We saw this behavior in these unit tests where after the victim partition selection
   * algorithm was correctly implemented, the stats for each unit test increased considerably.
   * There was an increase in the number of times we hit OOM but the total number of unique
   * partitions spilled was low.
   *
   * Since we want near-deterministic behavior in these unit tests, we introduced a way
   * to disable the selection algorithm partially. Instead of first looking at the set
   * of spilled partitions, it directly looks at all active partitions and picks
   * the partition with highest memory usage.
   */

  /**
   * Test spill of 3K rows -- no recursive spilling
   * @throws Exception
   */
  @Test
  public void testSpill3K() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 4_000_000, 990);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true);
         AutoCloseable maxHashTableBatchSizeBytes = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES, 128 * 1024)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(12, stats.getSpills());
        assertEquals(6, stats.getOoms());
        assertEquals(7, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
      }
      /* run with allocator limit same as minimum reservation */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
      }
      /* run with micro spilling disabled */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
      }
    }
  }

  @Test
  public void testSpill3KWithLargeAccum() throws Exception {
    final int numAccum = 128;
    final HashAggregate agg = getHashAggregateWithLargeAccum(1_000_000, 4_000_000, 990, numAccum);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true);
         AutoCloseable numpartitions = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_NUMPARTITIONS, 1);
         AutoCloseable maxHashTableBatchSizeBytes = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES, 128 * 1024)) {
      try (CustomHashAggDataGeneratorLargeAccum generator = new CustomHashAggDataGeneratorLargeAccum(8000, getTestAllocator(), numAccum);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        validateSingle(agg, VectorizedHashAggOperator.class, generator, null, 3000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();

        //it must spill
        assertTrue(stats.getSpills() > 0);
      }
    }
  }

  @Test
  public void testSpill50KDecimal() throws Exception {
    final HashAggregate agg = getHashAggregateDecimal(1_000_000, 2_100_000, 990);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true);
         AutoCloseable maxHashTableBatchSizeBytes = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES, 64 * 1024)) {
      try (CustomHashAggDataGeneratorDecimal generator = new CustomHashAggDataGeneratorDecimal
        (50000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator
             .VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false);
           AutoCloseable optionsDecimals = with(PlannerSettings.ENABLE_DECIMAL_V2, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(8, stats.getSpills());
      }
      /* run with allocator limit same as minimum reservation */
      try (CustomHashAggDataGeneratorDecimal generator = new CustomHashAggDataGeneratorDecimal(50000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT, true);
           AutoCloseable optionsDecimals = with(PlannerSettings.ENABLE_DECIMAL_V2, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
      }
      /* run with micro spilling disabled */
      try (CustomHashAggDataGeneratorDecimal generator = new CustomHashAggDataGeneratorDecimal(50000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS, false);
           AutoCloseable optionsDecimals = with(PlannerSettings.ENABLE_DECIMAL_V2, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
      }
    }
  }
  private HashAggregate getHashAggregateWithCount(long reserve, long max, int hashTableBatchSize) {
    OpProps props = PROPS.cloneWithNewReserve(reserve);
    props.setMemLimit(max);
    return new HashAggregate(props, null,
                             Arrays.asList(n("INT_KEY"), n("BIGINT_KEY"), n("VARCHAR_KEY"),
                                           n("FLOAT_KEY"), n("DOUBLE_KEY"), n("BOOLEAN_KEY"), n("DECIMAL_KEY")),
                             Arrays.asList(n("sum(INT_MEASURE)", "SUM_INT"),
                                           n("min(INT_MEASURE)", "MIN_INT"),
                                           n("max(INT_MEASURE)", "MAX_INT"),
                                           n("count(INT_MEASURE)", "COUNT_INT"),
                                           n("sum(BIGINT_MEASURE)", "SUM_BIGINT"),
                                           n("min(BIGINT_MEASURE)", "MIN_BIGINT"),
                                           n("max(BIGINT_MEASURE)", "MAX_BIGINT"),
                                           n("count(BIGINT_MEASURE)", "COUNT_BIGINT"),
                                           n("sum(FLOAT_MEASURE)", "SUM_FLOAT"),
                                           n("min(FLOAT_MEASURE)", "MIN_FLOAT"),
                                           n("max(FLOAT_MEASURE)", "MAX_FLOAT"),
                                           n("count(FLOAT_MEASURE)", "COUNT_FLOAT"),
                                           n("sum(DOUBLE_MEASURE)", "SUM_DOUBLE"),
                                           n("min(DOUBLE_MEASURE)", "MIN_DOUBLE"),
                                           n("max(DOUBLE_MEASURE)", "MAX_DOUBLE"),
                                           n("count(DOUBLE_MEASURE)", "COUNT_DOUBLE"),
                                           n("sum(DECIMAL_MEASURE)", "SUM_DECIMAL"),
                                           n("min(DECIMAL_MEASURE)", "MIN_DECIMAL"),
                                           n("max(DECIMAL_MEASURE)", "MAX_DECIMAL"),
                                           n("count(DECIMAL_MEASURE)", "COUNT_DECIMAL")
                             ),
                             true,
                             true,
                             1f,
                              hashTableBatchSize);
  }

  /**
   * Same as (number of rows, memory) previous test but with count accumulator
   * resulting in slightly more spilling
   * @throws Exception
   */
  @Test
  public void testSpill3KWithCount() throws Exception {
    final HashAggregate agg = getHashAggregateWithCount(1_000_000, 4_000_000, 990);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true);
         AutoCloseable maxHashTableBatchSizeBytes = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES, 128 * 1024)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregationsWithCount();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
      }
      /* run with allocator limit same as minimum reservation */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregationsWithCount();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
      }
      /* run with micro spilling disabled */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregationsWithCount();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
      }
    }
  }

  /**
   * Test spill of 4K rows -- no recursive spilling
   * @throws Exception
   */
  @Test
  public void testSpill4K() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 4_000_000, 990);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true);
         AutoCloseable maxHashTableBatchSizeBytes = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES, 128 * 1024)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(4000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(20, stats.getSpills());
        assertEquals(12, stats.getOoms());
        assertEquals(9, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
      }
      /* run with allocator limit same as minimum reservation */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(4000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
      /* run with micro spilling disabled */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(4000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
    }
  }

  /**
   * Test spilll of 20K rows with very large varchars (10KB-20KB)
   * causing excessive spilling with recursion
   * @throws Exception
   */
  @Test
  public void testSpill20K() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 12_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(20000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        /* all partitions spilled with recursive spilling -- 20K rows with largeVarChar
         * set to true ends up generating some varchar column values of size
         * 10KB-20KB and so per varchar block in hashtable, we can store only few records
         * and thus the request for having gap in ordinals and adding a new batch
         * keeps on increasing. This is why extremely large number of spills with each
         * partition being spilled multiple times and recursive spilling
         */
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertTrue(stats.getSpills() > 0);
      }
      /* run with allocator limit same as minimum reservation */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(20000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
      /* run with micro spilling disabled */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(20000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
    }
  }

  /**
   * Test spill of 100K rows -- reasonably sized varchars so no
   * recursive spilling
   * @throws Exception
   */
  @Test
  public void testSpill100K() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 4_000_000, 990);
      try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true);
           AutoCloseable maxHashTableBatchSizeBytes = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES, 128 * 1024)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(100000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(14, stats.getSpills());
        assertEquals(7, stats.getOoms());
        assertEquals(8, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
      }
      /* run with allocator limit same as minimum reservation */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(100000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
      /* run with micro spilling disabled */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(100000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
    }
  }

  /**
   * Test spill of 1million rows with slightly more memory and no recursive
   * spilling
   * @throws Exception
   */
  @Test
  public void testSpill1M() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 12_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
      }
      /* run with allocator limit same as minimum reservation */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
      /* run with micro spilling disabled */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
    }
  }

  /**
   * Similar to previous test with twice as many rows causing
   * recursive spilling
   * @throws Exception
   */
  @Test
  public void testSpill2M() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 12_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
      /* run with allocator limit same as minimum reservation */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
      /* run with micro spilling disabled */
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
    }
  }

  @Test
  public void testCloseWithoutSetup() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 12_000_000);
    SabotContext context = mock(SabotContext.class);
    try (BufferAllocator allocator = allocatorRule.newAllocator("test-spilling-hashagg", 0, Long.MAX_VALUE)) {
      when(context.getAllocator()).thenReturn(allocator);
      OptionManager optionManager = mock(OptionManager.class);
      try (BufferAllocator alloc = context.getAllocator().newChildAllocator("sample-alloc", 0, Long.MAX_VALUE);
          OperatorContextImpl operatorContext = new OperatorContextImpl(context.getConfig(), alloc, optionManager, 1000);
          final VectorizedHashAggOperator op = new VectorizedHashAggOperator(agg, operatorContext)) {
      }
    }
  }

  @Test
  public void testSpillWithDifferentAllocationThresholds() throws Exception {
    final HashAggregate agg = getHashAggregate(1_000_000, 12_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 16*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }

      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 32*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }

      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 64*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }

      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 128*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }

      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 256*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }

      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 512*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
    }
  }
}
