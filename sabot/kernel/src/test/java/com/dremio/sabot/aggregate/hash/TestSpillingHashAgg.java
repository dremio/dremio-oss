/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.CustomHashAggDataGenerator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggSpillStats;
import com.dremio.test.UserExceptionMatcher;

public class TestSpillingHashAgg extends BaseTestOperator {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(1000, TimeUnit.SECONDS);

  private HashAggregate getHashAggregate() {
    return new HashAggregate(null,
                             Arrays.asList(n("FIXKEY1"), n("FIXKEY2"), n("VARKEY1")),
                             Arrays.asList(n("sum(MEASURE1)", "SUM_M1"),
                                           n("min(MEASURE1)", "MIN_M1"),
                                           n("max(MEASURE1)", "MAX_M1"),
                                           n("sum(MEASURE2)", "SUM_M2"),
                                           n("min(MEASURE2)", "MIN_M2"),
                                           n("max(MEASURE2)", "MAX_M2"),
                                           n("sum(MEASURE3)", "SUM_M3"),
                                           n("min(MEASURE3)", "MIN_M3"),
                                           n("max(MEASURE3)", "MAX_M3"),
                                           n("sum(MEASURE4)", "SUM_M4"),
                                           n("min(MEASURE4)", "MIN_M4"),
                                           n("max(MEASURE4)", "MAX_M4")
                             ),
                             true,
                             1f);
  }

  /**
   * Test no spilling
   * @throws Exception
   */
  @Test
  public void testNoSpill() throws Exception {
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(4_000_000);
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
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true);
           AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
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
   * Test failure during operator setup when provided memory constraints
   * are lower than the memory required for preallocating data structures.
   * @throws Exception
   */
  @Test
  public void testSetupFailureForHashTableInit() throws Exception {
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(2_000_000);
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
      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true);
          AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
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
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(2_100_000);
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
      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true);
          AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
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
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(2_300_000);
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

      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true);
          AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
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
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(2_450_000);
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

      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2000, getTestAllocator(), true);
           AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
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
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(4_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        /* 2 distinct partitions spilled twice and no recursive spilling */
        assertEquals(4, stats.getSpills());
        assertEquals(2, stats.getOoms());
        assertEquals(3, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
      }
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false);
           AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        /* 4 distinct partitions spilled twice and no recursive spilling */
        assertEquals(4, stats.getSpills());
        assertEquals(2, stats.getOoms());
        assertEquals(3, stats.getIterations());
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

  private HashAggregate getHashAggregateWithCount() {
    return new HashAggregate(null,
                             Arrays.asList(n("FIXKEY1"), n("FIXKEY2"), n("VARKEY1")),
                             Arrays.asList(n("sum(MEASURE1)", "SUM_M1"),
                                           n("min(MEASURE1)", "MIN_M1"),
                                           n("max(MEASURE1)", "MAX_M1"),
                                           n("count(MEASURE1)", "COUNT_M1"),
                                           n("sum(MEASURE2)", "SUM_M2"),
                                           n("min(MEASURE2)", "MIN_M2"),
                                           n("max(MEASURE2)", "MAX_M2"),
                                           n("count(MEASURE2)", "COUNT_M2"),
                                           n("sum(MEASURE3)", "SUM_M3"),
                                           n("min(MEASURE3)", "MIN_M3"),
                                           n("max(MEASURE3)", "MAX_M3"),
                                           n("count(MEASURE3)", "COUNT_M3"),
                                           n("sum(MEASURE4)", "SUM_M4"),
                                           n("min(MEASURE4)", "MIN_M4"),
                                           n("max(MEASURE4)", "MAX_M4"),
                                           n("count(MEASURE4)", "COUNT_M4")
                             ),
                             true,
                             1f);
  }

  /**
   * Same as (number of rows, memory) previous test but with count accumulator
   * resulting in slightly more spilling
   * @throws Exception
   */
  @Test
  public void testSpill3KWithCount() throws Exception {
    final HashAggregate agg = getHashAggregateWithCount();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(4_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregationsWithCount();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        /* 4 distinct partitions spilled twice and no recursive spilling */
        assertEquals(8, stats.getSpills());
        assertEquals(4, stats.getOoms());
        assertEquals(5, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
      }
      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(3000, getTestAllocator(), true);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false);
          AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregationsWithCount();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 3000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(8, stats.getSpills());
        assertEquals(4, stats.getOoms());
        assertEquals(5, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
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
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(4_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(4000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        /* 6 distinct partitions spilled twice and no recursive spilling */
        assertEquals(12, stats.getSpills());
        assertEquals(6, stats.getOoms());
        assertEquals(7, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
      }
      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(4000, getTestAllocator(), true);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false);
          AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        /* 6 distinct partitions spilled twice and no recursive spilling */
        assertEquals(12, stats.getSpills());
        assertEquals(6, stats.getOoms());
        assertEquals(7, stats.getIterations());
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
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(4_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(20000, getTestAllocator(), true);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        /* all partitions spilled with recursive spilling -- 20K rows with largeVarChars
         * set to true ends up generating some varchar column values of size
         * 10KB-20KB and so per varchar block in hashtable, we can store only few records
         * and thus the request for having gap in ordinals and adding a new batch
         * keeps on increasing. This is why extremely large number of spills with each
         * partition being spilled multiple times and recursive spilling
         */
        assertEquals(447, stats.getSpills());
        assertEquals(381, stats.getOoms());
        assertEquals(73, stats.getIterations());
        assertEquals(2, stats.getRecursionDepth());
      }
      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(20000, getTestAllocator(), true);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false);
          AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(447, stats.getSpills());
        assertEquals(381, stats.getOoms());
        assertEquals(73, stats.getIterations());
        assertEquals(2, stats.getRecursionDepth());
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
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(4_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(100000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(6, stats.getSpills());
        assertEquals(3, stats.getOoms());
        assertEquals(4, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
      }
      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(100000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false);
          AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertEquals(6, stats.getSpills());
        assertEquals(3, stats.getOoms());
        assertEquals(4, stats.getIterations());
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
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(8_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertTrue("expected number of spills out of range", 24 <= stats.getSpills() && stats.getSpills() <= 27);
        assertTrue("expected number of ooms out of range", 16 <= stats.getOoms() && stats.getOoms() <= 19);
        assertEquals(9, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
      }
      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false);
          AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertTrue("expected number of spills out of range", 24 <= stats.getSpills() && stats.getSpills() <= 27);
        assertTrue("expected number of ooms out of range", 16 <= stats.getOoms() && stats.getOoms() <= 19);
        assertEquals(9, stats.getIterations());
        assertEquals(1, stats.getRecursionDepth());
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
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(8_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertTrue("expected number of spills out of range", 77 <= stats.getSpills() && stats.getSpills() <= 80);
        assertTrue("expected number of ooms out of range", 55 <= stats.getOoms() && stats.getOoms() <= 58);
        assertEquals(25, stats.getIterations());
        assertEquals(2, stats.getRecursionDepth());
      }
      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(2_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS, false);
          AutoCloseable withInsertionSort = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION, true)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
        assertTrue("expected number of spills out of range", 77 <= stats.getSpills() && stats.getSpills() <= 80);
        assertTrue("expected number of ooms out of range", 55 <= stats.getOoms() && stats.getOoms() <= 58);
        assertEquals(25, stats.getIterations());
        assertEquals(2, stats.getRecursionDepth());
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
  public void testSpillWithDifferentAllocationThresholds() throws Exception {
    final HashAggregate agg = getHashAggregate();
    agg.setInitialAllocation(1_000_000);
    agg.setMaxAllocation(8_000_000);
    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
           AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 16*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
      }

      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 32*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
      }

      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 64*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
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
        final VectorizedHashAggSpillStats stats = agg.getSpillStats();
      }

      try(CustomHashAggDataGenerator generator = new CustomHashAggDataGenerator(1_000_000, getTestAllocator(), false);
          AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX, 512*1024)) {
        Fixtures.Table table = generator.getExpectedGroupsAndAggregations();
        validateSingle(agg, VectorizedHashAggOperator.class, generator, table, 2000);
      }
    }
  }
}
