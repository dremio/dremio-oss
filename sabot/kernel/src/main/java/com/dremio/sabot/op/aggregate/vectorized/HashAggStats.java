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

package com.dremio.sabot.op.aggregate.vectorized;

import com.dremio.sabot.exec.context.MetricDef;

/**
 * Stats for {@link VectorizedHashAggOperator} and
 * {@link com.dremio.sabot.op.aggregate.hash.HashAggOperator}
 *
 * VERY IMPORTANT
 * Please add new stats at the end of Metric table and
 * be careful about changing the order of metrics and/or
 * removing metrics. Changes may result in incorrectly
 * rendering old profiles.
 */
public class HashAggStats {

  public static final int SKIP_METRIC_START = 10;

  public enum Metric implements MetricDef {

    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME,
    PIVOT_TIME,
    INSERT_TIME,
    ACCUMULATE_TIME,
    REVERSE_TIME,
    UNPIVOT_TIME,
    VECTORIZED,
    NUM_HASH_PARTITIONS,
    MAX_HASHTABLE_BATCH_SIZE,
    MIN_HASHTABLE_ENTRIES,
    MAX_HASHTABLE_ENTRIES,
    MIN_REHASH_COUNT,
    MAX_REHASH_COUNT,
    PREALLOCATED_MEMORY,      /* total preallocated memory */
    SPILL_COUNT,              /* number of times operator spilled to disk */
    PARTITIONS_SPILLED,       /* total number of partitions spilled */
    ITERATIONS,               /* number of iterations of hash aggregations (initial + recursive) */
    RAN_OUT_OF_MEMORY,        /* number of times operator ran out of memory while processing data */
    SPILL_TIME,               /* cumulative time taken to spill all partitions */
    READ_SPILLED_BATCH_TIME,  /* cumulative time taken to read all spilled batches */
    TOTAL_BATCHES_SPILLED,    /* total number of batches spilled across all spills */
    MAX_BATCHES_SPILLED,      /* maximum number of batches spilled across all spills */
    TOTAL_RECORDS_SPILLED,    /* total number of records spilled across all spills */
    MAX_RECORDS_SPILLED,      /* maximum number of records spilled across all spills */
    RECURSION_DEPTH,          /* recursion depth, 0 (no spilling), 1 (no recursive spilling), >= 2(recursive spilling) */
    TOTAL_SPILLED_DATA_SIZE,  /* total size (in bytes) of data spilled by vectorized hash agg operator */
    MAX_SPILLED_DATA_SIZE,    /* max size (in bytes) of data spilled by vectorized hash agg operator */
    MAX_TOTAL_NUM_BUCKETS,    /* max total capacity in hash tables */
    MAX_TOTAL_NUM_ENTRIES,    /* max total size in hash tables */
    ALLOCATED_FOR_FIXED_KEYS, /* total capacity allocated for fixed block vectors */
    UNUSED_FOR_FIXED_KEYS,      /* unused capacity for fixed block vectors */
    ALLOCATED_FOR_VARIABLE_KEYS, /* total capacity allocated for variable block vectors */
    UNUSED_FOR_VARIABLE_KEYS, /* unused capacity for variable block vectors */
    MAX_VARIABLE_BLOCK_LENGTH, /* maximum amount of data (pivoted keys) that can be stored in variable block vector */
    SPLICE_TIME,               /* total time takes for splicing */
    SPLICE_COUNT,              /* total number of batches spliced */

    // OOB related metrics
    OOB_SENDS, // Number of times operator informed others of spilling
    OOB_RECEIVES, // Number of times operator received a notification of spilling.
    OOB_DROP_LOCAL, // Number of times operator dropped self-referencing spilling notification
    OOB_DROP_WRONG_STATE, // Number of times operator dropped spilling notification as it was in wrong state to spill
    OOB_DROP_UNDER_THRESHOLD, // Number of times OOB dropped spilling notification as it was under the threshold.
    OOB_DROP_NO_VICTIM, // Number of times OOB dropped spilling notification as all allocations were minimal.
    OOB_SPILL, // Spill was done due to oob.
    OOB_DROP_ALREADY_SPILLING // Number of times operator dropped spilling notification as it was already spilling

    ;


    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public static int getCorrectOrdinalForOlderProfiles(final int ordinal) {
    switch (ordinal) {
      case 0:
        return Metric.NUM_BUCKETS.ordinal();
      case 1:
        return Metric.NUM_ENTRIES.ordinal();
      case 2:
        return Metric.NUM_RESIZING.ordinal();
      case 3:
        return Metric.RESIZING_TIME.ordinal();
      case 4:
        return Metric.PIVOT_TIME.ordinal();
      case 5:
        return Metric.INSERT_TIME.ordinal();
      case 6:
        return Metric.ACCUMULATE_TIME.ordinal();
      case 7:
        return Metric.REVERSE_TIME.ordinal();
      case 8:
        return Metric.UNPIVOT_TIME.ordinal();
      case 9:
        return Metric.VECTORIZED.ordinal();
      /* 10 to 20 are skipped as they were earlier join related stats and
       * were never set by HashAggOperator. So serialized profiles
       * never carried their ordinals/ids. The caller of this method
       * first determines if we are working with old v/s new profiles
       * and this method is used specifically for old profiles that
       * will not be carrying ids [10 - 20]
       */
      /* 22 is skipped as it was join related stat */
      case 23:
        return Metric.NUM_HASH_PARTITIONS.ordinal();
      case 24:
        return Metric.MIN_HASHTABLE_ENTRIES.ordinal();
      case 25:
        return Metric.MAX_HASHTABLE_ENTRIES.ordinal();
      case 26:
        return Metric.MIN_REHASH_COUNT.ordinal();
      case 27:
        return Metric.MAX_REHASH_COUNT.ordinal();
      case 28:
        return Metric.PREALLOCATED_MEMORY.ordinal();
      case 29:
        return Metric.SPILL_COUNT.ordinal();
      case 30:
        return Metric.PARTITIONS_SPILLED.ordinal();
      case 31:
        return Metric.ITERATIONS.ordinal();
      case 32:
        return Metric.RAN_OUT_OF_MEMORY.ordinal();
      case 33:
        return Metric.SPILL_TIME.ordinal();
      case 34:
        return Metric.READ_SPILLED_BATCH_TIME.ordinal();
      case 35:
        return Metric.TOTAL_BATCHES_SPILLED.ordinal();
      case 36:
        return Metric.MAX_BATCHES_SPILLED.ordinal();
      case 37:
        return Metric.TOTAL_RECORDS_SPILLED.ordinal();
      case 38:
        return Metric.MAX_RECORDS_SPILLED.ordinal();
      case 39:
        return Metric.RECURSION_DEPTH.ordinal();
      case 40:
        return Metric.TOTAL_SPILLED_DATA_SIZE.ordinal();
      case 41:
        return Metric.MAX_SPILLED_DATA_SIZE.ordinal();
      default:
        throw new IllegalStateException("Unexpected metric ID for Vectorized HashAgg");
    }
  }
}
