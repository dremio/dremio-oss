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
package com.dremio.sabot.op.aggregate.vectorized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.dremio.options.TypeValidators.PowerOfTwoLongValidator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.SimpleBigIntVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.google.common.base.Preconditions;
import com.dremio.sabot.op.common.ht2.BlockChunk;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashComputation;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.common.ht2.BoundedPivots;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggPartitionSpillHandler.SpilledPartitionIterator;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggDebug.HashAggErrorType;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.aggregate.vectorized.HashAggStats.Metric;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.koloboke.collect.hash.HashConfig;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;


/******************************************************************************
 *
 * GENERAL NOTES ON HASH AGGREGATION ALGORITHM:
 *
 * The incoming batch for VectorizedHashAgg operator has 2 kinds of columns.
 * First is the GROUP BY key columns and the rest are Aggregation columns
 * needed to run SUM, SUM0, MIN, MAX, AVG, COUNT agg operations.
 *
 * During setup(), we first create a pivot definition using the GROUP BY
 * expressions. As part of this, we also add corresponding output vector
 * to the outgoing. A pair of vector <input vector, output vector> in
 * pivot definition represents a FieldVectorPair and a list of such field
 * vector pairs makes up the pivot definition. Note that pivot definition
 * is for GROUP BY key columns.
 *
 * In consumeData(), we first create a temporary pivot space. We then pivot
 * the columnar data to a row-wise representation of keys into the
 * temporary pivot space using the pivot definition created earlier.
 *
 * We then compute hash for the entire batch of data -- hash computation
 * is done on the GROUP BY key columns.
 *
 * For each incoming record, we use the corresponding hash value from
 * previous step and hash partition the dataset. For each record,
 * target partition number is computed and the pivoted data (from temp
 * pivot space) is then inserted into the hash table of corresponding
 * target partition. At this point, we record some metadata for each
 * record as we are partitioning and inserting into different hash
 * tables. For each record, we store a tuple of form:
 *
 *  <partition number(1), hash table insertion ordinal(4), record index(4)>
 *
 * The tuple is 9 bytes in size and they are as many tuples as the number
 * of records in the incoming batch.
 *
 * The tuples are created because we need to create a mapping between
 * records and the corresponding partitions and this helps in doing the
 * accumulation on a per-partition basis.
 *
 * We then do an in-place rewrite of the tuples such that information for
 * each partition is segregated. Then for each partition, we accumulate
 * the values using corresponding accumulators.
 *
 * During setup phase earlier, in addition to creating pivot definition,
 * we also create accumulators for each partition. Accumulator is also
 * a pair of vector <input, output> where the latter one is added to
 * the outgoing vector container and stores the accumulated results
 * from the values in input vector. Since accumulators are created
 * per partition, we ensure that output vector for a particular type
 * of accumulator is added exactly once to the outgoing container.
 *
 * Later during outputData(), each accumulator just does a vector transfer
 * from it's accumulator vector to the corresponding output vector in the
 * outgoing container.
 *
 * Similarly, we unpivot the GROUP BY key columns from the pivoted space
 * (row-wise representation of keys) into corresponding output vector
 * in the outgoing container.
 *
 * NOTES ON INTERNAL STATE MACHINE FOR HANDLING RECURSIVE SPILLING
 *
 * External State of operator: (nothing has changed)
 * DONE,
 * NEEDS_SETUP,
 * CAN_CONSUME
 * CAN_PRODUCE
 *
 * Internal States:
 * NONE,
 * OUTPUT_INMEMORY_PARTITIONS,
 * PROCESS_SPILLED_PARTITION,
 * TRANSITION_PARTITION_SPILL_STATE,
 * DONE
 *
 * When we are done with consuming all the incoming data from source, we
 * are in CAN_PRODUCE state since the source would have invoked
 * noMoreToConsume() on the operator.
 *
 *
 *
 *
 *        source --------->noMoreToConsume()-----if hash table empty-------->DONE
 *                               |
 *                               |
 *                               |
 *                               |
 *                               |
 *                               V
 *                    OUTPUT_INMEMORY_PARTITIONS
 *
 *
 *   noMoreToConsume() above would have set the operator's external state to
 *   CAN_PRODUCE and now we will expect calls into outputData() until we
 *   set the state to DONE. The following state machine diagram attempts to
 *   show the processing inside outputData and the transition through different
 *   states as the algorithm recurses.
 *
 *
 *                           +------------------------------------------------+
 *   outputData():           |                                                |
 *                           V                                                |
 *      OUTPUT_INMEMORY_PARTITIONS<-----------------+                         |
 *          |         |        |                    |                         |
 *          |         |        |     (output partitions not spilled      (all spilled batches read,
 *          |         |        |        one batch at a time)                  start output)
 *          |         |        |____________________|                         |
 *          |         |                                                       |
 *          |         +------------->TRANSITION_PARTITION_SPILL_STATE         |
 *          |                                       |                         |
 *     (no spilled partition                        |                         |
 *       to process)                                |                         |
 *          |                                       |                         |
 *          |                                       |     +-------------------+
 *          |                                       |     |
 *          |                                       |     |
 *          |                                       V     |
 *          |                           PROCESS_SPILLED_PARTITION<------+
 *          |                                       |                   |
 *          |                                       |       (read a single batch and process
 *          |                                       |         by feeding into the operator)
 *          V                                       |___________________|
 *        DONE
 *
 *************************************************************************/

@Options
public class VectorizedHashAggOperator implements SingleInputOperator {

  public static final PowerOfTwoLongValidator VECTORIZED_HASHAGG_NUMPARTITIONS = new PowerOfTwoLongValidator("exec.operator.aggregate.vectorize.num_partitions", 32, 8);
  /* concept of batch internal to vectorized hashagg operator and hashtable to manage the memory allocation.
   * as an example, if incoming batch size is 4096, consumeData() will internally treat this as 4 batches
   * inserted into hash table and accumulator. This is used to reduce the pre-allocated memory
   * for all partitions.
   */
  public static final PositiveLongValidator VECTORIZED_HASHAGG_BATCHSIZE = new PositiveLongValidator("exec.operator.aggregate.vectorize.max_hashtable_batch_size", 4096, 990);

  /* When running on large datasets with limited amount of memory (and thus excessive spilling), this setting
   * will generate huge amount of debug information potentially resulting in out of heap memory error.
   * The setting also has a noticeable performance impact on queries on large datasets.
   */
  public static final BooleanValidator VECTORIZED_HASHAGG_DEBUG_DETAILED_EXCEPTION = new BooleanValidator("exec.operator.aggregate.vectorize.tracedetails_on_exception", false);
  public static final PositiveLongValidator VECTORIZED_HASHAGG_DEBUG_MAX_OOMEVENTS = new PositiveLongValidator("exec.operator.aggregate.vectorize.debug_max_oomevents", 10000, 500);
  /* options to create a constrained memory setup for testing spill functionality in hashagg */
  public static final PositiveLongValidator VECTORIZED_HASHAGG_ALLOCATOR_INIT = new PositiveLongValidator("exec.operator.aggregate.vectorize.allocator_init", 1_000_000, 1_000_000);
  /* default limit has to be Long.MAX_VALUE as rest of the test infrastructure (including perf tests) that have agg queries
   * that don't spill should still work with the operator as though it has all the memory available.
   * agg spilling regression tests set this parameter on a query to query basis to vary the memory pressure
   */
  public static final PositiveLongValidator VECTORIZED_HASHAGG_ALLOCATOR_LIMIT = new PositiveLongValidator("exec.operator.aggregate.vectorize.allocator_limit", Long.MAX_VALUE, Long.MAX_VALUE);
  public static final BooleanValidator VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS = new BooleanValidator("exec.operator.aggregate.vectorize.minimize_spilled_partitions", true);
  public static final BooleanValidator VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION = new BooleanValidator("exec.operator.aggregate.vectorize.use_insertion_sort", false);
  public static final BooleanValidator VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR = new BooleanValidator("exec.operator.aggregate.vectorize.use_spilling_operator", false);
  public static final PowerOfTwoLongValidator VECTORIZED_HASHAGG_JOINT_ALLOCATION_MIN = new PowerOfTwoLongValidator("exec.operator.aggregate.vectorize.joint_allocation_min", 4*1024, 4*1024);
  public static final PowerOfTwoLongValidator VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX = new PowerOfTwoLongValidator("exec.operator.aggregate.vectorize.joint_allocation_max", 1024*1024, 64*1024);
  public static final BooleanValidator VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT = new BooleanValidator("exec.operator.aggregate.vectorize.use_minimum_as_limit", false);
  public static final PositiveLongValidator VARIABLE_FIELD_SIZE_ESTIMATE = new PositiveLongValidator("exec.operator.aggregate.vectorize.variable_width_size_estimate", Integer.MAX_VALUE, 15);
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedHashAggOperator.class);

  private final OperatorContext context;
  private VectorContainer outgoing;
  private final HashAggregate popConfig;

  private final Stopwatch pivotWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();
  private final Stopwatch accumulateWatch = Stopwatch.createUnstarted();
  private final Stopwatch unpivotWatch = Stopwatch.createUnstarted();
  private final Stopwatch hashComputationWatch = Stopwatch.createUnstarted();
  private final Stopwatch sortPriorToAccumulateWatch = Stopwatch.createUnstarted();
  private final Stopwatch spillPartitionWatch = Stopwatch.createUnstarted();
  private final Stopwatch readSpilledBatchWatch = Stopwatch.createUnstarted();

  private ImmutableList<FieldVector> vectorsToValidate;
  private PivotDef pivot;
  private VectorAccessible incoming;
  private State state = State.NEEDS_SETUP;
  private boolean fixedOnly;
  private VectorizedHashAggPartition[] hashAggPartitions;
  private PartitionToLoadSpilledData partitionToLoadSpilledData;
  private VectorizedHashAggPartitionSpillHandler partitionSpillHandler;
  private InternalState internalStateMachine;

  /* configured options */
  private final int numPartitions;
  private final int minHashTableSize;
  private final int minHashTableSizePerPartition;
  private final int estimatedVariableWidthKeySize;
  private final int maxHashTableBatchSize;

  private int hashPartitionMask;
  private final HashTableStatsHolder statsHolder;
  private int outputPartitionIndex;
  private int outputBatchCount;
  private int iterations;
  private int ooms;
  private final BufferAllocator allocator;
  private final VectorizedHashAggDebug debug;
  private boolean closed;


  /* preallocated data structures for hash table insertion */
  private ArrowBuf sortedHashTableOrdinals;
  private ArrowBuf hashTableOrdinals;
  private SimpleBigIntVector hashValues;
  private FixedBlockVector fixedBlockVector;
  private VariableBlockVector variableBlockVector;

  @VisibleForTesting
  public static final int PARTITIONINDEX_HTORDINAL_WIDTH = 9;
  public static final int HTORDINAL_OFFSET = 1;
  public static final int KEYINDEX_OFFSET = 5;

  /* cache widely used hashtable info */
  private int maxVariableBlockLength;

  private final BufferAllocator outputAllocator;
  private boolean initDone;
  private final boolean minimizeSpilledPartitions;

  private final boolean useInsertionSort;
  private final int[] count;
  private final long jointAllocationMin;
  private final long jointAllocationLimit;

  private final boolean setLimitToMinReservation;

  public static final String OUT_OF_MEMORY_MSG = "Vectorized Hash Agg ran out of memory";

  public VectorizedHashAggOperator(HashAggregate popConfig, OperatorContext context) throws ExecutionSetupException {
    final OptionManager options = context.getOptions();
    this.context = context;
    this.allocator = context.getAllocator();
    this.popConfig = popConfig;
    this.numPartitions = (int)options.getOption(VECTORIZED_HASHAGG_NUMPARTITIONS);
    this.minHashTableSize = (int)options.getOption(ExecConstants.MIN_HASH_TABLE_SIZE);
    this.minHashTableSizePerPartition = (int)Math.ceil((minHashTableSize * 1.0)/numPartitions);
    this.estimatedVariableWidthKeySize = (int)options.getOption(VARIABLE_FIELD_SIZE_ESTIMATE);
    this.maxHashTableBatchSize = (int) options.getOption(VECTORIZED_HASHAGG_BATCHSIZE);
    Preconditions.checkArgument(maxHashTableBatchSize > 0 && maxHashTableBatchSize <= 4096,
      "Error: max hash table batch size should be greater than 0 and not exceed 4096");
    final boolean traceOnException = options.getOption(VECTORIZED_HASHAGG_DEBUG_DETAILED_EXCEPTION);
    this.hashPartitionMask = numPartitions - 1;
    this.statsHolder = new HashTableStatsHolder();
    this.outputPartitionIndex = 0;
    this.outputBatchCount = 0;
    /* there is atleast one iteration of processing */
    this.iterations = 1;
    this.ooms = 0;
    this.debug = new VectorizedHashAggDebug(traceOnException, (int)options.getOption(VECTORIZED_HASHAGG_DEBUG_MAX_OOMEVENTS));
    this.closed = false;
    this.outputAllocator = context.getFragmentOutputAllocator();
    /*
     * notes on usage of allocator:
     *
     * we use the operator allocator for meeting all memory needs of the operator except for output
     * -- hashtable, accumulators, preallocation, auxiliary structures etc and as and when we grow
     * these data structures. the settings (init, limit) on this allocator are currently configured
     * through an ExecConstant option (or through popconfig if we are running spilling unit tests)
     * and we vary the limit in our spilling functional tests (unit/regression) to put memory
     * pressure on agg operator. So the child allocator helps to create a constrained memory setup.
     */
    this.outgoing = new VectorContainer(outputAllocator);
    this.initDone = false;
    this.minimizeSpilledPartitions = options.getOption(VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS);
    /* we have implemented efficient sort (no comparisons) but the code change is on the
     * critical path and we don't have automated spilling regression tests yet. So using
     * an option temporarily to allow both sorting algos and during testing/debugging we
     * can possibly isolate issues since insertion sort has stood the test of time and
     * functions correctly but potentially less efficient sometimes. The overhead due to
     * insertion sort is absolutely negligible for long (few minutes or more) running queries
     * though.
     */
    this.useInsertionSort = options.getOption(VECTORIZED_HASHAGG_USE_INSERTION_SORT_FOR_ACCUMULATION);
    this.count = new int[numPartitions];
    /* joint allocation limit should be a power of 2, 0 is allowed as we treat that as no limit and is also
     * used by non spilling vectorized hash agg
     */
    this.jointAllocationMin = options.getOption(VECTORIZED_HASHAGG_JOINT_ALLOCATION_MIN);
    this.jointAllocationLimit = options.getOption(VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX);
    this.setLimitToMinReservation = options.getOption(VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT);
    logger.debug("partitions:{}, min-hashtable-size:{}, variable-width-key-size:{}, max-hashtable-batch-size:{}",
      numPartitions, minHashTableSize, estimatedVariableWidthKeySize, maxHashTableBatchSize);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    state.is(State.NEEDS_SETUP);
    this.incoming = accessible;
    this.pivot = createPivot();
    debug.setInfoBeforeInit(allocator.getInitReservation(), allocator.getLimit(),
                            maxHashTableBatchSize, maxVariableBlockLength,
                            estimatedVariableWidthKeySize, pivot.getVariableCount(),
                            pivot.getBlockWidth(), minHashTableSize,
                            numPartitions, minHashTableSizePerPartition);
    initStructures();
    partitionSpillHandler = new VectorizedHashAggPartitionSpillHandler(hashAggPartitions, context.getFragmentHandle(),
                                                                       context.getOptions(), context.getConfig(),
                                                                       popConfig.getOperatorId(), partitionToLoadSpilledData,
                                                                       context.getSpillService(), minimizeSpilledPartitions);
    this.outgoing.buildSchema();
    debug.setInfoAfterInit(allocator.getAllocatedMemory(), outgoing.getSchema());
    /* allocator.getAllocatorMemory() at this point represents the minimum reservation
     * (aka preallocation) that operator definitely needs to complete the query.
     * to stress test the spilling functionality, we allow the operator's
     * allocator limit to be same as preallocation.
     */
    if (setLimitToMinReservation) {
      allocator.setLimit(allocator.getAllocatedMemory());
    }
    this.fixedOnly = pivot.getVariableCount() == 0;
    this.internalStateMachine = InternalState.NONE;
    this.initDone = true;

    state = State.CAN_CONSUME;
    return outgoing;
  }

  private void setLocalInfoForHashTable() {
    /* the general hash table configuration is same for all partitions
     * so we can just pick the hashtable from first partition and grab
     * the info we need.
     */
    this.maxVariableBlockLength = hashAggPartitions[0].hashTable.getVariableBlockMaxLength();
    /* hashtable should not have changed the configured value of VECTORIZED_HASHAGG_MAXBATCHSIZE */
    Preconditions.checkArgument(maxHashTableBatchSize == hashAggPartitions[0].hashTable.getMaxValuesPerBatch(),
      "Error: detected inconsistent max batch size");
  }

  /**
   * The hash aggregation algorithm works at a partition level by
   * hash-partitioning the incoming batch into a fixed number of
   * partitions. To do the in-memory aggregation at a partition level,
   * the fundamental data structures are maintained as part of each
   * partition thereby handling an orthogonal chunk of incoming dataset.
   *
   * The in-memory aggregation on a particular partition is independent
   * of other partitions. When the entire input has been consumed and
   * the operator is ready to send back data, each partition outputs
   * the respective data (accumulators and group by columns).
   *
   * This function does the basic setup required to handle the partitions.
   * The partitions are literally empty in the sense that no data has been
   * partitioned yet but the data structures that are required to execute
   * the in-memory aggregation algorithm at a partition level are setup
   * here.
   *
   * The function is called exactly once when the setup for VectHashAggOp
   * is done. Subsequently as the incoming batches arrive in the call to
   * consumeData(), we hash partition the data, compute the target
   * partition number for each incoming key(s), and insert (and accumulate)
   * data for the corresponding target partition.
   */
  private void initStructures() throws Exception {
    final long memoryUsageBeforeInit = allocator.getAllocatedMemory();
    debug.setAllocatedMemoryBeforeInit(memoryUsageBeforeInit);
    final int numPartitions = this.numPartitions;
    this.hashAggPartitions = new VectorizedHashAggPartition[numPartitions];
    /*
     * STEP 1: Materialize the aggregate expressions once as opposed to repeating it for
     * every partition. We materialize and extract enough information about each accumulator
     * (measure) which can then be used to construct an accumulator for each partition.
     */
    final AccumulatorBuilder.MaterializedAggExpressionsResult materializeAggExpressionsResult =
      AccumulatorBuilder.getAccumulatorTypesFromExpressions(context.getClassProducer(), popConfig.getAggrExprs(), incoming);
    /*
     * STEP 2: Build data structures for each partition.
     *         -- build hash table
     *         -- build nested accumulator using the result of expression materialization
     *            from STEP 1.
     *
     *            the output accumulator vector for the very first
     *            partition will be added to the outgoing container of the operator.
     *            Since we output partition at a time, no need to repeatedly add
     *            output vectors from rest of the N-1 partitions to outgoing. They
     *            simply transfer their data to the vector in outgoing container.
     */
    try(AutoCloseables.RollbackCloseable rollbackable = new AutoCloseables.RollbackCloseable()) {
      for (int i = 0; i < numPartitions; i++) {
        /* this step doesn't allocate any memory for accumulators */
        final AccumulatorSet accumulator = AccumulatorBuilder.getAccumulator(allocator,
                                                                          outputAllocator,
                                                                          materializeAggExpressionsResult,
                                                                          outgoing,
                                                                          (i == 0),
                                                                          maxHashTableBatchSize,
                                                                          jointAllocationMin,
                                                                          jointAllocationLimit);
        /* this step allocates memory for control structure in hashtable and reverts itself if
         * allocation fails so we don't have to rely on rollback closeable
         */
        final LBlockHashTable hashTable = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator,
          minHashTableSizePerPartition, estimatedVariableWidthKeySize, true, accumulator, maxHashTableBatchSize);
        final String partitionIdentifier = "P" + String.format("%03d", i);
        final VectorizedHashAggPartition hashAggPartition =  new VectorizedHashAggPartition(accumulator, hashTable, pivot.getBlockWidth(), partitionIdentifier);
        this.hashAggPartitions[i] = hashAggPartition;
        /* add partition to rollbackable before preallocating because if preallocation
         * fails, we still need to release memory for control structures in hashtable
         * allocated during hashtable instantiation
         */
        rollbackable.add(hashAggPartition);
        /* preallocate memory for single batch, hashtable will internally do this for accumulator */
        hashTable.preallocateSingleBatch();
      }
      rollbackable.commit();
    } catch (OutOfMemoryException e) {
      ooms++;
      throw debug.prepareAndThrowException(e, "Error: Failed to preallocate memory for single batch in partitions", HashAggErrorType.OOM);
    }

    final long memoryAfterCreatingPartitions = allocator.getAllocatedMemory();
    debug.setPreallocatedMemoryForPartitions(memoryAfterCreatingPartitions - memoryUsageBeforeInit);

    /* STEP 3: grab commonly used hashtable info */
    setLocalInfoForHashTable();

    /* STEP 4: Build extra partition (aka loading or read partition) */
    final List<Field> postSpillAccumulatorVectorFields = materializeAggExpressionsResult.outputVectorFields;
    allocateExtraPartition(postSpillAccumulatorVectorFields);

    final long memoryAfterExtraPartition = allocator.getAllocatedMemory();
    debug.setPreallocatedMemoryForReadingSpilledData(memoryAfterExtraPartition - memoryAfterCreatingPartitions);

    /* STEP 5: Allocate auxiliary structures */
    allocateMemoryForHashTableInsertion();
    debug.setPreallocatedMemoryForAuxStructures(allocator.getAllocatedMemory() - memoryAfterExtraPartition);
  }

  /**
   * Pre-allocate the extra partition that will be used to load a single batch from
   * a spilled partition. This isn't an actual partition per se. We just pre-allocate
   * necessary data structures with enough memory to load a single batch from spilled
   * partition. By doing this we safeguard ourselves and don't run into a situation
   * where we have spilled a bunch of data but then there is not enough memory to
   * read back the spilled data and continue the processing.
   *
   * @param postSpillAccumulatorVectorFields accumulator vector types used to
   *                                         allocate vectors for reading accumulator
   *                                         data from spilled batch.
   * @throws Exception
   */
  private void allocateExtraPartition(final List<Field> postSpillAccumulatorVectorFields) throws Exception {
    Preconditions.checkArgument(partitionToLoadSpilledData == null, "Error: loading partition has already been pre-allocated");
    final int fixedWidthDataRowSize = pivot.getBlockWidth();
    final int fixedBlockSize = fixedWidthDataRowSize * maxHashTableBatchSize;
    final int variableBlockSize = maxVariableBlockLength;
    try {
      partitionToLoadSpilledData = new PartitionToLoadSpilledData(allocator, fixedBlockSize, variableBlockSize, postSpillAccumulatorVectorFields, maxHashTableBatchSize);
    } catch (OutOfMemoryException e) {
      ooms++;
      throw debug.prepareAndThrowException(e, "Error: Failed to preallocate memory for extra partition used to load spilled batch", HashAggErrorType.OOM);
    }
  }

  /**
   * When inserting data into hashtable (both during initial iteration
   * of aggregation and post-spill processing), we need auxiliary data structures
   * for inserting keys into the hash table. These auxiliary structures are
   * used as follows every time we receive an incoming batch
   *
   * (1) hashTableOrdinals - array to store partition indices and ordinals as
   *     the data is inserted into hashtable.
   *
   * (2) hashValues - array to store hashvalues since we compute the hash for
   *     entire batch at once and fill this array with hashvalues.
   *
   * (3) fixedBlockVector - fixed block buffer to store the pivoted fixed width
   *     key column values.
   *
   * (4) variableBlockVector - variable block buffer to store the pivoted
   *     variable width key column values.

   * The reason we need (3) and (4) is because we pivot into temporary space
   * and later do memcpy() to hashtable buffers/blocks during insertion. So we
   * don't want to allocate temporary buffers every time upon entry into
   * consumeData().
   */
  private void allocateMemoryForHashTableInsertion() throws Exception {
    try(AutoCloseables.RollbackCloseable rollbackable = new AutoCloseables.RollbackCloseable()) {
      sortedHashTableOrdinals = allocator.buffer(maxHashTableBatchSize * VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH);
      rollbackable.add(sortedHashTableOrdinals);
      hashTableOrdinals = allocator.buffer(maxHashTableBatchSize * VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH);
      rollbackable.add(hashTableOrdinals);
      hashValues = new SimpleBigIntVector("hashvalues", allocator);
      rollbackable.add(hashValues);
      hashValues.allocateNew(maxHashTableBatchSize);
      fixedBlockVector = new FixedBlockVector(allocator, pivot.getBlockWidth(), maxHashTableBatchSize, true);
      rollbackable.add(fixedBlockVector);
      variableBlockVector = new VariableBlockVector(allocator, pivot.getVariableCount(), maxVariableBlockLength, true);
      rollbackable.commit();
    } catch (OutOfMemoryException e) {
      sortedHashTableOrdinals = null;
      hashTableOrdinals = null;
      hashValues = null;
      fixedBlockVector = null;
      variableBlockVector = null;
      ooms++;
      throw debug.prepareAndThrowException(e, "Error: Failed to preallocate memory for auxiliary structures", HashAggErrorType.OOM);
    }
  }

  /**
   * Create pivot definition. This operation is independent of
   * partitioning and needs to be done once during setup and not
   * per partition. Populates the outgoing vector container
   * with target vectors and builds set of FieldVectorPair
   * <inputVector, outputVector> for creating the pivot
   * definition.
   *
   * @return vector pivot definition for the incoming GROUP BY
   * expressions.
   */
  private PivotDef createPivot(){
    final List<NamedExpression> groupByExpressions = popConfig.getGroupByExprs();
    final ImmutableList.Builder<FieldVector> validationVectors = ImmutableList.builder();

    final List<FieldVectorPair> fvps = new ArrayList<>();
    for (int i = 0; i < groupByExpressions.size(); i++) {
      final NamedExpression ne = groupByExpressions.get(i);
      final LogicalExpression expr = context.getClassProducer().materialize(ne.getExpr(), incoming);

      if(expr == null){
        throw unsup("Unable to resolve group by expression: " + ne.getExpr().toString());
      }
      if( !(expr instanceof ValueVectorReadExpression) ){
        throw unsup("Group by expression is non-trivial: " + ne.getExpr().toString());
      }

      final ValueVectorReadExpression vvread = (ValueVectorReadExpression) expr;
      final FieldVector inputVector = incoming.getValueAccessorById(FieldVector.class, vvread.getFieldId().getFieldIds()).getValueVector();
      if(inputVector instanceof VarCharVector || inputVector instanceof VarBinaryVector){
        validationVectors.add(inputVector);
      }
      final FieldVector outputVector = TypeHelper.getNewVector(expr.getCompleteType().toField(ne.getRef()), outputAllocator);
      outgoing.add(outputVector);
      fvps.add(new FieldVectorPair(inputVector, outputVector));
    }

    this.vectorsToValidate = validationVectors.build();
    return PivotBuilder.getBlockDefinition(fvps);
  }

  /**
   * We consume the data in multiple steps. In each step, the operator processes
   * a subset of records as follows:
   *
   * (1) pivot subset of records into preallocated pivot space
   * (2) insert the pivoted records into partitions
   * (3) accumulate the pivoted records across all partitions.
   *
   * the above three sub-steps are repeated until all records arriving in consumeData()
   * have been processed by the operator.
   *
   * The reason for pivoting a subset of records is that Pivots.pivot() internally
   * expands the variable block buffer. We don't want that and instead allocate a fixed size
   * variable block buffer and leave upto BoundedPivots.pivot() to decide how many records
   * can be pivoted without allocating more memory.
   *
   * @param records number of records to consume
   * @throws Exception
   */
  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);

    // ensure that none of the variable length vectors are corrupt so we can avoid doing bounds checking later.
    for(FieldVector v : vectorsToValidate){
      VariableLengthValidator.validateVariable(v, records);
    }

    final ArrowBuf offsets = this.hashTableOrdinals;
    final SimpleBigIntVector hashValues = this.hashValues;
    final FixedBlockVector fixedBlockVector = this.fixedBlockVector;
    final VariableBlockVector variableBlockVector = this.variableBlockVector;
    final long keyFixedVectorAddr = fixedBlockVector.getMemoryAddress();
    final long keyVarVectorAddr = variableBlockVector.getMemoryAddress();
    final boolean fixedOnly = this.fixedOnly;

    int recordsConsumed = 0;
    while (recordsConsumed != records) {
      /*
       * consumption of 4K records will happen in multiple steps and the
       * pre-allocated memory (pivot space, ordinal array, hashvalue vector) used in each
       * step is allocated based on maxHashTableBatchSize and cannot be expanded beyond that.
       */
      final int stepSize = Math.min(maxHashTableBatchSize, records - recordsConsumed);

      /* STEP 1: first we pivot, this step is unrelated to partitioning */
      pivotWatch.start();
      final int recordsPivoted = BoundedPivots.pivot(pivot, recordsConsumed, stepSize, fixedBlockVector, variableBlockVector);
      pivotWatch.stop();

      /* STEP 2:
       * then we do the hash computation on entire batch, this is again independent of
       * partitioning. the 64 bit hash is computed for all the keys
       */
      final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
        pivot.getBlockWidth(), recordsPivoted, hashValues.getBufferAddress(), 0);
      hashComputationWatch.start();
      HashComputation.computeHash(blockChunk);
      hashComputationWatch.stop();

      /* STEP 3: then we hash partition the dataset and add pivoted data to multiple hash tables */
      long partitionsUsed = insertIntoPartitions(offsets, hashValues, recordsPivoted, keyFixedVectorAddr, keyVarVectorAddr, recordsConsumed);

      /* STEP 4: then we do accumulators for all partitions in a single pass */
      accumulateForAllPartitions(offsets, recordsPivoted, partitionsUsed);

      recordsConsumed += recordsPivoted;
      /* TODO: evaluate if resetting the auxiliary structures between iterations is necessary */
      resetHashTableInsertionStructures();
    }

    /* STEP 5: update hashtable stats */
    updateStats();
  }

  private void resetHashTableInsertionStructures() {
    fixedBlockVector.reset();
    variableBlockVector.reset();
    hashValues.reset();
    hashTableOrdinals.setZero(0, hashTableOrdinals.capacity());
    sortedHashTableOrdinals.setZero(0, sortedHashTableOrdinals.capacity());
  }

  /**
   * Insert pivoted GROUP BY key columns data into hash table. We distribute
   * the data across a fixed number of partitions. The partition index is computed
   * using higher order 32 bits from 64 bit hashvalue. Subsequently key data is
   * inserted into the hashtable of corresponding partition.
   *
   * @param offsets buffer to store hashtable ordinal info, partition index and record index
   * @param hashValues vector that has precomputed hashvalues for the entire batch
   * @param records number of records in the incoming batch
   * @param keyFixedVectorAddr starting address of buffer that stores fixed width pivoted keys
   * @param keyVarVectorAddr starting address of buffer that stores variable width pivoted keys
   * @param startingRecordIndex absolute starting index of first record in the original
   *                            batch received by consumeData().
   *
   * @return bitmap to indicate which partitions were used during insertion
   *
   * IMPORTANT:
   *
   * consumeData() works in sequential stages where each stage processes a subset of records.
   * In order to correctly accumulate data, we still need to track the original absolute index
   * of record in the coming batch. This is the reason for using startingRecordIndex. Let's
   * take an example:
   *
   * input batch arriving into consumeData() has 4K records.
   *
   * stage 1: pivoted only 512 [0-511] and so process them.
   * stage 2: pivot from 512-1023 and process them. we are again processing 512 records in this stage
   *          but to correctly map each record to its correct column value in input accumulator
   *          vector, we cannot operate on 0 based record indexes when accumulating. In other
   *          words, 6th record in this stage is 517th record in incoming batch and we need
   *          to pass the latter index to accumulate() method.
   *
   * We can run out of memory while inserting into the hash table:
   *
   * (1) rehashing -- expanding ordinals by adding control blocks
   * (2) adding data blocks
   * (3) (2) also adds a corresponding accumulator vector in each accumulator
   *
   * The memory allocation in {@link LBlockHashTable} is made to be atomic
   * such that if it fails, we cleanup the state back to where it was as if
   * memory allocation was never attempted. hashtable code appropriately
   * propagates back the exception and here we handle it by spilling partition.
   * After spilling, we continue with insertion of record which earlier failed.
   */
  private long insertIntoPartitions(final ArrowBuf offsets, final SimpleBigIntVector hashValues,
                                    final int records, final long keyFixedVectorAddr,
                                    final long keyVarVectorAddr, final int startingRecordIndex) {
    long offsetAddr = offsets.memoryAddress();
    long partitionsUsed = 0;
    insertWatch.start();
    for(int keyIndex = 0; keyIndex < records; keyIndex++, offsetAddr += PARTITIONINDEX_HTORDINAL_WIDTH){
      final long keyHash = hashValues.get(keyIndex);
      /* get the partition index from higher order bits in hash */
      final int hashPartitionIndex = ((int)(keyHash >> 32)) & hashPartitionMask;
      final LBlockHashTable table = hashAggPartitions[hashPartitionIndex].hashTable;
      boolean insertSuccessful = false;
      while (!insertSuccessful) {
        try {
          final int ordinal = table.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, (int)keyHash);
          /* insert successful so store the tuple of <partition index, hash table ordinal, incoming key index> */
          PlatformDependent.putByte(offsetAddr, (byte)hashPartitionIndex);
          PlatformDependent.putInt(offsetAddr + HTORDINAL_OFFSET, ordinal);
          PlatformDependent.putInt(offsetAddr + KEYINDEX_OFFSET, keyIndex + startingRecordIndex);
          /* set the bit to remember the target partitions, this will be used later during accumulation */
          partitionsUsed = partitionsUsed | (1 << hashPartitionIndex);
          hashAggPartitions[hashPartitionIndex].bumpRecords(1);
          insertSuccessful = true;
        } catch (OutOfMemoryException e) {
          ooms++;
          debug.recordOOMEvent(iterations, ooms, allocator.getAllocatedMemory(), hashAggPartitions, partitionSpillHandler);
          logger.debug("Error: ran out of memory while inserting in hashtable, records to insert:{}, current record index:{}, absolute record index:{}, error: {}",
                       records, keyIndex, keyIndex + startingRecordIndex, e);
          handleOutOfMemory(e, offsets, keyIndex, hashPartitionIndex);
        }
      }
    }
    insertWatch.stop();

    return partitionsUsed;
  }

  /**
   * When operator detects (proactively) it will run out of memory or it has
   * already run of memory, we use this function to handle OOM by spilling a
   * partition. As of now we use it only in latter case.
   *
   * IMPORTANT:
   *
   * When we spill a partition we downsize it to minimum memory required to be
   * pre-allocated. Therefore if all partitions have a single batch in their
   * respective hashtable (and accumulator) then even after
   * spilling, the effective memory available to the allocator won't increase.
   * For example, say each of the 8 partitions have a single batch in their
   * respective hashtable and accumulator. Say insertion into P4 failed because
   * it required addition of new block and we are here to handle out of memory.
   * Say we choose victim partition as P7 and spill it. At the end of it, the memory
   * available to allocator hasn't increased since all partitions have single batch
   * (downsized to minimum preallocation needed at all times). We go back and reattempt
   * the insert into P4 hoping now its request to add a new block/batch will succeed.
   * But it won't since spilling didn't really release any memory. Yes for P7 we
   * have more memory available as it was spilled and re-initialized but that
   * doesn't help insertion into P4 -- we need to have memory to allocate additional
   * block that insertion into P4 is requesting.
   *
   * To solve this situation, we don't let {@link VectorizedHashAggPartitionSpillHandler} to
   * choose a victim partition. Instead just spill the partition that initially
   * failed the insert and hit OOM. This way that single batch in partition would
   * have gone to disk and the state would have been re-initialized to start storing
   * data from 0th ordinal (first record) as the memory usage of partition is now 0.
   *
   * @param oom OutOfMemoryException caught by the caller
   * @param offsets buffer that stores 9 byte tuples as incoming data is inserted into partitions
   *                (partition index (1), ht ordinal (4), record index (4)
   * @param keyIndex number of records inserted successfully before we hit OOM
   * @param failedPartitionIndex partition that we earlier failed to insert into
   */
  private void handleOutOfMemory(final OutOfMemoryException oom,
                                 final ArrowBuf offsets, final int keyIndex,
                                 final int failedPartitionIndex) {
    final long allocatedMemoryBeforeSpilling = allocator.getAllocatedMemory();
    VectorizedHashAggPartition victimPartition = partitionSpillHandler.chooseVictimPartition();
    if (victimPartition == null) {
      /* Just spill the current partition that we failed to insert data into.
       * this will not "release" any memory but reduce the partition's
       * memory usage to 0 and we should be able to continue with insertion.
       */
      victimPartition = hashAggPartitions[failedPartitionIndex];
    }
    accumulateBeforeSpill(victimPartition, offsets, keyIndex);
    spillPartitionWatch.start();
    try {
      partitionSpillHandler.spillPartition(victimPartition);
    } catch (Exception e) {
      throw debug.prepareAndThrowException(e, "Error: Failed to spill partition", HashAggErrorType.SPILL_WRITE);
    } finally {
      spillPartitionWatch.stop();
    }
    logger.debug("Partition: {} spilled. Memory usage before spilling: {}, memory usage after spilling: {}",
                 victimPartition.getIdentifier(), allocatedMemoryBeforeSpilling, allocator.getAllocatedMemory());
  }

  /**
   * Implementation of count sort algorithm to rearrange tuple
   * <partition index, HT ordinal, record index> on a per-partition
   * basis.
   * It is a stable sorting algorithm
   * @param buffer  buffer that contains tuples created when records were consumed
   *                and inserted into partitions
   * @param records total number of records consumed
   */
  private void countSort(final ArrowBuf buffer, final int records) {
   final int[] count = this.count;
    count[0] = 0;
    for (int i = 1; i <= numPartitions - 1; ++i) {
      /* compute start position for records of each partition in the sorted array */
      count[i] = hashAggPartitions[i - 1].records + hashAggPartitions[i - 1].getRecordsSpilled() + count[i - 1];
    }
    final ArrowBuf ordinalsSortedByPartition = this.sortedHashTableOrdinals;
    final long sortedStartAddr = ordinalsSortedByPartition.memoryAddress();
    final long startAddr = buffer.memoryAddress();
    final long maxAddr = startAddr + (records * PARTITIONINDEX_HTORDINAL_WIDTH);
    for(long addr = startAddr; addr < maxAddr; addr += PARTITIONINDEX_HTORDINAL_WIDTH) {
      final byte hashPartitionIndexToInsert = PlatformDependent.getByte(addr); /* first byte */
      final int hashTableOrdinalToInsert = PlatformDependent.getInt(addr + HTORDINAL_OFFSET); /* next 4 bytes */
      final int keyIndexToInsert = PlatformDependent.getInt(addr + KEYINDEX_OFFSET); /* next 4 bytes */
      final int offset = count[hashPartitionIndexToInsert];
      /* store the sorted tuples in sortedHashTableOrdinals ArrowBuf */
      final long addrNext = sortedStartAddr + (offset * PARTITIONINDEX_HTORDINAL_WIDTH);
      PlatformDependent.putByte(addrNext, hashPartitionIndexToInsert);
      PlatformDependent.putInt(addrNext + HTORDINAL_OFFSET, hashTableOrdinalToInsert);
      PlatformDependent.putInt(addrNext + KEYINDEX_OFFSET, keyIndexToInsert);
      /* bump offset to store the next item with same key at subsequent location,
       * this gives the "stable sort" nature. we need stable sort to account for spilled
       * records and skip them during accumulation
       */
      ++count[hashPartitionIndexToInsert];
    }
  }

  /**
   * We can run out of memory while consuming data (as we are inserting records into hashtable).
   * If we had planned to insert N records and hit OOM after inserting M records successfully
   * (M < N) then before we spill a partition, we need to complete accumulation of K records
   * (K < M) inserted so far and that belong to the partition we have decided to spill. This
   * is why we call accumulateBeforeSpill() from handleOutOfMemory().
   *
   * @param partitionToSpill partition chosen for spilling, accumulate records for this partition
   * @param offsets buffer that stores 9 byte tuples as incoming data is inserted into partitions
   *                (partition index (1), ht ordinal (4), record index (4)
   * @param keyIndex number of records (M) successfully inserted into the hashtable before we hit OOM.
   */
  private void accumulateBeforeSpill(final VectorizedHashAggPartition partitionToSpill,
                                     final ArrowBuf offsets, final int keyIndex) {
    /* re-arrange hash table ordinal info on a per-partition basis */
    sortPriorToAccumulateWatch.start();
    if (useInsertionSort) {
      insertionSort(offsets, keyIndex);
    } else {
      countSort(offsets, keyIndex);
    }
    sortPriorToAccumulateWatch.stop();

    /* skip the (M - K) records for other partitions */
    int recordsToSkip = 0;
    for (int i=0; i < numPartitions; i++) {
      if (hashAggPartitions[i].getIdentifier().equals(partitionToSpill.getIdentifier())) {
        recordsToSkip += hashAggPartitions[i].getRecordsSpilled();
        break;
      }
      recordsToSkip += hashAggPartitions[i].getRecordsSpilled() + hashAggPartitions[i].getRecords();
    }

    /* accumulate K records for partition chosen to spill */
    final ArrowBuf sortedOffsets = useInsertionSort ? offsets : this.sortedHashTableOrdinals;
    long offsetAddr = sortedOffsets.memoryAddress() + (recordsToSkip * PARTITIONINDEX_HTORDINAL_WIDTH);
    accumulateWatch.start();
    final int partitionRecords = partitionToSpill.getRecords();
    final AccumulatorSet accumulator = partitionToSpill.accumulator;
    accumulator.accumulate(offsetAddr, partitionRecords);
    partitionToSpill.resetRecords();
    partitionToSpill.bumpRecordsSpilled(partitionRecords);
    accumulateWatch.stop();
  }

  /**
   * Aggregate data for each partition.
   *
   * @param offsets buffer that stores the tuple <hashtable ordinal info, partition index and record index>
   * @param records number of records in the incoming batch
   * @param partitionsUsed bitmap to indicate which partitions were used during insertion
   *
   * IMPORTANT:
   *
   * We don't need to handle OutOfMemory here since accumulator target vector(s)
   * already exist (and allocated). If hashtable insertion required adding
   * new data blocks then it would have also added new corresponding
   * accumulator vector and allocated it. Whether or not this was successful
   * should have already been known to us at the time of insertion in function
   * insertIntoPartitions() method.
   */
  private void accumulateForAllPartitions(final ArrowBuf offsets, final int records,
                                          long partitionsUsed) {
    /* re-arrange hash table ordinal info on a per-partition basis */
    sortPriorToAccumulateWatch.start();
    if (useInsertionSort) {
      insertionSort(offsets, records);
    } else {
      countSort(offsets, records);
    }
    sortPriorToAccumulateWatch.stop();

    /* accumulate -- if we used count sort above then it would have rearranged HT ordinals
     * and other info into another buffer sorted by partition indices. we use the latter
     * buffer to do accumulation of all records one partition at a time
     */
    final ArrowBuf sortedOffsets = useInsertionSort ? offsets : this.sortedHashTableOrdinals;
    long offsetAddr = sortedOffsets.memoryAddress();
    accumulateWatch.start();
    while (partitionsUsed > 0) {
      final byte hashPartitionIndex = (byte)Long.numberOfTrailingZeros(partitionsUsed);
      final VectorizedHashAggPartition partition = hashAggPartitions[hashPartitionIndex];
      if (partition.isSpilled()) {
        offsetAddr += partition.getRecordsSpilled() * PARTITIONINDEX_HTORDINAL_WIDTH;
      }
      final int partitionRecords = partition.getRecords();
      final AccumulatorSet accumulator = partition.accumulator;
      accumulator.accumulate(offsetAddr, partitionRecords);
      partitionsUsed = partitionsUsed & (partitionsUsed - 1);
      offsetAddr += partitionRecords * PARTITIONINDEX_HTORDINAL_WIDTH;
      partition.resetRecords();
      partition.resetSpilledRecords();
    }
    accumulateWatch.stop();
  }

  /**
   * Driver function to start recursion -- process a "SINGLE" spilled partition one batch
   * at a time.
   *
   * (1) Use {@link VectorizedHashAggPartitionSpillHandler} to get a disk iterator for
   *     the next spilled partition to process.
   * (2) Use disk iterator to read the spill file and load a batch into pre-allocated
   *     structures in {@link PartitionToLoadSpilledData}.
   * (3) Use the deserialized batch data as new incoming for the operator to do the
   *     in-memory aggregation.
   * (4) After reading and processing a single batch, reset the state of data structures
   *     in {@link PartitionToLoadSpilledData} to avoid mixing meta-data between different
   *     deserialized batches.
   * (5) Once all batches are read, close the disk iterator. This in turn closes
   *     the input stream and deletes the spill file.
   * (6) Indicate to the internal state machine that input data from spilled
   *     partition has been fully consumed and the operator can output.
   */
  private void consumeSpilledData() throws Exception {
    /* as far as externally visible state of operator is concerned, we are in outputData()
     * since we have consumed all incoming from source and noMoreToConsume() has been
     * invoked on the operator which should have set the state to CAN_PRODUCE.
     * outputData() should have set the internal state to PROCESS_SPILLED_PARTITION
     */
    state.is(State.CAN_PRODUCE);
    Preconditions.checkArgument(internalStateMachine == InternalState.PROCESS_SPILLED_PARTITION,
      "Error: Invalid internal operator state detected when attempting to process spilled data");
    final PartitionToLoadSpilledData partitionToLoadSpilledData = this.partitionToLoadSpilledData;
    /* Step 1: get disk iterator for the spilled partition to process */
    final SpilledPartitionIterator spilledPartitionIterator = partitionSpillHandler.getNextSpilledPartitionToProcess();
    final String partitionIdentifier = spilledPartitionIterator.getIdentifier();
    logger.debug("Processing disk partition:{}", partitionIdentifier);
    /* Step 2: read a single batch from the spill file */
    readSpilledBatchWatch.start();
    int numRecordsInBatch;
    try {
      numRecordsInBatch = spilledPartitionIterator.getNextBatch();
    } catch (Exception e) {
      throw debug.prepareAndThrowException(e, "Error: Failed to read a spilled batch", HashAggErrorType.SPILL_READ);
    } finally {
      readSpilledBatchWatch.stop();
    }
    if (numRecordsInBatch == -1) {
      /* we should not be attempting to read a spilled batch if all batches have been read.
       * if we are here, it indicates some inconsistency in the state maintained by the
       * operator as it reads a spilled partition batch by batch. partition's disk
       * iterator keeps track of how many batches have been read v/s remaining. if
       * the disk iterator attempted to read a batch and couldn't read anything due
       * to end of stream, there is a bug.
       */
      throw debug.prepareAndThrowException(null, "Error: Unexpected end of stream while reading a spilled batch",
                                           HashAggErrorType.SPILL_READ);
    }
    if (numRecordsInBatch == 0) {
      logger.debug("Successfully finished reading all batches of spilled partition:{}", partitionIdentifier);
      noMoreToConsumeFromSpill();
      partitionSpillHandler.closeSpilledPartitionIterator();
      partitionToLoadSpilledData.reset();
    } else {
      logger.debug("Read {} records from spilled batch", numRecordsInBatch);
      final ArrowBuf fixedWidthPivotedData = partitionToLoadSpilledData.getFixedKeyColPivotedData();
      final ArrowBuf variableWidthPivotedData = partitionToLoadSpilledData.getVariableKeyColPivotedData();
      final byte[] accumulatorTypes = partitionToLoadSpilledData.getAccumulatorTypes();
      final FieldVector[] accumulatorVectors = partitionToLoadSpilledData.getPostSpillAccumulatorVectors();
      if (spilledPartitionIterator.getCurrentBatchIndex() == 1) {
        /* we just read the first batch */
        initStateBeforeProcessingSpilledPartition(accumulatorTypes, accumulatorVectors, partitionIdentifier);
      }
      /* Step 3: re-partition, insert into hash tables and aggregate */
      consumeSpilledDataHelper(numRecordsInBatch, fixedWidthPivotedData, variableWidthPivotedData);
      /* Step 4: prepare the loading partition to read next batch */
      partitionToLoadSpilledData.reset();
    }
  }

  /**
   * Initialize the partitions because the incoming spilled batch will be repartitioned.
   * (by using a different seed in hash function) into same number of partitions we
   * originally started with.
   *
   * The partitions already exist and were earlier downsized to minimum pre-allocated
   * memory. Nothing needs to be done for the partition's hash table since it
   * is already ready -- downsized to the required minimum allocation.
   *
   * Each partition already has an instance of NestedAccumulator which internally
   * manages all child accumulators -- one for each measure column. So we don't
   * need to construct a brand new instance of accumulator for each partition
   * when starting to process a spilled partition.
   *
   * However, we ensure the following:
   *
   * The accumulator vectors read from disk now become the input accumulator
   * vectors for post-spill processing. So we need to set the input vector in
   * each partition's respective accumulator.
   *
   * There is one additional action we need to take for the accumulator in
   * each partition. This depends on the exact type of accumulator (sum, min etc)
   * and is explained in detail in
   * {@link VectorizedHashAggPartition#updateAccumulator(byte[], FieldVector[], BufferAllocator)}.
   *
   * Nothing needs to be done for the output vector of the accumulator
   * in each partition. This is because these vectors store the computed
   * (accumulated) values and already exist in the accumulator -- again
   * downsized to required minimum allocation.
   *
   * Update name(identifier) for each partition to reflect partition
   * hierarchy and the number of iterations the algorithm has gone through.
   *
   * So if we are processing spilled partition P0 from disk, then names of all
   * in-memory partitions will be {P0.P0, P0.P1, P0.P2, P0.P3}
   *
   * This indicates that we are processing spilled partition P0 from its spill
   * file with name "/some/path/P0" and the data is now getting re-partitioned
   * into {P0.P0, P0.P1, P0.P2, P0.P3}
   *
   * @param accumulatorTypes   types of accumulators
   * @param accumulatorVectors deserialized accumulator vectors
   * @param parentIdentifier   name of spilled partition we are about to process
   */
  private void initStateBeforeProcessingSpilledPartition(final byte[] accumulatorTypes,
                                                         final FieldVector[] accumulatorVectors,
                                                         final String parentIdentifier) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numPartitions; i++) {
      final VectorizedHashAggPartition partition = hashAggPartitions[i];
      if (iterations == 2) {
        /*
         * Update accumulator exactly once before the first post-spill
         * iteration begins. This is similar to how we setup incoming
         * in operator during operator setup time to get the vectors
         * that will store incoming data.
         */
        partition.updateAccumulator(accumulatorTypes, accumulatorVectors, allocator);
      }
      sb.append(parentIdentifier);
      sb.append(String.format(".P%03d", i));
      partition.updateIdentifier(sb.toString());
      sb.setLength(0);
    }
  }

  /**
   * When we begin the second iteration of aggregation algorithm by
   * reading spilled partition and re-partitioning it, we need to use
   * a different seed for hash computation else the hashvalues will be
   * the same as in the previous iteration and all the data from spilled
   * batch will end up going into a single partition. To avoid this problem
   * and allow good re-partitioning, we use a different seed value.
   *
   * @return seed
   */
  private long getSeedForRepartitioning() {
    return (long)iterations;
  }

  /**
   * Another version of consumeData() specifically for processing a single batch
   * from spilled partition. The spilled batch is essentially treated as new
   * incoming batch into the operator. The only key difference between this
   * function and consumeData() is that we don't have to pivot incoming
   * data from a spilled batch since it is already pivoted.
   *
   * @param records number of records in the deserialized spilled batch
   * @param fixedWidthPivotedData fixed width pivoted data from deserialized spilled batch
   * @param variableWidthPivotedData variable width pivoted data from deserialized spilled batch
   *
   * IMPORTANT:
   *
   * Unlike consumeData(), we don't need to run in a loop here to process the incoming
   * records in multiple stages. Since the data is already pivoted and we spill at the
   * granularity of hashtable (and accumulator) batches we can process the entire batch
   * at one go as no memory allocation is needed for auxiliary structures. they were already
   * pre-allocated based on maxHashTableBatchSize. we can run out of memory
   * during insertion and insertIntoPartitions() function will handle that.
   */
  private void consumeSpilledDataHelper(final int records, final ArrowBuf fixedWidthPivotedData,
                                        final ArrowBuf variableWidthPivotedData) {
    final long keyFixedVectorAddr = fixedWidthPivotedData.memoryAddress();
    final long keyVarVectorAddr = variableWidthPivotedData.memoryAddress();
    final boolean fixedOnly = this.fixedOnly;
    final long seed = getSeedForRepartitioning();

    /* grab local references */
    final ArrowBuf hashTableOrdinals = this.hashTableOrdinals;
    final SimpleBigIntVector hashValues = this.hashValues;

    /* STEP 1:
     * we do the hash computation on entire batch, this is again independent of
     * partitioning. the 64 bit hash is computed for all the keys
     */
    final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
      pivot.getBlockWidth(), records, hashValues.getBufferAddress(), seed);
    hashComputationWatch.start();
    HashComputation.computeHash(blockChunk);
    hashComputationWatch.stop();

    /* STEP 2: then we hash partition the dataset and add pivoted data to multiple hash tables */
    long partitionsUsed = insertIntoPartitions(hashTableOrdinals, hashValues, records, keyFixedVectorAddr, keyVarVectorAddr, 0);

    /* STEP 3: then we do accumulators for all partitions in a single pass */
    accumulateForAllPartitions(hashTableOrdinals, records, partitionsUsed);

    resetHashTableInsertionStructures();

    /* STEP 4: update hashtable stats */
    updateStats();
  }

  /**
   * sorts(in-place) the buffer containing <partition index, hash table ordinal, key index> tuples on
   * partition indices. Uses the insertion sort algorithm as of now.
   * @param buffer buffer containing 9 byte tuples of partition index and hash table ordinal
   * @param records number of tuples.
   */
  private void insertionSort(final ArrowBuf buffer, final int records) {
    final long startAddr = buffer.memoryAddress();
    final long maxAddr = startAddr + (records * PARTITIONINDEX_HTORDINAL_WIDTH);
    for(long addr = startAddr + PARTITIONINDEX_HTORDINAL_WIDTH; addr < maxAddr; addr += PARTITIONINDEX_HTORDINAL_WIDTH) {
      final byte hashPartitionIndexToInsert = PlatformDependent.getByte(addr); /* first byte */
      final int hashTableOrdinalToInsert = PlatformDependent.getInt(addr + HTORDINAL_OFFSET); /* next 4 bytes */
      final int keyIndexToInsert = PlatformDependent.getInt(addr + KEYINDEX_OFFSET); /* next 4 bytes */

      long addrPrevious = addr - PARTITIONINDEX_HTORDINAL_WIDTH;
      long addrNext;
      while (addrPrevious >= startAddr && PlatformDependent.getByte(addrPrevious) > hashPartitionIndexToInsert) {
        addrNext = addrPrevious + PARTITIONINDEX_HTORDINAL_WIDTH;
        PlatformDependent.putByte(addrNext, PlatformDependent.getByte(addrPrevious));
        PlatformDependent.putInt(addrNext + HTORDINAL_OFFSET, PlatformDependent.getInt(addrPrevious + HTORDINAL_OFFSET));
        PlatformDependent.putInt(addrNext + KEYINDEX_OFFSET, PlatformDependent.getInt(addrPrevious + KEYINDEX_OFFSET));
        addrPrevious -= PARTITIONINDEX_HTORDINAL_WIDTH;
      }

      addrNext = addrPrevious + PARTITIONINDEX_HTORDINAL_WIDTH;
      PlatformDependent.putByte(addrNext, hashPartitionIndexToInsert);
      PlatformDependent.putInt(addrNext + HTORDINAL_OFFSET, hashTableOrdinalToInsert);
      PlatformDependent.putInt(addrNext + KEYINDEX_OFFSET, keyIndexToInsert);
    }
  }

  private int getHashTableSize() {
    final VectorizedHashAggPartition[] hashAggPartitions = this.hashAggPartitions;
    int tableSize = 0;
    for (int i = 0; i < numPartitions; i++) {
      tableSize += hashAggPartitions[i].hashTable.size();
    }
    return tableSize;
  }

  private void computeAllStats() {
    final VectorizedHashAggPartition[] hashAggPartitions = this.hashAggPartitions;
    final int numPartitions = this.numPartitions;
    int tableSize = 0;
    int tableCapacity = 0;
    int tableRehashCount = 0;
    long allocatedForFixedBlocks = 0;
    long unusedForFixedBlocks = 0;
    long allocatedForVarBlocks = 0;
    long unusedForVarBlocks = 0;
    long rehashTime = 0;
    int minTableSize = Integer.MAX_VALUE;
    int maxTableSize = Integer.MIN_VALUE;
    int minRehashCount = Integer.MAX_VALUE;
    int maxRehashCount = Integer.MIN_VALUE;

    for (int i = 0; i < numPartitions; i++) {
      final LBlockHashTable hashTable = hashAggPartitions[i].hashTable;
      final int size = hashAggPartitions[i].hashTable.size();
      final int rehashCount = hashAggPartitions[i].hashTable.getRehashCount();
      tableCapacity += hashAggPartitions[i].hashTable.capacity();
      rehashTime += hashAggPartitions[i].hashTable.getRehashTime(TimeUnit.NANOSECONDS);
      tableSize += size;
      tableRehashCount += rehashCount;
      if (size < minTableSize) {
        minTableSize = size;
      }
      if (size > maxTableSize) {
        maxTableSize = size;
      }
      if (rehashCount < minRehashCount) {
        minRehashCount = rehashCount;
      }
      if (rehashCount > maxRehashCount) {
        maxRehashCount = rehashCount;
      }
      if (iterations == 1) {
        /* just compute these stats in the first iteration when we consumed incoming
         * data and before we start processing spilled data from disk. After
         * repartitioning and re-inserting during recursion, computing these stats will not
         * add any additional information
         */
        allocatedForFixedBlocks += hashTable.getAllocatedForFixedBlocks();
        unusedForFixedBlocks += hashTable.getUnusedForFixedBlocks();
        allocatedForVarBlocks += hashTable.getAllocatedForVarBlocks();
        unusedForVarBlocks += hashTable.getUnusedForVarBlocks();
      }
    }

    statsHolder.hashTableSize = tableSize;
    statsHolder.hashTableCapacity = tableCapacity;
    statsHolder.maxTotalHashTableSize = Math.max(statsHolder.maxTotalHashTableSize, tableSize);
    statsHolder.maxTotalHashTableCapacity = Math.max(statsHolder.maxTotalHashTableCapacity, tableCapacity);
    statsHolder.hashTableRehashCount = tableRehashCount;
    statsHolder.hashTableRehashTime = rehashTime;
    statsHolder.minHashTableSize = minTableSize;
    statsHolder.maxHashTableSize = maxTableSize;
    statsHolder.minHashTableRehashCount = minRehashCount;
    statsHolder.maxHashTableRehashCount = maxRehashCount;

    if (iterations == 1) {
      statsHolder.allocatedForFixedBlocks = allocatedForFixedBlocks;
      statsHolder.unusedForFixedBlocks = unusedForFixedBlocks;
      statsHolder.allocatedForVarBlocks = allocatedForVarBlocks;
      statsHolder.unusedForVarBlocks = unusedForVarBlocks;
    }
  }

  /**
   * IMPORTANT: all stats should only be defined in
   * {@link com.dremio.sabot.op.aggregate.vectorized.HashAggStats}
   * as we use that in {@link com.dremio.exec.ops.OperatorMetricRegistry}
   * to register each operator's stats which are eventually propagated
   * to profile. OperatorMetricRegistry doesn't care if its vectorized
   * hash agg or row-wise. It just registers all stats under HashAggStats
   * as stats for hashagg operator. So if we define any local stats here
   * using MetricDef interface, they won't be visible anywhere since
   * they are not registered as operator's stats in OperatorMetricRegistry
   */
  private void updateStats(){
    if (hashAggPartitions == null || !initDone) {
      return;
    }

    final OperatorStats stats = context.getStats();

    computeAllStats();
    stats.setLongStat(Metric.NUM_HASH_PARTITIONS, this.numPartitions);
    stats.setLongStat(Metric.NUM_ENTRIES, statsHolder.hashTableSize);
    stats.setLongStat(Metric.MAX_TOTAL_NUM_ENTRIES, statsHolder.maxTotalHashTableSize);
    stats.setLongStat(Metric.NUM_BUCKETS,  statsHolder.hashTableCapacity);
    stats.setLongStat(Metric.MAX_TOTAL_NUM_BUCKETS, statsHolder.maxTotalHashTableCapacity);
    stats.setLongStat(Metric.NUM_RESIZING, statsHolder.hashTableRehashCount);
    stats.setLongStat(Metric.RESIZING_TIME, statsHolder.hashTableRehashTime);
    stats.setLongStat(Metric.MIN_HASHTABLE_ENTRIES, statsHolder.minHashTableSize);
    stats.setLongStat(Metric.MAX_HASHTABLE_ENTRIES, statsHolder.maxHashTableSize);
    stats.setLongStat(Metric.MIN_REHASH_COUNT, statsHolder.minHashTableRehashCount);
    stats.setLongStat(Metric.MAX_REHASH_COUNT, statsHolder.maxHashTableRehashCount);

    stats.setLongStat(Metric.VECTORIZED, 1);
    stats.setLongStat(Metric.PIVOT_TIME, pivotWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.INSERT_TIME, insertWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.ACCUMULATE_TIME, accumulateWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.REVERSE_TIME, 0);
    stats.setLongStat(Metric.UNPIVOT_TIME, unpivotWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.HASHCOMPUTATION_TIME, hashComputationWatch.elapsed(TimeUnit.NANOSECONDS));

    stats.setLongStat(Metric.PREALLOCATED_MEMORY, debug.getTotalPreallocatedMemory());
    stats.setLongStat(Metric.SPILL_COUNT, partitionSpillHandler.getNumberOfSpills());
    stats.setLongStat(Metric.PARTITIONS_SPILLED, partitionSpillHandler.getNumPartitionsSpilled());
    stats.setLongStat(Metric.ITERATIONS, iterations);
    stats.setLongStat(Metric.RAN_OUT_OF_MEMORY, ooms);
    stats.setLongStat(Metric.SPILL_TIME, spillPartitionWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.READ_SPILLED_BATCH_TIME, readSpilledBatchWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.MAX_BATCHES_SPILLED, partitionSpillHandler.getMaxBatchesSpilled());
    stats.setLongStat(Metric.TOTAL_BATCHES_SPILLED, partitionSpillHandler.getTotalBatchesSpilled());
    stats.setLongStat(Metric.MAX_RECORDS_SPILLED, partitionSpillHandler.getMaxRecordsSpilled());
    stats.setLongStat(Metric.TOTAL_RECORDS_SPILLED, partitionSpillHandler.getTotalRecordsSpilled());
    stats.setLongStat(Metric.RECURSION_DEPTH, computeRecursionDepth());
    stats.setLongStat(Metric.SORT_ACCUMULATE_TIME, sortPriorToAccumulateWatch.elapsed(TimeUnit.NANOSECONDS));
    stats.setLongStat(Metric.TOTAL_SPILLED_DATA_SIZE, partitionSpillHandler.getTotalSpilledDataSize());
    stats.setLongStat(Metric.MAX_SPILLED_DATA_SIZE, partitionSpillHandler.getMaxSpilledDataSize());

    if (iterations == 1) {
      stats.setLongStat(Metric.ALLOCATED_FOR_FIXED_KEYS, statsHolder.allocatedForFixedBlocks);
      stats.setLongStat(Metric.UNUSED_FOR_FIXED_KEYS, statsHolder.unusedForFixedBlocks);
      stats.setLongStat(Metric.ALLOCATED_FOR_VARIABLE_KEYS, statsHolder.allocatedForVarBlocks);
      stats.setLongStat(Metric.UNUSED_FOR_VARIABLE_KEYS, statsHolder.unusedForVarBlocks);
      stats.setLongStat(Metric.MAX_VARIABLE_BLOCK_LENGTH, maxVariableBlockLength);
    }
  }

  private class HashTableStatsHolder {
    private int hashTableSize;
    private int hashTableCapacity;
    private int maxTotalHashTableSize = 0;
    private int maxTotalHashTableCapacity = 0;
    private int hashTableRehashCount;
    private long hashTableRehashTime;
    private int minHashTableSize;
    private int maxHashTableSize;
    private int minHashTableRehashCount;
    private int maxHashTableRehashCount;
    private long allocatedForFixedBlocks;
    private long unusedForFixedBlocks;
    private long allocatedForVarBlocks;
    private long unusedForVarBlocks;

    HashTableStatsHolder() { }
  }

  /**
   * This method works at a partition level. Earlier it used to output data from
   * the hash table and accumulators in 1 or more batches. In each iteration we bumped
   * the batch index and upon the next call to outputData(), we checked if we have
   * exhausted the hash table or not. If we haven't then proceed outputting the
   * current batch index and this repeats until a call to outputData() determines
   * that there are no batches remaining in hash table.
   *
   * This above algorithm is now extended to operate at partition level. We output data
   * from one partition at a time and this can be comprised of one or more batches.
   * So outputData() does state maintenance to determine when to move on to the next
   * partition and when we have exhausted all the partitions.
   * @return the number of records in the batch sent back
   * @throws Exception
   *
   *
   * STATE MACHINE EXAMPLES:
   *
   * Let's take three examples for no spill and spill (simple and deep hierarchy)
   *
   *      --------- EXAMPLE 1 (no spill) ----------
   *
   * Partitions: p0, p1, p2, p3
   * active spilled partitions: empty
   * spill queue: empty
   * no partitions were spilled in consumeData()
   * noMoreToConsume()
   * external state: CAN_PRODUCE
   * internal state: OUTPUT_INMEMORY_PARTITIONS
   * outputData()
   * outputPartitions p0, p1, p2, p3
   * postOutputProcessing()
   * external state: DONE
   * internal state: DONE
   *
   *
   *      --------- EXAMPLE 2 (spill with simple hierarchy) ----------
   *
   *      -------- ITERATION 0 ---------
   *
   * Partitions: p0, p1, p2, p3
   * active spilled partitions: empty
   * spill queue: empty
   * Spill partitions p0, p1 and downsize them
   * Partitions: p0, p1, p2, p3
   * active spilled partitions: p0, p1
   * spill queue: empty
   * noMoreToConsume()
   * external state: CAN_PRODUCE
   * internal state: OUTPUT_INMEMORY_PARTITIONS
   * outputData()
   * outputPartitions p2, p3 and downsize them
   * postOutputProcessing()
   * external state: CAN_PRODUCE
   * internal state: TRANSITION_PARTITION_SPILL_STATE
   *
   *      -------- ITERATION 1 ---------
   *
   * outputData()
   * transitionPartitionSpillState()
   *      -- form disjoint sets of "memory only" and "disk only"
   *         partitions.
   *      -- add active spilled partitions p0, p1 to spill queue and
   *         clear the former list
   * active spilled partitions: empty
   * spill queue: p0, p1
   * consumeSpilledData()
   * getNextSpilledPartitionToProcess()
   *      -- check we don't have an open disk iterator
   *      -- remove p0 from spill queue
   *      -- get a brand new disk iterator
   * Partitions: p0.p0, p0.p1, p0.p2, p0.p3
   * spill queue: p1
   * read single batch of spilled partition p0 and process (insert, aggregate etc)
   * no subpartition is spilled
   * external state: CAN_PRODUCE
   * internal state: PROCESS_SPILLED_PARTITION
   *
   * outputData()
   * consumeSpilledData()
   * getNextSpilledPartitionToProcess()
   *      -- check we already have an open disk iterator
   *      -- return it
   * Partitions: p0.p0, p0.p1, p0.p2, p0.p3
   * active spilled partitions: empty
   * spill queue: p1
   * read next batch of spilled partition p0 and process (insert, aggregate etc)
   * no subpartition is spilled
   * external state: CAN_PRODUCE
   * internal state: PROCESS_SPILLED_PARTITION
   *
   * outputData()
   * consumeSpilledData()
   * getNextSpilledPartitionToProcess()
   *      -- check we already have an open disk iterator
   *      -- return it
   * Partitions: p0.p0, p0.p1, p0.p2, p0.p3
   * active spilled partitions: empty
   * spill queue: p1
   * no batches read, we are done with processing all batches from spill file of p0
   * closeDiskIterator()
   * noMoreToConsumeFromSpill()
   * external state: CAN_PRODUCE
   * internal state: OUTPUT_INMEMORY_PARTITIONS
   *
   * outputData()
   * outputPartitions p0.p0, p0.p1, p0.p2, p0.p3 and downsize them
   * postOutputProcessing()
   * external state: CAN_PRODUCE
   * internal state: TRANSITION_PARTITION_SPILL_STATE
   *
   *      -------- ITERATION 2 ---------
   *
   * outputData()
   * transitionPartitionSpillState()
   *      -- NOOP since active spilled partition list is empty
   * consumeSpilledData()
   *    getNextSpilledPartitionToProcess()
   *      -- check we don't have an open disk iterator
   *      -- remove p1 from spill queue
   *      -- get a brand new disk iterator
   * Partitions: p1.p0, p1.p1, p1.p2, p1.p3
   * active spilled partitions: empty
   * spill queue: empty
   * read single batch of spilled partition p1 and process (insert, aggregate etc)
   * no subpartition is spilled
   * external state: CAN_PRODUCE
   * internal state: PROCESS_SPILLED_PARTITION
   *
   * outputData()
   * consumeSpilledData()
   * getNextSpilledPartitionToProcess()
   *      -- check we already have an open disk iterator
   *      -- return it
   * Partitions: p1.p0, p1.p1, p1.p2, p1.p3
   * active spilled partitions: empty
   * spill queue: empty
   * no batches read, we are done with processing all batches from spill file of p1
   * closeDiskIterator()
   * noMoreToConsumeFromSpill()
   * external state: CAN_PRODUCE
   * internal state: OUTPUT_INMEMORY_PARTITIONS
   *
   * outputData()
   * outputPartitions p1.p0, p1.p1, p1.p2, p1.p3 and downsize them
   * postOutputProcessing()
   * external state: DONE
   * internal state: DONE
   *
   * Slightly complex scenario
   *
   *      --------- EXAMPLE 3 (spill with deeper hierarchy) ----------
   *
   *      -------- ITERATION 0 ---------
   *
   * Partitions: p0, p1, p2, p3
   * active spilled partitions: empty
   * spill queue: empty
   * Spill partitions p0, p2 and downsize them
   * Partitions: p0, p1, p2, p3
   * active spilled partitions: p0, p1
   * spill queue: empty
   * noMoreToConsume()
   * external state: CAN_PRODUCE
   * internal state: OUTPUT_INMEMORY_PARTITIONS
   * outputData()
   * outputPartitions p2, p3 and downsize them
   * postOutputProcessing()
   * external state: CAN_PRODUCE
   * internal state: TRANSITION_PARTITION_SPILL_STATE
   *
   *      -------- ITERATION 1 ---------
   *
   * outputData()
   * transitionPartitionSpillState()
   *      -- form disjoint sets of "memory only" and "disk only"
   *         partitions.
   *      -- add active spilled partitions p0, p1 to spill queue and
   *         clear the former list
   * active spilled partitions: empty
   * spill queue: p0, p1
   * consumeSpilledData()
   * getNextSpilledPartitionToProcess
   *      -- check we don't have an open disk iterator
   *      -- remove p0 from spill queue
   *      -- get a brand new disk iterator
   * Partitions: p0.p0, p0.p1, p0.p2, p0.p3
   * spill queue: p1
   * read single batch of spilled partition p0 and process (insert, aggregate etc)
   * subpartition p0.p1 is spilled and downsized
   * external state: CAN_PRODUCE
   * internal state: PROCESS_SPILLED_PARTITION
   *
   * outputData()
   * consumeSpilledData()
   * getNextSpilledPartitionToProcess()
   *      -- check we already have an open disk iterator
   *      -- return it
   * Partitions: p0.p0, p0.p1, p0.p2, p0.p3
   * active spilled partitions: p0.p1
   * spill queue: p1
   * read next batch of spilled partition p0 and process (insert, aggregate etc)
   * no subpartition is spilled
   * external state: CAN_PRODUCE
   * internal state: PROCESS_SPILLED_PARTITION
   *
   * outputData()
   * consumeSpilledData()
   * getNextSpilledPartitionToProcess()
   *      -- check we already have an open disk iterator
   *      -- return it
   * Partitions: p0.p0, p0.p1, p0.p2, p0.p3
   * active spilled partitions: p0.p1
   * spill queue: p1
   * no batches read, we are done with processing all batches from spill file of p0
   * closeDiskIterator()
   * noMoreToConsumeFromSpill()
   * external state: CAN_PRODUCE
   * internal state: OUTPUT_INMEMORY_PARTITIONS
   *
   * outputData()
   * outputPartitions p0.p0, p0.p2, p0.p3 and downsize them
   * postOutputProcessing()
   * external state: CAN_PRODUCE
   * internal state: TRANSITION_PARTITION_SPILL_STATE
   *
   *      --------- ITERATION 2 ---------
   *
   * outputData()
   * transitionPartitionSpillState()
   *      -- add active spilled partitions p0.p1 to spill queue and
   *         clear the former list
   * consumeSpilledData()
   *    getNextSpilledPartitionToProcess()
   *      -- check we don't have an open disk iterator
   *      -- remove p1 from spill queue
   *      -- get a brand new disk iterator
   * Partitions: p1.p0, p1.p1, p1.p2, p1.p3
   * active spilled partitions: empty
   * spill queue: p0.p1
   * read single batch of spilled partition p1 and process (insert, aggregate etc)
   * no subpartition is spilled
   * external state: CAN_PRODUCE
   * internal state: PROCESS_SPILLED_PARTITION
   *
   * outputData()
   * consumeSpilledData()
   * getNextSpilledPartitionToProcess()
   *      -- check we already have an open disk iterator
   *      -- return it
   * Partitions: p1.p0, p1.p1, p1.p2, p1.p3
   * active spilled partitions: empty
   * spill queue: p0.p1
   * no batches read, we are done with processing all batches from spill file of p1
   * closeDiskIterator()
   * noMoreToConsumeFromSpill()
   * external state: CAN_PRODUCE
   * internal state: OUTPUT_INMEMORY_PARTITIONS
   *
   * outputData()
   * outputPartitions p1.p0, p1.p1, p1.p2, p1.p3 and downsize them
   * postOutputProcessing()
   * external state: CAN_PRODUCE
   * internal state: TRANSITION_PARTITION_SPILL_STATE
   *
   *        --------- ITERATION 3 ---------
   *
   * outputData()
   * transitionPartitionSpillState()
   *      -- NOOP since no active spilled partitions
   * consumeSpilledData()
   *    getNextSpilledPartitionToProcess()
   *      -- check we don't have an open disk iterator
   *      -- remove p0.p1 from spill queue
   *      -- get a brand new disk iterator
   * transition state for two sets of partitions
   *      -- this is done only because we got a new iterator
   *         and are beginning to process a spilled partition.
   * Partitions: p0.p1.p0, p0.p1.p1, p0.p1.p2, p0.p1.p3
   * active spilled partitions: empty
   * spill queue: empty
   * read single batch of spilled partition p0.p1 and process (insert, aggregate etc)
   * no subpartitions were spilled
   * external state: CAN_PRODUCE
   * internal state: PROCESS_SPILLED_PARTITION
   *
   * outputData()
   * consumeSpilledData()
   *    getNextSpilledPartitionToProcess()
   *      -- check we already have an open disk iterator
   *      -- return it
   * partitions: p0.p1.p0, p0.p1.p1, p0.p1.p2, p0.p1.p3
   * active spilled partitions: empty
   * spill queue: empty
   * no batches read, we are done with processing all batches from spill file of p0.p1
   * closeDiskIterator()
   * noMoreToConsumeFromSpill()
   * external state: CAN_PRODUCE
   * internal state: OUTPUT_INMEMORY_PARTITIONS
   *
   * outputData()
   * outputPartitions p0.p1.p0, p0.p1.p1, p0.p1.p2, p0.p1.p3
   * postOutputProcessing()
   * external state: DONE
   * internal state: DONE
   *
   */
  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    Preconditions.checkArgument(internalStateMachine != InternalState.DONE &&
      internalStateMachine != InternalState.NONE, "Error: detected invalid internal operator state");

    int records = 0;

    switch (internalStateMachine) {
      case OUTPUT_INMEMORY_PARTITIONS:
        records = outputPartitions();
        break;
      case TRANSITION_PARTITION_SPILL_STATE:
        transitionPartitionSpillState();
        consumeSpilledData();
        records = 0;
        break;
      case PROCESS_SPILLED_PARTITION:
        consumeSpilledData();
        records = 0;
        break;
    }

    return records;
  }

  /**
   * Before starting the next iteration of aggregation,
   * we transition the state of partitions to ensure
   * there are two disjoint sets of partitions --
   * "MEMORY ONLY" and "DISK ONLY".
   */
  private void transitionPartitionSpillState() {
    state.is(State.CAN_PRODUCE);
    Preconditions.checkArgument(internalStateMachine == InternalState.TRANSITION_PARTITION_SPILL_STATE,
      "Error: detected invalid internal operator state");
    partitionSpillHandler.transitionPartitionState();
    internalStateMachine = InternalState.PROCESS_SPILLED_PARTITION;
  }

  /**
   * Helper function for outputData(). Outputs a single
   * partition one batch at a time. Internally manages state
   * to decide when to move to next batch of a partition v/s
   * moving to next partition
   *
   * @return number of records outputted
   */
  private int outputPartitions() throws Exception {

    if(outputPartitionIndex == numPartitions) {
      postOutputProcessing();
      outputPartitionIndex = 0;
      outputBatchCount = 0;
      return 0;
    }

    final VectorizedHashAggPartition partitionToOutput = hashAggPartitions[outputPartitionIndex];

    final LBlockHashTable hashTable = partitionToOutput.hashTable;
    final int hashTableSize = hashTable.size();
    final int hashTableBlocks = hashTable.blocks();

    if (partitionToOutput.isSpilled() || hashTableSize == 0) {
      /* skip partitions that are spilled or not spilled but empty */
      outputPartitionIndex++;
      return 0;
    }

    final int recordsInBatch = hashTable.getRecordsInBatch(outputBatchCount);

    /* unpivot GROUP BY key columns into corresponding vectors in outgoing container */
    unpivotWatch.start();
    partitionToOutput.hashTable.unpivot(outputBatchCount, recordsInBatch);
    unpivotWatch.stop();

    /* transfer accumulation vectors to the target vector in transferPair -- output vector in outgoing container */
    partitionToOutput.accumulator.output(outputBatchCount);

    outputBatchCount++;
    if (outputBatchCount == hashTableBlocks) {
      /* this partition outputted, move onto next partition */
      outputPartitionIndex++;
      /* start from first batch of next partition */
      outputBatchCount = 0;
      /* downsize the partition */
      partitionToOutput.resetToMinimumSize();
    }

    updateStats();

    return outgoing.setAllCount(recordsInBatch);
  }

  /**
   * Once outputData() is done outputting all batches from
   * all (not spilled) partitions, this function decides
   * what should be the next state of internal state machine.
   *
   * if there are one or more active spilled partitions or
   * one or more spilled partitions in FIFO queue then we
   * are not done and need to start the next iteration.
   */
  private void postOutputProcessing() {
    if ((partitionSpillHandler.getActiveSpilledPartitionCount() == 0) && partitionSpillHandler.isSpillQueueEmpty()) {
      /* if we are inside recursion, that is we are outputting after
       * consuming the input from a spilled partition, we need to check if
       * queue is empty. If yes then operator is done and the state machine
       * inside outputData() is terminated. But if spill queue is not empty,
       * then we need to continue with processing spilled partition.
       */
      state = State.DONE;
      internalStateMachine = InternalState.DONE;

      /* propagate some metric back for verification in tests.
       * Note: this is already part of metrics collected here that are
       * later on visible in query profile. We just use a subset of
       * such metrics for verification in oom unit tests.
       */
      final VectorizedHashAggSpillStats spillStats = new VectorizedHashAggSpillStats();
      spillStats.setSpills(partitionSpillHandler.getNumberOfSpills());
      spillStats.setOoms(ooms);
      spillStats.setIterations(iterations);
      spillStats.setRecursionDepth(computeRecursionDepth());
      popConfig.setSpillStats(spillStats);
    } else {
      internalStateMachine = InternalState.TRANSITION_PARTITION_SPILL_STATE;
      /* we need another iteration */
      iterations++;
    }
  }

  private int computeRecursionDepth() {
    String partitionID = hashAggPartitions[0].getIdentifier();
    return partitionID.length()/5;
  }

  /**
   * Once the entire input (all spilled batches) has been consumed from
   * a spilled partition, this method is invoked to indicate that input is
   * over and operator is ready to output data.
   *
   * This is an internal version of noMoreToConsume() where the source
   * is spilled partition.
   *
   * @throws Exception
   */
  private void noMoreToConsumeFromSpill() throws Exception {
    state.is(State.CAN_PRODUCE);
    Preconditions.checkArgument(internalStateMachine == InternalState.PROCESS_SPILLED_PARTITION,
      "Error: detected invalid state machine");
    if (partitionSpillHandler.getActiveSpilledPartitionCount() > 0) {
      try {
        partitionSpillHandler.spillAnyInMemoryDataForSpilledPartitions();
      } catch (Exception e) {
        throw debug.prepareAndThrowException(e, "Error: Failed to spill partition", HashAggErrorType.SPILL_WRITE);
      }
    }
    internalStateMachine = InternalState.OUTPUT_INMEMORY_PARTITIONS;
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    Preconditions.checkArgument(internalStateMachine == InternalState.NONE,
      "Error: detected invalid state machine");

    if(getHashTableSize() == 0){
      /* nothing to output */
      state = State.DONE;
      internalStateMachine = InternalState.DONE;
    }else {
      state = State.CAN_PRODUCE;
      internalStateMachine = InternalState.OUTPUT_INMEMORY_PARTITIONS;
      if (partitionSpillHandler.getActiveSpilledPartitionCount() > 0) {
        try {
          partitionSpillHandler.spillAnyInMemoryDataForSpilledPartitions();
        } catch (Exception e) {
          throw debug.prepareAndThrowException(e, "Error: Failed to spill partition", HashAggErrorType.SPILL_WRITE);
        }
      }
    }
  }

  private enum InternalState {
    NONE,
    OUTPUT_INMEMORY_PARTITIONS,
    PROCESS_SPILLED_PARTITION,
    TRANSITION_PARTITION_SPILL_STATE,
    DONE
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    /* BaseTestOperator calls operator.close() twice on each operator it creates */
    if (!closed) {
      updateStats();
      try {
        AutoCloseables.close(Iterables.concat(
            Arrays.asList(partitionToLoadSpilledData, partitionSpillHandler, sortedHashTableOrdinals, hashTableOrdinals, hashValues, fixedBlockVector, variableBlockVector),
            Arrays.asList(hashAggPartitions),
            outgoing));
      } finally {
        partitionToLoadSpilledData = null;
        partitionSpillHandler = null;
        sortedHashTableOrdinals = null;
        hashTableOrdinals = null;
        hashValues = null;
        fixedBlockVector = null;
        variableBlockVector = null;
        closed = true;
      }
    }
  }

  private static UserException unsup(String msg) {
    throw UserException.unsupportedError().message("Aggregate not supported. %s", msg).build(logger);
  }
}
