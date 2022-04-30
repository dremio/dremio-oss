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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.fn.hll.StatisticsAggrFunctions;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.planner.physical.HashAggMemoryEstimator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.CoordExecRPC.FragmentAssignment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.HashAggSpill;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.TypeValidators.PowerOfTwoLongValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.fragment.OutOfBandMessage.Payload;
import com.dremio.sabot.op.aggregate.vectorized.HashAggStats.Metric;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggDebug.HashAggErrorType;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggPartitionSpillHandler.SpilledPartitionIterator;
import com.dremio.sabot.op.common.ht2.BoundedPivots;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotBuilder.PivotInfo;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.koloboke.collect.hash.HashConfig;

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
 * NOTES ON INTERNAL STATE MACHINE
 *
 * External State of operator: (nothing has changed)
 * DONE,
 * NEEDS_SETUP,
 * CAN_CONSUME
 * CAN_PRODUCE
 *
 * Internal States:
 *
 * NONE -- It could be in any of the external state except DONE
 *
 * OUTPUT_INMEMORY_PARTITIONS -- The operator can output data from the partitions
 * completely resident in memory (the ones that were never spilled)
 *
 * PROCESS_SPILLED_PARTITION -- Take the next input data from a spilled partition
 * and continue with in-memory aggregation
 *
 * TRANSITION_PARTITION_SPILL_STATE -- Before starting an iteration, update
 * the partition identifiers, move the partitions spilled in previous iteration to
 * spill queue
 *
 * RESUME_CONSUMING -- we don't handle OOM completely at one go. spill a single
 * batch from the chosen victim partition, change state to SPILL_NEXT_BATCH and
 * yield. Once the entire partition has been spilled, we come back to this state
 * and resume inserting data into the partitions from the point we failed earlier
 * with OOM. So we continue consuming that batch and accordingly move onto next
 * batch or spill again.
 *
 * SPILL_NEXT_BATCH -- spill next batch from an already chosen victim partition
 * We will be in this state as long as there are batches to spill from the
 * victim partition.
 *
 * FORCE_SPILL_INMEMORY_DATA -- when we finish an iteration and have spilled
 * some partitions, it is quite possible that some/all of the spilled partitions
 * have data in memory (the data that was inserted into the inmemory portion
 * of the spilled partitions after they were spilled). Before we start the next
 * iteration to process the spilled data, we ensure that all spilled partitions
 * are entirely on disk and thus a "force-spill" is needed to flush out
 * in-memory portion of each spilled partition.
 *
 * DONE -- finished processing. at this time external state should also be DONE.
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
 *                    (if some partitions were spilled, make sure
 *                     they are entirely on disk)
 *                               |
 *                               |
 *                               |
 *                               |
 *                               V
 *                    FORCE_SPILL_INMEMORY_DATA
 *
 *
 *   noMoreToConsume() above would have set the operator's external state to
 *   CAN_PRODUCE and now we will expect calls into outputData() until we
 *   set the state to DONE. The following state machine diagram attempts to
 *   show the processing inside outputData and the transition through different
 *   states as the algorithm recurses.
 *
 *
 *   FORCE_SPILL_INMEMORY_DATA---->---(spill inmemory portion of-------->----+
 *        |        ^                     each active spilled partition)       |
 *        |        |                                                          |
 *        |        |                                                          V
 *        |        +-------<-----(move to next partition)------<------SPILL_NEXT_BATCH
 *        |                                                             |         ^
 *    (finished force spilling                                     keep spilling and yielding
 *     inmemory portions of spilled                                     |         |
 *     partitions, now output inmemory only                             |         |
 *     partitions before processing the spilled                         +---->----+
 *     partitions)
 *          |
 *          |
 *          |
 *          |
 *          |                 +------------------------------------------------+
 *          |                 |                                                |
 *          V                 V                                                |
 *      OUTPUT_INMEMORY_PARTITIONS<-----------------+                          |
 *          |         |        |                    |                          |
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
 *          |                                       |     |    +------------>---------+
 *          |                                       V     |    |                      |
 *          |                           PROCESS_SPILLED_PARTITION<------+             |
 *          |                                       |                   |             |
 *          |                                (read a single batch and process         |
 *          |                                  by feeding into the operator)          |
 *          V                                       |_________________|               |
 *        DONE                                                                        recurse
 *                                                                                hit oom so spill
 *                                                                                   and yield
 *        NOTE: state machine has been augmented to support micro spilling.           |
 *        the high level idea/algorithm shown in above diagram is still the same.     |
 *        following diagram just depicts state transitions as micro spilling          |
 *        comes into action                                                           |
 *                                                                                    |
 *        states shown as: (external state, internal state)                           V
 *                                                                                    |
 *                                                                                    |
 *                                                              keep spilling and     |
 *                                                                    yielding        |
 *                                                                    +--------+      |
 *                                                                    |        |      |
 *                                                                    V        |      V
 *     |------>(CAN_CONSUME, NONE)---hit oom so-------------->(CAN_PRODUCE, SPILL_NEXT_BATCH)<-+
 *     |                             spill little and yield         |                 ^        |
 *     |                                                            |                 |        |
 *     |                                                            |                 |        |
 *    finished consuming from                                       |                 |        |
 *    failure point,                                                |                 |        |
 *    take next batch from pipeline                             finished spilling     |        |
 *     |                                                        now resume insertion  |        |
 *     |                                                        from failure point    |        |
 *     |                                                             |                |        |
 *     |                                                             |                |        |
 *     +------(CAN_PRODUCE, RESUME_CONSUMING)<-----------------------+                |        |
 *                            |           |                                           |        |
 *                            |           |                                           |        |
 *                            |           +--------------hit oom so spill-------------+        |
 *                            |                          little and yield                      |
 *                            |                                                                |
 *             finished consuming from failure point,                                          |
 *             take next batch from spilled data                                               |
 *                            |                                                                |
 *                            +-----(CAN_PRODUCE, PROCESS_SPILLED_PARTITION)----hit oom so-----+
 *                                                                              spill little
 *                                                                              and yield
 *
 *
 ************************************************************************************************/

@Options
public class VectorizedHashAggOperator implements SingleInputOperator {
  public interface VarLenVectorResizer {
    boolean tryResize(int accumIndex, int newSize);
  }

  public class VarLenVectorResizerImpl implements VarLenVectorResizer {
    @Override
    public boolean tryResize(int accumIndex, int newSize) {
      try {
        while (tempAccumulatorHolder[accumIndex].getByteCapacity() < newSize) {
          tempAccumulatorHolder[accumIndex].reallocDataBuffer();
        }

        FieldVector[] partitionToLoadAccumulator = partitionToLoadSpilledData.getPostSpillAccumulatorVectors();
        while (((BaseVariableWidthVector)partitionToLoadAccumulator[accumIndex]).getByteCapacity() < newSize) {
          ((BaseVariableWidthVector)partitionToLoadAccumulator[accumIndex]).reallocDataBuffer();
        }

        return true;
      } catch (Exception e) {
        /*
         * This can be any exception. The idea here is, when this API is called, VarLenAccumulator is
         * trying to determine to increase the it's vector size for the new batch. If for any reason
         * could not increase the tempAccumulatorHolder (to save the data in record order and then
         * spilled) or partitionToLoadSpillData (accumulator to read the previously spilled data
         * from disk), then VarLenAccumulator cannot increase it's vector size.
         */
        logger.debug("Failed to extend temporary accumulator or partition load accumulator");
      }
      return false;
    }
  }

  private static final ControlsInjector injector =
    ControlsInjectorFactory.getInjector(VectorizedHashAggOperator.class);

  @VisibleForTesting
  public static final String INJECTOR_SETUP_OOM_ERROR = "setupOOMError";

  public static final PowerOfTwoLongValidator VECTORIZED_HASHAGG_NUMPARTITIONS = new PowerOfTwoLongValidator("exec.operator.aggregate.vectorize.num_partitions", 32, 8);
  /* concept of batch internal to vectorized hashagg operator and hashtable to manage the memory allocation.
   * as an example, if incoming batch size is 4096 and the HashAgg batch size is computed to be 1024,
   * consumeData() will internally treat this as 4 batches inserted into hash table and accumulator.
   * This is used to reduce the pre-allocated memory for all partitions.
   *
   * The max batch size bytes is for one partitions i.e if this value is 2MB, and
   * there are 8 partitions, the total for all all partitions would be 2*8 = 16 MB.
   */
  public static final PositiveLongValidator VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES = new PositiveLongValidator("exec.operator.aggregate.vectorize.max_hashtable_batch_size_bytes", Integer.MAX_VALUE, 1 * 1024 * 1024);

  /* When running on large datasets with limited amount of memory (and thus excessive spilling), this setting
   * will generate huge amount of debug information potentially resulting in out of heap memory error.
   * The setting also has a noticeable performance impact on queries on large datasets.
   */
  public static final BooleanValidator VECTORIZED_HASHAGG_DEBUG_DETAILED_EXCEPTION = new BooleanValidator("exec.operator.aggregate.vectorize.tracedetails_on_exception", false);
  public static final PositiveLongValidator VECTORIZED_HASHAGG_DEBUG_MAX_OOMEVENTS = new PositiveLongValidator("exec.operator.aggregate.vectorize.debug_max_oomevents", 10000, 500);
  public static final BooleanValidator VECTORIZED_HASHAGG_MINIMIZE_DISTINCT_SPILLED_PARTITIONS = new BooleanValidator("exec.operator.aggregate.vectorize.minimize_spilled_partitions", true);
  public static final BooleanValidator VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR = new BooleanValidator("exec.operator.aggregate.vectorize.use_spilling_operator", true);
  public static final PowerOfTwoLongValidator VECTORIZED_HASHAGG_JOINT_ALLOCATION_MIN = new PowerOfTwoLongValidator("exec.operator.aggregate.vectorize.joint_allocation_min", 4*1024, 4*1024);
  public static final PowerOfTwoLongValidator VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX = new PowerOfTwoLongValidator("exec.operator.aggregate.vectorize.joint_allocation_max", 1024*1024, 64*1024);
  public static final BooleanValidator VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT = new BooleanValidator("exec.operator.aggregate.vectorize.use_minimum_as_limit", false);

  // how close this allocation has to be to the spilling operator to trigger a spill.
  public static final DoubleValidator OOB_SPILL_TRIGGER_FACTOR = new RangeDoubleValidator("exec.operator.aggregate.vectorize.oob_trigger_factor", 0.0d, 10.0d, .75d);
  public static final DoubleValidator OOB_SPILL_TRIGGER_HEADROOM_FACTOR = new RangeDoubleValidator("exec.operator.aggregate.vectorize.oob_trigger_headroom_factor", 0.0d, 10.0d, .2d);
  public static final BooleanValidator OOB_SPILL_TRIGGER_ENABLED = new BooleanValidator("exec.operator.aggregate.vectorize.oob_trigger_enabled", true);
  public static final BooleanValidator VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS = new BooleanValidator("exec.operator.aggregate.vectorize.enable_micro_spills", true);
  /*
   * If variable column records size is much larger then default (15) size, let the vector created for new batches
   * can go up to 1M (256 * 4K). Config option can be used to reduce, if really needed.
   */
  public static final PositiveLongValidator VECTORIZED_HASHAGG_MAX_VARIABLE_SIZE =
    new PositiveLongValidator("exec.operator.aggregate.vectorize.max_variable_size", 256, 256);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorizedHashAggOperator.class);

  private final OperatorContext context;
  private VectorContainer outgoing;
  private final HashAggregate popConfig;

  private final Stopwatch pivotWatch = Stopwatch.createUnstarted();
  private final Stopwatch insertWatch = Stopwatch.createUnstarted();
  private final Stopwatch accumulateWatch = Stopwatch.createUnstarted();
  private final Stopwatch unpivotWatch = Stopwatch.createUnstarted();
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
  private final int maxVariableWidthKeySize;
  private final VarLenVectorResizerImpl varLenVectorResizer = new VarLenVectorResizerImpl();
  private int maxHashTableBatchSize;

  private int hashPartitionMask;
  private final HashTableStatsHolder statsHolder;
  private int outputPartitionIndex;
  private int outputBatchIndex;
  private int iterations;
  private int ooms;
  private int oobSends;
  private int oobReceives;
  private int oobDropLocal;
  private int oobDropWrongState;
  private int oobDropUnderThreshold;
  private int oobDropNoVictim;
  private int oobSpills;
  private int oobDropSpill;
  private final BufferAllocator allocator;
  private final VectorizedHashAggDebug debug;
  private boolean closed;


  /* preallocated data structures for hash table insertion */
  private FixedBlockVector fixedBlockVector;
  private VariableBlockVector variableBlockVector;

  @VisibleForTesting
  public static final int PARTITIONINDEX_HTORDINAL_WIDTH = 8;
  public static final int HTORDINAL_OFFSET = 0;
  public static final int KEYINDEX_OFFSET = 4;
  public static final int SKETCH_ACCURACY = StatisticsAggrFunctions.HLL_ACCURACY;
  public static final TgtHllType SKETCH_HLLTYPE = TgtHllType.HLL_8;
  public static final int SKETCH_SIZE = HllSketch.getMaxUpdatableSerializationBytes(SKETCH_ACCURACY, SKETCH_HLLTYPE);

  /* cache widely used hashtable info */
  private int maxVariableBlockLength;

  private final BufferAllocator outputAllocator;
  private boolean initDone;
  private final boolean minimizeSpilledPartitions;

  private final long jointAllocationMin;
  private final long jointAllocationLimit;
  private final boolean decimalV2Enabled;

  private final boolean setLimitToMinReservation;
  private VectorizedHashAggPartition ongoingVictimPartition;
  private final boolean enableSmallSpills;
  private ResumableInsertState resumableInsertState;
  private OperatorStateBeforeOOB operatorStateBeforeOOB;
  private ForceSpillState forceSpillState;
  /**
   * This is used to read/write a spilled varlen accumulator from/to disk.
   * Preallocate its memory using the default variable width size however if
   * any batch dynamically increased the size of the vector, this vector must
   * also increased as a prerequsite.
   * One vector for each variable length accumulator and the same used across
   * all partitions. The contents of MutableVarcharVector are copied into this,
   * before being spilled. (MutableVarcharVector cant be spilled to disk
   * directly as they are not in order).
   */
  private BaseVariableWidthVector[] tempAccumulatorHolder;
  /**
   * The buffer capacity of the varlen accumulator
   */
  private boolean hasVarLenAccumAppend = false;

  private int bitsInChunk;
  private int chunkOffsetMask;

  public static final String OUT_OF_MEMORY_MSG = "Vectorized Hash Agg ran out of memory";

  public static final String PREALLOC_FAILURE_PARTITIONS = "Error: Failed to preallocate minimum memory in vectorized hashagg for single batch in all partitions";
  public static final String PREALLOC_FAILURE_LOADING_PARTITION = "Error: Failed to preallocate minimum memory in vectorized hashagg for extra partition";
  public static final String PREALLOC_FAILURE_AUX_STRUCTURES = "Error: Failed to preallocate minimum memory in vectorized hashagg for auxiliary structures";

  public VectorizedHashAggOperator(HashAggregate popConfig, OperatorContext context) throws ExecutionSetupException {
    final OptionManager options = context.getOptions();
    this.context = context;
    this.allocator = context.getAllocator();
    this.popConfig = popConfig;
    this.numPartitions = (int)options.getOption(VECTORIZED_HASHAGG_NUMPARTITIONS);
    this.minHashTableSize = (int)options.getOption(ExecConstants.MIN_HASH_TABLE_SIZE);
    this.minHashTableSizePerPartition = (int)Math.ceil((minHashTableSize * 1.0)/numPartitions);
    this.estimatedVariableWidthKeySize = (int)options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
    this.maxVariableWidthKeySize = (int)options.getOption(VECTORIZED_HASHAGG_MAX_VARIABLE_SIZE);
    this.maxHashTableBatchSize = popConfig.getHashTableBatchSize();
    final boolean traceOnException = options.getOption(VECTORIZED_HASHAGG_DEBUG_DETAILED_EXCEPTION);
    this.hashPartitionMask = numPartitions - 1;
    this.statsHolder = new HashTableStatsHolder();
    this.outputPartitionIndex = 0;
    this.outputBatchIndex = 0;
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
    /* joint allocation limit should be a power of 2, 0 is allowed as we treat that as no limit and is also
     * used by non spilling vectorized hash agg
     */
    this.jointAllocationMin = options.getOption(VECTORIZED_HASHAGG_JOINT_ALLOCATION_MIN);
    this.jointAllocationLimit = options.getOption(VECTORIZED_HASHAGG_JOINT_ALLOCATION_MAX);
    this.setLimitToMinReservation = options.getOption(VECTORIZED_HASHAGG_USE_MINIMUM_AS_LIMIT);
    this.decimalV2Enabled = options.getOption(PlannerSettings.ENABLE_DECIMAL_V2);
    this.ongoingVictimPartition = null;
    this.enableSmallSpills = options.getOption(VECTORIZED_HASHAGG_ENABLE_MICRO_SPILLS);
    this.resumableInsertState = null;
    this.operatorStateBeforeOOB = null;
    this.forceSpillState = null;
    logger.debug("partitions:{}, min-hashtable-size:{}, max-hashtable-batch-size:{} variable-width-key-size:{}",
      numPartitions, minHashTableSize, maxHashTableBatchSize, estimatedVariableWidthKeySize);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    state.is(State.NEEDS_SETUP);
    this.incoming = accessible;
    this.pivot = createPivot();

    injector.injectChecked(context.getExecutionControls(), INJECTOR_SETUP_OOM_ERROR,
      OutOfMemoryException.class);

    debug.setInfoBeforeInit(allocator.getInitReservation(), allocator.getLimit(),
                            estimatedVariableWidthKeySize, pivot.getVariableCount(),
                            pivot.getBlockWidth(), minHashTableSize,
                            numPartitions, minHashTableSizePerPartition);
    initStructures();
    partitionSpillHandler = new VectorizedHashAggPartitionSpillHandler(
      hashAggPartitions, context.getFragmentHandle(),
      context.getOptions(), context.getConfig(),
      popConfig.getProps().getLocalOperatorId(), partitionToLoadSpilledData,
      context.getSpillService(), minimizeSpilledPartitions, context.getStats());
    debug.setInfoAfterInit(maxHashTableBatchSize, allocator.getAllocatedMemory(), outgoing.getSchema());
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
    debug.setMaxVarBlockLength(this.maxVariableBlockLength); //for debugging purpose
    this.bitsInChunk = hashAggPartitions[0].hashTable.getBitsInChunk();
    this.chunkOffsetMask = hashAggPartitions[0].hashTable.getChunkOffsetMask();
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
      AccumulatorBuilder.getAccumulatorTypesFromExpressions(context.getClassProducer(),
        popConfig.getAggrExprs(), incoming);

    final List<Field> outputVectorFields = materializeAggExpressionsResult.outputVectorFields;
    final byte[] accumulatorTypes = materializeAggExpressionsResult.getAccumulatorTypes();

    /* Adjust maxHashTableBatchSize, if NDV is present */
    for (int i = 0; i < accumulatorTypes.length; ++i) {
      if (accumulatorTypes[i] == AccumulatorBuilder.AccumulatorType.HLL.ordinal() ||
        accumulatorTypes[i] == AccumulatorBuilder.AccumulatorType.HLL_MERGE.ordinal()) {
        final int maxOutgoingBatchSize = (int)context.getOptions().getOption(VectorizedHashAggOperator.VECTORIZED_HASHAGG_MAX_BATCHSIZE_BYTES);
        this.maxHashTableBatchSize = maxOutgoingBatchSize / SKETCH_SIZE;
        break;
      }
    }

    for (int i = 0; i < accumulatorTypes.length; ++i) {
      final FieldVector outputVector = TypeHelper.getNewVector(outputVectorFields.get(i), outputAllocator);
      outgoing.add(outputVector);
    }
    outgoing.buildSchema();

    final HashAggMemoryEstimator estimator = HashAggMemoryEstimator.create(
      new PivotInfo(pivot.getBlockWidth(), pivot.getVariableCount()),
      materializeAggExpressionsResult,
      maxHashTableBatchSize,
      context.getOptions()
    );
    debug.setPreAllocEstimator(estimator);

    /* Now allocate temporary buffers for accumulators which output variable size data */
    tempAccumulatorHolder = new BaseVariableWidthVector[accumulatorTypes.length];
    for (int i = 0; i < accumulatorTypes.length; ++i) {
      final TypeProtos.MinorType type = CompleteType.fromField(outputVectorFields.get(i)).toMinorType();
      if (type == TypeProtos.MinorType.VARCHAR || type == TypeProtos.MinorType.VARBINARY) {
        final int accumLen;
        if (accumulatorTypes[i] == AccumulatorBuilder.AccumulatorType.MIN.ordinal() ||
          accumulatorTypes[i] == AccumulatorBuilder.AccumulatorType.MAX.ordinal()) {
          tempAccumulatorHolder[i] = new VarCharVector("holder", allocator);
          accumLen = estimatedVariableWidthKeySize * maxHashTableBatchSize;
          hasVarLenAccumAppend = true;
        } else {
          Preconditions.checkArgument(accumulatorTypes[i] == AccumulatorBuilder.AccumulatorType.HLL.ordinal() ||
            accumulatorTypes[i] == AccumulatorBuilder.AccumulatorType.HLL_MERGE.ordinal());
          tempAccumulatorHolder[i] = new VarBinaryVector("holder", allocator);
          accumLen = SKETCH_SIZE * maxHashTableBatchSize;
        }
        tempAccumulatorHolder[i].allocateNew(accumLen, maxHashTableBatchSize);
      } else {
        tempAccumulatorHolder[i] = null;
      }
    }

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
      final ArrowBuf combined = allocator.buffer(numPartitions * PARTITIONINDEX_HTORDINAL_WIDTH * maxHashTableBatchSize);
      rollbackable.add(combined);

      final int maxVarWidthVecUsagePercent = (int)context.getOptions().getOption(ExecConstants.VARIABLE_WIDTH_VECTOR_MAX_USAGE_PERCENT);
      for (int i = 0; i < numPartitions; i++) {
        final AccumulatorSet accumulator = AccumulatorBuilder.getAccumulator(
          allocator, outputAllocator, materializeAggExpressionsResult, outgoing,
          maxHashTableBatchSize, jointAllocationMin, jointAllocationLimit,
          decimalV2Enabled, estimatedVariableWidthKeySize, maxVariableWidthKeySize,
          maxVarWidthVecUsagePercent, tempAccumulatorHolder, varLenVectorResizer);
        /* this step allocates memory for control structure in hashtable and reverts itself if
         * allocation fails so we don't have to rely on rollback closeable
         */
        final LBlockHashTable hashTable = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator,
          minHashTableSizePerPartition, estimatedVariableWidthKeySize, true,
          maxHashTableBatchSize);
        hashTable.registerResizeListener(accumulator);
        final String partitionIdentifier = "P" + String.format("%03d", i);

        final ArrowBuf buffer = combined.slice(i * PARTITIONINDEX_HTORDINAL_WIDTH * maxHashTableBatchSize,
            PARTITIONINDEX_HTORDINAL_WIDTH * maxHashTableBatchSize);

        final VectorizedHashAggPartition hashAggPartition =  new VectorizedHashAggPartition(
          accumulator, hashTable, pivot.getBlockWidth(), partitionIdentifier,
          buffer, decimalV2Enabled);
        this.hashAggPartitions[i] = hashAggPartition;
        /* add partition to rollbackable before preallocating because if preallocation
         * fails, we still need to release memory for control structures in hashtable
         * allocated during hashtable instantiation
         */
        rollbackable.add(hashAggPartition);
        /* preallocate memory for single batch, hashtable will internally do this for accumulator */
        hashTable.preallocateSingleBatch();
      }
      combined.close();
      rollbackable.commit();
    } catch (OutOfMemoryException e) {
      ooms++;
      throw debug.prepareAndThrowException(e, PREALLOC_FAILURE_PARTITIONS, HashAggErrorType.OOM);
    }

    final long memoryAfterCreatingPartitions = allocator.getAllocatedMemory();
    debug.setPreallocatedMemoryForPartitions(memoryAfterCreatingPartitions - memoryUsageBeforeInit);

    /* STEP 3: grab commonly used hashtable info */
    setLocalInfoForHashTable();

    /* STEP 4: Build extra partition (aka loading or read partition) */
    allocateExtraPartition(outputVectorFields, accumulatorTypes);

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
  private void allocateExtraPartition(final List<Field> postSpillAccumulatorVectorFields,
                                      final byte[] accumulatorTypes) throws Exception {
    Preconditions.checkArgument(partitionToLoadSpilledData == null, "Error: loading partition has already been pre-allocated");
    final int fixedWidthDataRowSize = pivot.getBlockWidth();
    final int fixedBlockSize = fixedWidthDataRowSize * maxHashTableBatchSize;
    final int variableBlockSize = maxVariableBlockLength;
    try {
      partitionToLoadSpilledData = new PartitionToLoadSpilledData(allocator, fixedBlockSize, variableBlockSize,
        postSpillAccumulatorVectorFields, accumulatorTypes, maxHashTableBatchSize,
        estimatedVariableWidthKeySize * maxHashTableBatchSize);
    } catch (OutOfMemoryException e) {
      ooms++;
      throw debug.prepareAndThrowException(e, PREALLOC_FAILURE_LOADING_PARTITION, HashAggErrorType.OOM);
    }
  }

  /**
   * When inserting data into hashtable (both during initial iteration
   * of aggregation and post-spill processing), we need auxiliary data structures
   * for inserting keys into the hash table. These auxiliary structures are
   * used as follows every time we receive an incoming batch
   *
   * (1) fixedBlockVector - fixed block buffer to store the pivoted fixed width
   *     key column values.
   *
   * (2) variableBlockVector - variable block buffer to store the pivoted
   *     variable width key column values.

   * The reason we need (1) and (2) is because we pivot into temporary space
   * and later do memcpy() to hashtable buffers/blocks during insertion. So we
   * don't want to allocate temporary buffers every time upon entry into
   * consumeData().
   */
  private void allocateMemoryForHashTableInsertion() throws Exception {
    try(AutoCloseables.RollbackCloseable rollbackable = new AutoCloseables.RollbackCloseable()) {
      fixedBlockVector = new FixedBlockVector(allocator, pivot.getBlockWidth(), maxHashTableBatchSize, true);
      rollbackable.add(fixedBlockVector);
      variableBlockVector = new VariableBlockVector(allocator, pivot.getVariableCount(), maxVariableBlockLength, true);
      rollbackable.commit();
    } catch (OutOfMemoryException e) {
      fixedBlockVector = null;
      variableBlockVector = null;
      ooms++;
      throw debug.prepareAndThrowException(e, PREALLOC_FAILURE_AUX_STRUCTURES, HashAggErrorType.OOM);
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
    Preconditions.checkState(resumableInsertState == null, "Error: not expecting resumable insert state");

    // ensure that none of the variable length vectors are corrupt so we can avoid doing bounds checking later.
    for(FieldVector v : vectorsToValidate){
      VariableLengthValidator.validateVariable(v, records);
    }
    consumeDataHelper(records);
  }

  /**
   * Helper function for consuming incoming data from pipeline.
   * With micro spilling, this function also resumes consuming data
   * from an earlier failure (OOM) point.
   *
   * @param records number of records to consume
   * @throws Exception
   */
  private void consumeDataHelper(final int records) throws Exception {
    final FixedBlockVector fixedBlockVector = this.fixedBlockVector;
    final VariableBlockVector variableBlockVector = this.variableBlockVector;
    final long keyFixedVectorAddr = fixedBlockVector.getMemoryAddress();
    final long keyVarVectorAddr = variableBlockVector.getMemoryAddress();

    int recordsConsumed = 0;
    int recordsPivoted = 0;

    /* first check if we need to finish consuming some records from previous batch */
    if (resumableInsertState != null) {
      recordsPivoted = resumableInsertState.recordsPivoted;
      recordsConsumed = resumableInsertState.recordsConsumed;
      final int resumeFromIndex = resumableInsertState.resumeFromIndex;
      final long partitionsUsedMask = resumableInsertState.partitionUsedMask;

      logger.debug("Resume consuming data from pipeline with state: records:{}, recordsPivoted:{}, recordsConsumed:{}, resumeFromIndex:{}, partitionsUsedMask:{}",
                   records, recordsPivoted, recordsConsumed, resumeFromIndex, partitionsUsedMask);

      /* we might recurse (again create a resumable state if we hit OOM below in insertIntoPartitions).
       * since we only need at most one global resumable insert state, we can set the current state to null.
       */
      resumableInsertState = null;

      /* attempt to consume remaining (already pivoted) data */
      long partitionsUsed = insertIntoPartitions(records, recordsPivoted, keyFixedVectorAddr,
        keyVarVectorAddr, recordsConsumed, resumeFromIndex, partitionsUsedMask, false, 0);

      if (internalStateMachine == InternalState.SPILL_NEXT_BATCH) {
        /* we hit OOM and it wasn't handled completely (micro spilling)
         * so we need to stop consuming here.
         */
        state.is(State.CAN_PRODUCE);
        Preconditions.checkState(resumableInsertState != null, "Should have created a valid state to resume insertion");
        updateStats();
        return;
      }

      /* accumulate data for all partitions */
      accumulateForAllPartitions(partitionsUsed);
      /* prepare for next iteration */
      resetPivotStructures();
      /* track the number of records consumed in this batch */
      recordsConsumed += recordsPivoted;
    }

    /* continue consuming the subsequent records (if any) from the same batch */
    while (recordsConsumed != records) {
      /*
       * consumption of 4K records will happen in multiple steps and the
       * pre-allocated memory (pivot space, ordinal array, hashvalue vector) used in each
       * step is allocated based on maxHashTableBatchSize and cannot be expanded beyond that.
       */
      final int stepSize = Math.min(maxHashTableBatchSize, records - recordsConsumed);

      /* STEP 1: first we pivot, this step is unrelated to partitioning */
      pivotWatch.start();
      recordsPivoted = BoundedPivots.pivot(pivot, recordsConsumed, stepSize, fixedBlockVector, variableBlockVector);
      pivotWatch.stop();

      /* STEP 2: then we hash partition the dataset and add pivoted data to multiple hash tables */
      long partitionsUsed = insertIntoPartitions(records, recordsPivoted, keyFixedVectorAddr,
        keyVarVectorAddr, recordsConsumed, 0, 0, false, 0);

      if (internalStateMachine == InternalState.SPILL_NEXT_BATCH) {
        /* insertion for this set of pivoted records failed in between.
         * we will resume later from the failure point for these set of records and any
         * subsequent records.
         */
        state.is(State.CAN_PRODUCE);
        Preconditions.checkState(resumableInsertState != null, "Should have a valid state to resume insertion");
        updateStats();
        return;
      }

      /* STEP 3: then we do accumulators for all partitions in a single pass */
      accumulateForAllPartitions(partitionsUsed);

      recordsConsumed += recordsPivoted;

      /* STEP 4: prepare for next iteration */
      resetPivotStructures();
    }

    /* STEP 5: update hashtable stats */
    updateStats();
  }

  private void resetPivotStructures() {
    fixedBlockVector.reset();
    variableBlockVector.reset();
  }

  /**
   * This function inserts a set of pivoted records into multiple partitions.
   * The number of such pivoted records is controlled by consumeData() and could
   * be the total number of records received by the operator in consumeData() or
   * some contiguous subset of it.
   *
   * We distribute the data across a fixed number of partitions. The partition index
   * is computed using higher order 32 bits from 64 bit hashvalue. Subsequently key data is
   * inserted into the hashtable of corresponding partition.
   *
   * @param records total number of records to consume by the operator
   * @param recordsPivoted number of records to insert into hashtable
   * @param keyFixedVectorAddr starting address of buffer that stores fixed width pivoted keys
   * @param keyVarVectorAddr starting address of buffer that stores variable width pivoted keys
   * @param recordsConsumed total number of records consumed so far from the incoming batch
   *                        this is used to track the absolute index of record in incoming
   *                        batch for accumulation
   * @param insertStartIndex insertion will begin from this index (relative to the set of pivoted
   *                         records).
   * @param partitionsUsedMask bitmap indicating which partitions have been used. used when resuming insertion
   * @param processingSpilledData true if the incoming is from spilled partition, false if it is from pipeline
   *
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
   *          to pass the latter index to accumulate() method. To correctly compute the
   *          absolute index of 517, we need to know how many records have already been
   *          consumed (in this case 512) and that's how recordsConsumed argument helps.
   *
   *          This is also why the loop below starts inserting from 0th index
   *          for a given set of pivoted records since the caller follows the
   *          sequence of  -- pivot, insert, accumulate, clear buffers, repeat.
   *          recordsConsumed helps us to know the absolute index of a record.
   *
   * Now let's take two examples of failure where the insertion within a pivoted
   * batch may not start from 0th index.
   *
   * 1. Say we failed at 300th record in the first pivoted batch [0-511]. We spilled
   *    a batch and built state to resume insertion later on when victim partition
   *    has been completely spilled.
   *    recordsConsumed - 0
   *    resumeFromIndex - 300 and this is where the loop will start from
   *    next time. accordingly, we advance the offset buffer that had stored
   *    the HT ordinal info for previous 300 records [0-299] in this batch
   *
   * 2. Say we succeeded in inserting the first pivoted batch [0-511]
   *    and now start inserting the next set of pivoted records [512-1023]
   *
   *    recordsConsumed - 512
   *    insertStartIndex - 0
   *
   *    No we fail at 250th record in this batch of 512 records. Again we build
   *    state to resume insertion
   *
   *    recordsConsumed - 512
   *    resumeFromIndex - 250
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
  private long insertIntoPartitions(final int records, final int recordsPivoted, final long keyFixedVectorAddr,
                                    final long keyVarVectorAddr, final int recordsConsumed,
                                    final int insertStartIndex, final long partitionsUsedMask,
                                    final boolean processingSpilledData, final long seed) {
    final int blockWidth = pivot.getBlockWidth();
    final int dataWidth = fixedOnly ? blockWidth : blockWidth - LBlockHashTable.VAR_OFFSET_SIZE;
    final boolean fixedOnly = this.fixedOnly;

    long keyFixedAddr = keyFixedVectorAddr + (insertStartIndex * blockWidth);
    long keyVarAddr;
    int keyVarLen;
    long partitionsUsed = partitionsUsedMask;

    insertWatch.start();
    insertAllRecords:
    {
      for (int keyIndex = insertStartIndex; keyIndex < recordsPivoted; keyIndex++, keyFixedAddr += blockWidth) {
        final long keyHash;
        if (fixedOnly) {
          keyHash = LBlockHashTable.fixedKeyHashCode(keyFixedAddr, dataWidth, seed);
          keyVarAddr = -1;
          keyVarLen = 0;
        } else {
          keyVarAddr = keyVarVectorAddr + PlatformDependent.getInt(keyFixedAddr + dataWidth);
          keyVarLen = PlatformDependent.getInt(keyVarAddr);
          keyHash = LBlockHashTable.keyHashCode(keyFixedAddr, dataWidth, keyVarAddr, keyVarLen, seed);
        }

        /* get the partition index from higher order bits in hash */
        final int hashPartitionIndex = ((int) (keyHash >> 32)) & hashPartitionMask;
        final VectorizedHashAggPartition partition = hashAggPartitions[hashPartitionIndex];
        final LBlockHashTable table = partition.hashTable;
        int varFieldLen = 0;

        if (hasVarLenAccumAppend) {
          final List<Accumulator> varLenAccums = partition.accumulator.getVarlenAccumChildren();
          for (int i = 0; i < varLenAccums.size(); ++i) {
            if (varLenAccums.get(i) instanceof BaseNdvAccumulator ||
              varLenAccums.get(i) instanceof  BaseNdvUnionAccumulator) {
              continue;
            }
            final BaseVarBinaryAccumulator bvb = ((BaseVarBinaryAccumulator) varLenAccums.get(i));
            final BaseVariableWidthVector inVec = (BaseVariableWidthVector) bvb.getInput();
            /* Use keyIndex + recordsConsumed to read the input record length */
            final int inputLen = inVec.getValueLength(keyIndex + recordsConsumed);
            if (varFieldLen < inputLen) {
              varFieldLen = inputLen;
            }
          }
        }
        partition.setVarFieldLen(varFieldLen);

        boolean insertSuccessful = false;
        while (!insertSuccessful) {
          try {
            // XXX: directly pass the absolute address of the record in pivot buffer to hash table
            final int ordinal = table.getOrInsertWithAccumSpaceCheck(keyFixedAddr, keyVarAddr, keyVarLen,
              (int) keyHash, dataWidth, seed);

            /* Now update the new space usage */
            if (varFieldLen > 0) {
              partition.addBatchUsedSpace(table.getBatchIndexForOrdinal(ordinal), varFieldLen);
            }
            /* insert successful so store the tuple of <hash table ordinal, incoming key index> */
            /* set the bit to remember the target partitions, this will be used later during accumulation */
            partitionsUsed = partitionsUsed | (1 << hashPartitionIndex);
            hashAggPartitions[hashPartitionIndex].appendRecord(ordinal, keyIndex + recordsConsumed);
            insertSuccessful = true;
          } catch (OutOfMemoryException e) {
            ooms++;
            debug.recordOOMEvent(iterations, ooms, allocator.getAllocatedMemory(), hashAggPartitions, partitionSpillHandler);
            logger.debug("Error: ran out of memory while inserting in hashtable, records to insert:{}, current record index:{}, absolute record index:{}, error: {}",
                         recordsPivoted, keyIndex, keyIndex + recordsConsumed, e);

            /* handle out of memory condition */
            final boolean oomHandled = handleOutOfMemory(hashPartitionIndex);
            if (!oomHandled) {
              /* If micro spilling is enabled then OOM will not be handled all at once
               * since after spilling a single batch of the chosen victim partition,
               * we will yield control and resume spilling (subsequent batches of the victim
               * partition) later when the pipeline is pumped again.
               *
               * Since the OOM is not handled immediately, we can't continue inserting records in
               * this for loop. So we need to build some state to resume insertion later on when
               * the OOM has been handled completely -- all batches of the victim partition have
               * been spilled.
               */
              buildResumableInsertState(records, recordsPivoted, recordsConsumed, keyIndex, partitionsUsed, processingSpilledData);
              break insertAllRecords;
            }
          }
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
   * @param failedPartitionIndex partition that we earlier failed to insert into
   *
   * @return true if OOM handled completely
   *         false if OOM not handled completely
   */
  private boolean handleOutOfMemory(final int failedPartitionIndex) {
    final long allocatedMemoryBeforeSpilling = allocator.getAllocatedMemory();
    VectorizedHashAggPartition victimPartition = partitionSpillHandler.chooseVictimPartition();
    if (victimPartition == null) {
      /* Just spill the current partition that we failed to insert data into.
       * this will not "release" any memory but reduce the partition's
       * memory usage to 0 and we should be able to continue with insertion.
       */
      victimPartition = hashAggPartitions[failedPartitionIndex];
    }
    this.ongoingVictimPartition = victimPartition;
    accumulateBeforeSpill(victimPartition);
    boolean done = spill(victimPartition, true);
    logger.debug("Partition: {} spilled. Memory usage before spilling: {}, memory usage after spilling: {}",
                 victimPartition.getIdentifier(), allocatedMemoryBeforeSpilling, allocator.getAllocatedMemory());

    if (!done) {
      /* micro spilling */
      transitionStateToResumeSpilling();
    }

    return done;
  }

  /**
   * Spill a partition
   *
   * @param victimPartition chosen victim partition to spill
   * @param notify, if true we need to notify other hashagg fragments to spill
   * @return true if we have finished spilling all batches of this partition
   *         false if one or more batches from this partition are yet to be spilled
   */
  private boolean spill(final VectorizedHashAggPartition victimPartition, final boolean notify) {
    boolean done;
    spillPartitionWatch.start();
    try {
      /* send an out of band message to other fragments */
      if (notify) {
        /* if we are about to start spilling this partition, only then we should notify
         * other agg fragments. no need to notify if we are going to resume spilling
         * for this partition
         */
        notifyOthersOfSpill();
      }
      if (enableSmallSpills) {
        /* micro spilling enabled so use stateful API to spill a single batch from this victim partition */
        done = partitionSpillHandler.spillSingleBatchFromPartition(victimPartition);
      } else {
        /* micro spilling disabled so use stateless API to spill all batches from this victim partition */
        partitionSpillHandler.spillPartition(victimPartition);
        done = true;
      }
    } catch (Exception e) {
      throw debug.prepareAndThrowException(e, "Error: Failed to spill partition", HashAggErrorType.SPILL_WRITE);
    } finally {
      spillPartitionWatch.stop();
    }

    return done;
  }

  /**
   * When we have finished spilling all batches from victim partition, we should change
   * both internal and external states for the operator to resume
   * inserting/consuming data from the point we failed at earlier with OOM.
   */
  private void transitionStateToResumeConsuming() {
    internalStateMachine = InternalState.RESUME_CONSUMING;
    state = State.CAN_PRODUCE;
    logger.debug("Transitioned state to resume consuming");
  }

  /**
   * When we hit OOM, handleOutOfMemory() decides if OOM has been handled completely.
   * If OOM wasn't handled completely, we should change both internal and external
   * states for the operator to resume spilling.
   */
  private void transitionStateToResumeSpilling() {
    internalStateMachine = InternalState.SPILL_NEXT_BATCH;
    state = State.CAN_PRODUCE;
    logger.debug("Transitioned state to resume spilling");
  }

  /**
   * After OOB message has been handled to spill a partition, we go back
   * to the state operator was in when fragment executor invoked workOnOOB()
   */
  private void transitionStateToResumeAfterOOB() {
    Preconditions.checkState(operatorStateBeforeOOB != null, "Error: expecting valid cached operator state before OOB");
    state = operatorStateBeforeOOB.getStateBeforeOOB();
    internalStateMachine = operatorStateBeforeOOB.getInternalStateBeforeOOB();
    operatorStateBeforeOOB = null;
    logger.debug("Transitioned state to resume after handling OOB message for spilling a partition");
  }

  /**
   * Once we have finished "force-spilling" a partition, we need to move onto the next
   * partition in the queue.
   */
  private void transitionStateToForceSpillNextPartition() {
    Preconditions.checkState(forceSpillState != null, "Error: expecting valid force spill state");
    /* verify current external and internal states */
    state.is(State.CAN_PRODUCE);
    assertInternalState(InternalState.SPILL_NEXT_BATCH);
    forceSpillState.bumpVictimPartitionIndex();
    /* external state remains same, change internal state */
    internalStateMachine = InternalState.FORCE_SPILL_INMEMORY_DATA;
    logger.debug("Transitioned state to force-spill inmemory portion of next spilled partition");
  }

  /**
   * If micro spilling is enabled, this function manages the "in-progress" spill
   * of a partition. outputData() checks the internal state machine and calls it
   * appropriately.
   */
  private void spillNextBatch() {
    state.is(State.CAN_PRODUCE);
    assertInternalState(InternalState.SPILL_NEXT_BATCH);
    Preconditions.checkState(ongoingVictimPartition != null, "Error: expecting valid victim partition to spill");
    logger.debug("Resume spilling for victim partition:{}", ongoingVictimPartition.getIdentifier());
    final boolean done = spill(ongoingVictimPartition, false);
    if (done) {
      logger.debug("Finished spilling all batches of victim partition:{}", ongoingVictimPartition.getIdentifier());
      ongoingVictimPartition = null;
      if (operatorStateBeforeOOB != null) {
        Preconditions.checkState(forceSpillState == null && resumableInsertState == null,
                                 "Error: not expecting any force spill and resumable insert states");
        transitionStateToResumeAfterOOB();
      } else if (forceSpillState != null) {
        Preconditions.checkState(resumableInsertState == null, "Error: not expecting any resumable insert state");
        transitionStateToForceSpillNextPartition();
      } else {
        transitionStateToResumeConsuming();
      }
    }
  }

  private void assertInternalState(InternalState internalState) {
    Preconditions.checkState(internalStateMachine == internalState,
                             "Error: detected incorrect internal state, expecting: {}", internalState.toString());
  }

  /**
   * If micro spilling is enabled, this function attempts to resume consuming (inserting
   * into hashtable) data from the point we had failed earlier with OOM.
   * outputData() checks the internal state machine and calls it
   * appropriately.
   *
   * If no state is available to resume insertion, then it changes the state to consume
   * the next batch of data (from spilled partition or pipeline)
   */
  private void resumeConsuming() throws Exception {
    state.is(State.CAN_PRODUCE);
    assertInternalState(InternalState.RESUME_CONSUMING);
    Preconditions.checkState(ongoingVictimPartition == null, "Error: expecting null victim partition");
    if (resumableInsertState == null) {
      /* we had already resumed insertion in the past and that succeeded implying
       * we have finished consuming that batch and can ask for next batch
       */
      if (iterations > 1) {
        /* if we have already started recursion, we need to take next batch from spilled partition */
        logger.debug("Resumable insert state no longer available. We have consumed the incoming batch. Now ready to consume next batch from spilled partition");
        internalStateMachine = InternalState.PROCESS_SPILLED_PARTITION;
      } else {
        logger.debug("Resumable insert state no longer available. We have consumed the incoming batch. Now ready to consume next batch from pipeline");
        state = State.CAN_CONSUME;
        internalStateMachine = InternalState.NONE;
      }
    } else {
      /* if we are processing the initial incoming, then next batch will come from pipeline */
      if (resumableInsertState.processingSpilledData) {
        logger.debug("Resume consuming previous incoming data from spilled partitions");
        consumeSpilledDataHelper(resumableInsertState.records, partitionToLoadSpilledData.getFixedKeyColPivotedData(),
                                 partitionToLoadSpilledData.getVariableKeyColPivotedData());
      } else {
        logger.debug("Resume consuming previous incoming data from pipeline");
        consumeDataHelper(resumableInsertState.records);
      }
    }
  }

  /**
   * Encapsulates all the information we need to resume
   * consuming data. This is used for micro spilling.
   */
  private static class ResumableInsertState {
    private final int records;
    private final int recordsPivoted;
    private final int recordsConsumed;
    private final int resumeFromIndex;
    private final long partitionUsedMask;
    private final boolean processingSpilledData;

    ResumableInsertState(final int records, final int recordsPivoted, final int recordsConsumed,
                         final int resumeFromIndex, final long partitionUsedMask, final boolean processingSpilledData) {
      this.records = records;
      this.recordsPivoted = recordsPivoted;
      this.recordsConsumed = recordsConsumed;
      this.resumeFromIndex = resumeFromIndex;
      this.partitionUsedMask = partitionUsedMask;
      this.processingSpilledData = processingSpilledData;
    }
  }

  /**
   * Build the state to resume consuming data at a later point
   *
   * @param records total number of records to be consumed
   * @param recordsPivoted records pivoted -- a contiguous subset of the total records we have to consume
   * @param recordsConsumed number of records already consumed at the time of OOM
   * @param resumeFromRecordIndex resume insertion from this record index
   * @param partitionsUsedMask bitmask indicating the partitions holding data at the time of OOM
   * @param processingSpilledData true if incoming is from spilled data, false if incoming is from upstream
   */
  private void buildResumableInsertState(final int records, final int recordsPivoted, final int recordsConsumed,
                                         final int resumeFromRecordIndex, final long partitionsUsedMask,
                                         final boolean processingSpilledData) {
    Preconditions.checkState(resumableInsertState == null, "Error: expecting null resumable insert state");
    assertInternalState(InternalState.SPILL_NEXT_BATCH);
    this.resumableInsertState = new ResumableInsertState(records, recordsPivoted, recordsConsumed, resumeFromRecordIndex, partitionsUsedMask, processingSpilledData);
    logger.debug("Built resumable insert state: records:{}, recordsPivoted:{}, recordsConsumed:{}, resumeFromIndex:{}, partitionsUsedMask:{}, processingSpilledData:{}",
                 records, recordsPivoted, recordsConsumed, resumeFromRecordIndex, partitionsUsedMask, processingSpilledData);
  }

  /**
   * When this operator starts spilling, notify others if the triggering is enabled.
   */
  private void notifyOthersOfSpill() {
    if(!context.getOptions().getOption(OOB_SPILL_TRIGGER_ENABLED)) {
      return;
    }

    try {
      Payload payload = new Payload(HashAggSpill.newBuilder().setMemoryUse(allocator.getAllocatedMemory()).build());
      for(FragmentAssignment a : context.getAssignments()) {
        OutOfBandMessage message = new OutOfBandMessage(
            context.getFragmentHandle().getQueryId(),
            context.getFragmentHandle().getMajorFragmentId(),
            a.getMinorFragmentIdList(),
            popConfig.getProps().getOperatorId(),
            context.getFragmentHandle().getMinorFragmentId(),
            payload, true);

        NodeEndpoint endpoint = context.getEndpointsIndex().getNodeEndpoint(a.getAssignmentIndex());
        context.getTunnelProvider().getExecTunnel(endpoint).sendOOBMessage(message);
      }
      oobSends++;
    } catch(Exception ex) {
      logger.warn("Failure while attempting to notify others of spilling.", ex);
    }
  }

  /**
   * When a out of band message arrives, spill if we're within a factor of the other operator that is spilling.
   */
  @Override
  public void workOnOOB(OutOfBandMessage message) {

    oobReceives++;

    // don't pay attention to self notification.
    if(message.getSendingMinorFragmentId() == context.getFragmentHandle().getMinorFragmentId()) {
      oobDropLocal++;
      logger.debug("Ignoring the OOB spill trigger self notification");
      return;
    }

    if (internalStateMachine == InternalState.SPILL_NEXT_BATCH) {
      oobDropSpill++;
      logger.debug("Ignoring OOB spill trigger as fragment is already spilling");
      return;
    }

    if(internalStateMachine != InternalState.NONE && internalStateMachine != InternalState.PROCESS_SPILLED_PARTITION) {
      oobDropWrongState++;
      logger.debug("Ignoring OOB spill trigger as fragment is either outputting data or transitioning state to process spilled partitions");
      return;
    }

    // check to see if we're at the point where we want to spill.
    final HashAggSpill spill = message.getPayload(HashAggSpill.PARSER);
    final long allocatedMemoryBeforeSpilling = allocator.getAllocatedMemory();
    final double triggerFactor = context.getOptions().getOption(OOB_SPILL_TRIGGER_FACTOR);
    final double headroomRemaining = allocator.getHeadroom() * 1.0d / (allocator.getHeadroom() + allocator.getAllocatedMemory());
    if(allocatedMemoryBeforeSpilling < (spill.getMemoryUse() * triggerFactor) && headroomRemaining > context.getOptions().getOption(OOB_SPILL_TRIGGER_HEADROOM_FACTOR)) {
      logger.debug("Skipping OOB spill trigger, current allocation is {}, which is not within the current factor of the spilling operator ({}) which has memory use of {}. Headroom is at {} which is greater than trigger headroom of {}",
          allocatedMemoryBeforeSpilling, triggerFactor, spill.getMemoryUse(), headroomRemaining, context.getOptions().getOption(OOB_SPILL_TRIGGER_HEADROOM_FACTOR));
      oobDropUnderThreshold++;
      return;
    }

    VectorizedHashAggPartition victimPartition = partitionSpillHandler.chooseVictimPartition();
    if(victimPartition == null) {
      ++oobDropNoVictim;
      logger.debug("Ignoring OOB spill trigger as no victim partitions found.");
      return;
    }

    /* remember the victim */
    this.ongoingVictimPartition = victimPartition;
    /* spill the victim */
    boolean done = spill(victimPartition, false);
    ++oobSpills;
    if (!done) {
      /*
       * the victim partition has more than 1 batch, so the above call
       * would have spilled only a single batch. we now cache operator state
       * (as of now), transition external, internal states and yield
       */
      cacheOperatorStateBeforeOOB();
      transitionStateToResumeSpilling();
    }
  }

  /**
   * Holder to cache operator state (external and internal)
   * to handle OOB message (by spilling a partition) and
   * when we finish spilling the partition, we can restore
   * both internal and external states of the operator
   * as they were before FragmentExecutor asked the
   * operator to work on OOB message.
   */
  private static class OperatorStateBeforeOOB {
    private final State stateBeforeOOB;
    private final InternalState internalStateBeforeOOB;
    OperatorStateBeforeOOB(final State state, final InternalState internalState) {
      stateBeforeOOB = state;
      internalStateBeforeOOB = internalState;
    }

    State getStateBeforeOOB() {
      return stateBeforeOOB;
    }

    InternalState getInternalStateBeforeOOB() {
      return internalStateBeforeOOB;
    }
  }

  private void cacheOperatorStateBeforeOOB() {
    this.operatorStateBeforeOOB = new OperatorStateBeforeOOB(state, internalStateMachine);
  }

  /**
   * We can run out of memory while consuming data (as we are inserting records into hashtable).
   * If we had planned to insert N records and hit OOM after inserting M records successfully
   * (M < N) then before we spill a partition, we need to complete accumulation of K records
   * (K < M) inserted so far and that belong to the partition we have decided to spill. This
   * is why we call accumulateBeforeSpill() from handleOutOfMemory().
   *
   * @param partitionToSpill partition chosen for spilling, accumulate records for this partition
   */
  private void accumulateBeforeSpill(final VectorizedHashAggPartition partitionToSpill) {
    long offsetAddr = partitionToSpill.buffer.memoryAddress();
    final int partitionRecords = partitionToSpill.getRecords();
    final AccumulatorSet accumulator = partitionToSpill.accumulator;
    accumulateWatch.start();
    accumulator.accumulate(offsetAddr, partitionRecords, bitsInChunk, chunkOffsetMask);
    accumulateWatch.stop();
    partitionToSpill.resetRecords();
  }

  /**
   * Aggregate data for each partition.
   *
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
  private void accumulateForAllPartitions(long partitionsUsed) {
    /* accumulate -- if we used count sort above then it would have rearranged HT ordinals
     * and other info into another buffer sorted by partition indices. we use the latter
     * buffer to do accumulation of all records one partition at a time
     */
    accumulateWatch.start();
    while (partitionsUsed > 0) {
      final byte hashPartitionIndex = (byte)Long.numberOfTrailingZeros(partitionsUsed);
      final VectorizedHashAggPartition partition = hashAggPartitions[hashPartitionIndex];
      long offsetAddr = partition.buffer.memoryAddress();
      final int partitionRecords = partition.getRecords();
      if (partitionRecords > 0) {
        final AccumulatorSet accumulator = partition.accumulator;
        accumulator.accumulate(offsetAddr, partitionRecords, bitsInChunk, chunkOffsetMask);
      }
      partitionsUsed = partitionsUsed & (partitionsUsed - 1);
      partition.resetRecords();
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
    assertInternalState(InternalState.PROCESS_SPILLED_PARTITION);
    final PartitionToLoadSpilledData partitionToLoadSpilledData = this.partitionToLoadSpilledData;
    /* Step 1: get disk iterator for the spilled partition to process */
    final SpilledPartitionIterator spilledPartitionIterator = partitionSpillHandler.getNextSpilledPartitionToProcess();
    final String partitionIdentifier = spilledPartitionIterator.getIdentifier();
    logger.debug("Processing disk partition:{}", partitionIdentifier);
    /* Step 2: prepare the loading partition to read next batch */
    partitionToLoadSpilledData.reset();
    readSpilledBatchWatch.start();
    int numRecordsInBatch;
    try {
      /* Step 3: read a single batch from the spill file */
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
    } else {
      logger.debug("pname: {} Read {} records from spilled batch", partitionIdentifier, numRecordsInBatch);
      final ArrowBuf fixedWidthPivotedData = partitionToLoadSpilledData.getFixedKeyColPivotedData();
      final ArrowBuf variableWidthPivotedData = partitionToLoadSpilledData.getVariableKeyColPivotedData();
      final byte[] accumulatorTypes = partitionToLoadSpilledData.getAccumulatorTypes();
      final FieldVector[] accumulatorVectors = partitionToLoadSpilledData.getPostSpillAccumulatorVectors();
      if (spilledPartitionIterator.getCurrentBatchIndex() == 1) {
        /* we just read the first batch */
        initStateBeforeProcessingSpilledPartition(accumulatorTypes, accumulatorVectors, partitionIdentifier);
      }
      /* Step 4: re-partition, insert into hash tables and aggregate */
      consumeSpilledDataHelper(numRecordsInBatch, fixedWidthPivotedData, variableWidthPivotedData);
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
    final long seed = getSeedForRepartitioning();

    if (resumableInsertState != null) {
      final int recordsPivoted = resumableInsertState.recordsPivoted;
      final int recordsConsumed = resumableInsertState.recordsConsumed;
      final int resumeFromIndex = resumableInsertState.resumeFromIndex;
      final long partitionsUsedMask = resumableInsertState.partitionUsedMask;

      logger.debug("Resume consuming spilled data with state: records:{}, recordsPivoted:{}, recordsConsumed:{}, resumeFromIndex:{}, partitionsUsedMask:{}",
                   records, recordsPivoted, recordsConsumed, resumeFromIndex, partitionsUsedMask);

      resumableInsertState = null;

      long partitionsUsed = insertIntoPartitions(records, recordsPivoted, keyFixedVectorAddr, keyVarVectorAddr,
        recordsConsumed, resumeFromIndex, partitionsUsedMask, true, seed);

      if (internalStateMachine == InternalState.SPILL_NEXT_BATCH) {
        state.is(State.CAN_PRODUCE);
        Preconditions.checkState(resumableInsertState != null, "Should have a valid state to resume insertion");
        updateStats();
        return;
      }

      accumulateForAllPartitions(partitionsUsed);
      resetPivotStructures();
      updateStats();
    } else {
      /* STEP 1: then we hash partition the dataset and add pivoted data to multiple hash tables */
      long partitionsUsed = insertIntoPartitions(records, records, keyFixedVectorAddr, keyVarVectorAddr,
        0, 0, 0, true, seed);

      if (internalStateMachine == InternalState.SPILL_NEXT_BATCH) {
        state.is(State.CAN_PRODUCE);
        Preconditions.checkState(resumableInsertState != null, "Should have a valid state to resume insertion");
        updateStats();
        return;
      }

      /* STEP 2: then we do accumulators for all partitions in a single pass */
      accumulateForAllPartitions(partitionsUsed);

      /* STEP 3: prepare for next iteration */
      resetPivotStructures();

      /* STEP 4: update hashtable stats */
      updateStats();
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

  @VisibleForTesting
  public VectorizedHashAggPartition getPartition(final int index) {
    return this.hashAggPartitions[index];
  }


  private void computeAllStats() {
    final VectorizedHashAggPartition[] hashAggPartitions = this.hashAggPartitions;
    final int numPartitions = this.numPartitions;
    int tableSize = 0;
    int tableCapacity = 0;
    int tableRehashCount = 0;
    int tableSpliceCount = 0;
    int tableForceAccumCount = 0;
    int tableAccumCompactionCount = 0;
    long allocatedForFixedBlocks = 0;
    long unusedForFixedBlocks = 0;
    long allocatedForVarBlocks = 0;
    long unusedForVarBlocks = 0;
    long rehashTime = 0;
    long spliceTimeNs = 0;
    long forceAccumTimeNs = 0;
    long accumCompactionTimeNs = 0;
    int maxVarLenKeySize = 0;
    int minTableSize = Integer.MAX_VALUE;
    int maxTableSize = Integer.MIN_VALUE;
    int minRehashCount = Integer.MAX_VALUE;
    int maxRehashCount = Integer.MIN_VALUE;

    for (int i = 0; i < numPartitions; i++) {
      final LBlockHashTable hashTable = hashAggPartitions[i].hashTable;
      final int size = hashAggPartitions[i].hashTable.size();
      final int rehashCount = hashAggPartitions[i].hashTable.getRehashCount();
      final int spliceCount = hashAggPartitions[i].hashTable.getSpliceCount();
      final int forceAccumCount = hashAggPartitions[i].getForceAccumCount();
      final int accumCompactionCount = hashAggPartitions[i].hashTable.getAccumCompactionCount();
      tableCapacity += hashAggPartitions[i].hashTable.capacity();
      rehashTime += hashAggPartitions[i].hashTable.getRehashTime(TimeUnit.NANOSECONDS);
      spliceTimeNs += hashAggPartitions[i].hashTable.getSpliceTime(TimeUnit.NANOSECONDS);
      forceAccumTimeNs += hashAggPartitions[i].getForceAccumTime(TimeUnit.NANOSECONDS);
      accumCompactionTimeNs += hashAggPartitions[i].hashTable.getAccumCompactionTime(TimeUnit.NANOSECONDS);
      final int varLenKeySize = hashAggPartitions[i].hashTable.getMaxVarLenKeySize();
      if (maxVarLenKeySize < varLenKeySize) {
        maxVarLenKeySize = varLenKeySize;
      }
      tableSize += size;
      tableRehashCount += rehashCount;
      tableSpliceCount += spliceCount;
      tableForceAccumCount += forceAccumCount;
      tableAccumCompactionCount += accumCompactionCount;
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
    statsHolder.hashTableSpliceCount = tableSpliceCount;
    statsHolder.hashTableSpliceTimeNs = spliceTimeNs;
    statsHolder.hashTableForceAccumCount = tableForceAccumCount;
    statsHolder.hashTableForceAccumTimeNs = forceAccumTimeNs;
    statsHolder.hashTableAccumCompactCount = tableAccumCompactionCount;
    statsHolder.hashTableAccumCompactTimeNs = accumCompactionTimeNs;
    statsHolder.hashTableMaxVarLenKeySize = maxVarLenKeySize;
    statsHolder.minHashTableSize = minTableSize;
    statsHolder.maxHashTableSize = maxTableSize;
    statsHolder.minHashTableRehashCount = minRehashCount;
    statsHolder.maxHashTableRehashCount = maxRehashCount;

    if (iterations == 1) {
      statsHolder.maxHashTableBatchSize = maxHashTableBatchSize;
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
    stats.setLongStat(Metric.NUM_SPLICE, statsHolder.hashTableSpliceCount);
    stats.setLongStat(Metric.SPLICE_TIME_NS, statsHolder.hashTableSpliceTimeNs);
    stats.setLongStat(Metric.NUM_FORCE_ACCUM, statsHolder.hashTableForceAccumCount);
    stats.setLongStat(Metric.FORCE_ACCUM_TIME_NS, statsHolder.hashTableForceAccumTimeNs);
    stats.setLongStat(Metric.NUM_ACCUM_COMPACTS, statsHolder.hashTableAccumCompactCount);
    stats.setLongStat(Metric.ACCUM_COMPACTS_TIME_NS, statsHolder.hashTableAccumCompactTimeNs);
    stats.setLongStat(Metric.MAX_VARLEN_KEY_SIZE, statsHolder.hashTableMaxVarLenKeySize);
    stats.setLongStat(Metric.MAX_HASHTABLE_BATCH_SIZE, statsHolder.maxHashTableBatchSize);
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
    stats.setLongStat(Metric.TOTAL_SPILLED_DATA_SIZE, partitionSpillHandler.getTotalSpilledDataSize());
    stats.setLongStat(Metric.MAX_SPILLED_DATA_SIZE, partitionSpillHandler.getMaxSpilledDataSize());

    stats.setLongStat(Metric.OOB_SENDS, oobSends);
    stats.setLongStat(Metric.OOB_RECEIVES, oobReceives);
    stats.setLongStat(Metric.OOB_DROP_LOCAL, oobDropLocal);
    stats.setLongStat(Metric.OOB_DROP_WRONG_STATE, oobDropWrongState);
    stats.setLongStat(Metric.OOB_DROP_UNDER_THRESHOLD, oobDropUnderThreshold);
    stats.setLongStat(Metric.OOB_DROP_NO_VICTIM, oobDropNoVictim);
    stats.setLongStat(Metric.OOB_SPILL, oobSpills);
    stats.setLongStat(Metric.OOB_DROP_ALREADY_SPILLING, oobDropSpill);

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
    private int hashTableSpliceCount;
    private long hashTableSpliceTimeNs;
    private int hashTableForceAccumCount;
    private long hashTableForceAccumTimeNs;
    private int hashTableAccumCompactCount;
    private long hashTableAccumCompactTimeNs;
    private int hashTableMaxVarLenKeySize;
    private int minHashTableSize;
    private int maxHashTableSize;
    private int minHashTableRehashCount;
    private int maxHashTableRehashCount;
    private int maxHashTableBatchSize;
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
    Preconditions.checkState(internalStateMachine != InternalState.DONE &&
      internalStateMachine != InternalState.NONE, "Error: detected invalid internal operator state");

    int records = 0;

    switch (internalStateMachine) {
      case SPILL_NEXT_BATCH:
        spillNextBatch();
        break;
      case RESUME_CONSUMING:
        resumeConsuming();
        break;
      case FORCE_SPILL_INMEMORY_DATA:
        forceSpillInmemoryData();
        break;
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
    assertInternalState(InternalState.TRANSITION_PARTITION_SPILL_STATE);
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

    if (outputPartitionIndex == numPartitions) {
      postOutputProcessing();
      outputPartitionIndex = 0;
      outputBatchIndex = 0;
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

    int numBatchesToOutput = popConfig.getHashTableBatchSize() / maxHashTableBatchSize;
    numBatchesToOutput = Math.min(numBatchesToOutput, hashTableBlocks - outputBatchIndex);

    int[] recordsInBatches = new int[numBatchesToOutput];
    int totalRecords = 0;
    for (int i = 0; i < numBatchesToOutput; i++) {
      recordsInBatches[i] = hashTable.getRecordsInBatch(outputBatchIndex + i);
      totalRecords += recordsInBatches[i];
    }
    logger.debug("output partition pname: {} batches: {}:{}  records: {}",
      partitionToOutput.getIdentifier(), outputBatchIndex, outputBatchIndex + numBatchesToOutput - 1, totalRecords);

    /* unpivot GROUP BY key columns for one or more batches into corresponding vectors in outgoing container */
    unpivotWatch.start();
    partitionToOutput.hashTable.unpivot(outputBatchIndex, recordsInBatches);
    unpivotWatch.stop();

    /* transfer accumulation vectors to the target vector in transferPair -- output vector in outgoing container */
    partitionToOutput.accumulator.output(outputBatchIndex, recordsInBatches);

    outputBatchIndex += numBatchesToOutput;
    if (outputBatchIndex == hashTableBlocks) {
      /* this partition outputted, move onto next partition */
      outputPartitionIndex++;
      /* start from first batch of next partition */
      outputBatchIndex = 0;
      /* downsize the partition */
      partitionToOutput.resetToMinimumSize();
    }

    updateStats();
    return outgoing.setAllCount(totalRecords);
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
      moveToFinalState();

      /* propagate some metric back for verification in tests.
       * Note: this is already part of metrics collected here that are
       * later on visible in query profile. We just use a subset of
       * such metrics for verification in oom unit tests.
       */
      final VectorizedHashAggSpillStats spillStats = new VectorizedHashAggSpillStats();
      spillStats.setSpills((int)partitionSpillHandler.getNumberOfSpills());
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
    assertInternalState(InternalState.PROCESS_SPILLED_PARTITION);
    checkIfForceSpillIsNeeded();
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);
    assertInternalState(InternalState.NONE);
    if (getHashTableSize() == 0) {
      /* nothing to output */
      moveToFinalState();
    } else {
      checkIfForceSpillIsNeeded();
    }
  }

  /**
   * Check if force spill is needed. During an iteration,
   * if some partitions were spilled, then at the end of iteration
   * we need to spill any inmemory-data belonging to those spilled
   * partitions to ensure that before we start processing spilled
   * partitions, all of them are entirely on disk
   */
  private void checkIfForceSpillIsNeeded() throws Exception {
    if (partitionSpillHandler.getActiveSpilledPartitionCount() > 0) {
      buildForceSpillState();
      /* spill some while we are already on CPU */
      forceSpillInmemoryData();
    } else {
      /* nothing to flush, so output the inmemory partitions before moving
       * on to next iteration
       */
      moveToOutputState();
    }
  }

  /**
   * Operator has finished processing. Both external and internal
   * states should be set to DONE
   */
  private void moveToFinalState() {
    state = State.DONE;
    internalStateMachine = InternalState.DONE;
  }

  /**
   * Transition both external and internal states for the operator
   * to start outputting data
   */
  private void moveToOutputState() {
    state = State.CAN_PRODUCE;
    internalStateMachine = InternalState.OUTPUT_INMEMORY_PARTITIONS;
  }

  /**
   * Build force spill state.
   */
  private void buildForceSpillState() throws Exception {
    Preconditions.checkState(forceSpillState == null, "Error: expecting null flush state");
    forceSpillState = new ForceSpillState();
    state = State.CAN_PRODUCE;
    internalStateMachine = InternalState.FORCE_SPILL_INMEMORY_DATA;
  }

  /**
   * Holder for force-spill state. Allows us to track which
   * partition we are currently spilling.
   */
  private static class ForceSpillState {
    private int victimPartitionIndex;
    ForceSpillState() {
      this.victimPartitionIndex = 0;
    }

    int getVictimPartitionIndex() {
      return victimPartitionIndex;
    }

    void bumpVictimPartitionIndex() {
      ++victimPartitionIndex;
    }
  }

  /**
   * Spill the memory portion of each spilled partition. This is done once
   * a single iteration of hash aggregation algorithm finishes consuming data.
   * Before we begin the next iteration (needed only if some data was spilled),
   * we ensure that all spilled partitions are entirely on disk. Therefore, once
   * the iteration gets over, we spill left-over inmemory data belonging to each
   * spilled partition.
   *
   * The in-memory portion of partition could be empty if after the partition
   * was spilled, no incoming data ever mapped to that particular partition. We
   * ignore such spilled partitions.
   */
  private void forceSpillInmemoryData() throws Exception {
    state.is(State.CAN_PRODUCE);
    assertInternalState(InternalState.FORCE_SPILL_INMEMORY_DATA);
    Preconditions.checkState(forceSpillState != null, "Error: expecting valid flush state");
    final int victimPartitionIndex = forceSpillState.getVictimPartitionIndex();
    if (victimPartitionIndex >= partitionSpillHandler.getActiveSpilledPartitionCount()) {
      /* we have finished "force-spilling" remaining inmemory data for all spilled partitions.
       * we can now start outputting data for inmemory partitions
       */
      forceSpillState = null;
      partitionSpillHandler.closeSpillStreams();
      moveToOutputState();
      return;
    }
    final VectorizedHashAggDiskPartition activeSpilledPartition = partitionSpillHandler.getActiveSpilledPartition(victimPartitionIndex);
    final VectorizedHashAggPartition inmemoryPartition = activeSpilledPartition.getInmemoryPartitionBackPointer();
    if (inmemoryPartition.hashTable.size() == 0) {
      /* inmemory portion of spilled partition is empty, ignore it */
      forceSpillState.bumpVictimPartitionIndex();
      return;
    }
    /* remember the partition and spill */
    this.ongoingVictimPartition = inmemoryPartition;
    final boolean done = spill(inmemoryPartition, false);
    if (!done) {
      /* if partition had more than one batch in-memory then we need to continue spilling */
      transitionStateToResumeSpilling();
    } else {
      /* the inmemory portion for this spilled partition fit in one batch
       * so we are done with this partition. take the next partition in next cycle.
       */
      forceSpillState.bumpVictimPartitionIndex();
    }
  }

  private enum InternalState {
    NONE,
    OUTPUT_INMEMORY_PARTITIONS,
    PROCESS_SPILLED_PARTITION,
    TRANSITION_PARTITION_SPILL_STATE,
    SPILL_NEXT_BATCH,
    RESUME_CONSUMING,
    FORCE_SPILL_INMEMORY_DATA,
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
          tempAccumulatorHolder != null ? Arrays.asList(tempAccumulatorHolder) : new ArrayList<>(0),
          partitionToLoadSpilledData != null ? Collections.singletonList(partitionToLoadSpilledData) : new ArrayList<>(0),
          partitionSpillHandler != null ? Collections.singletonList(partitionSpillHandler) : new ArrayList<>(0),
          fixedBlockVector != null ? Collections.singletonList(fixedBlockVector) : new ArrayList<>(0),
          variableBlockVector != null ? Collections.singletonList(variableBlockVector) : new ArrayList<>(0),
          hashAggPartitions != null ? Arrays.asList(hashAggPartitions) : new ArrayList<>(0),
          outgoing));
      } finally {
        partitionToLoadSpilledData = null;
        partitionSpillHandler = null;
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
