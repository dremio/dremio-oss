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
package com.dremio.sabot.op.join.vhash.spill;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;

/**
 * Join Setup Params: Common parameters used in several stages of hash-join.
 */
public final class JoinSetupParams {
  public static final int TABLE_HASH_SIZE = 4;

  // common
  private final OptionManager options;
  // Allocator used for temporary allocations (no limit)
  private final BufferAllocator opAllocator;
  // Allocator used for long-lived data structures (i.e may stay around till the end-of-life of partition/operator)
  private final BufferAllocator buildAllocator;
  // pivoted keys (fixed block) for incoming build or probe batch
  private final FixedBlockVector pivotedFixedBlock;
  // pivoted keys (variable block) for incoming build or probe batch
  private final VariableBlockVector pivotedVariableBlock;
  // type of join : inner/left/right/full
  private final JoinRelType joinType;
  // incoming build batch
  private final VectorAccessible right;
  // incoming probe batch
  private final VectorAccessible left;

  // Build side
  // Used to pivot the keys in hash table for build batch
  private final PivotDef buildKeyPivot;
  /* Used to unpivot the keys during outputting the non matched records in build side
   * Note that buildUnpivot is for output, but buildPivot is for incoming build batch
   */
  private final PivotDef buildKeyUnpivot;
  // List of build side columns that are part of the join key
  private final List<FieldVector> buildOutputKeys;
  // List of build side columns that are not part of the join key
  private final List<FieldVector> buildOutputCarryOvers;
  // schema of carry-over columns
  private final BatchSchema carryAlongSchema;
  private final ImmutableBitSet carryAlongFieldsBitset;
  private final NullComparator comparator;

  // Probe side
  // Used to pivot the keys in probe batch
  private final PivotDef probeKeyPivot;
  /* The keys of build batch will not be added to hyper container.
   * probeIncomingKeys and buildOutputKeys are used to maintain all the keys in probe side and build side,
   * And they will be used to build copier in VectorizedProbe, which will be used to copy the key vectors
   * from probe side to build side in output for matched records.
   */
  private final List<FieldVector> probeIncomingKeys;
  private final List<FieldVector> probeOutputs;

  JoinSetupParams(OptionManager options,
                  BufferAllocator opAllocator,
                  BufferAllocator buildAllocator,
                  FixedBlockVector pivotedFixedBlock,
                  VariableBlockVector pivotedVariableBlock,
                  JoinRelType joinType,
                  VectorAccessible right,
                  VectorAccessible left,
                  PivotDef buildKeyPivot,
                  PivotDef buildKeyUnpivot,
                  List<FieldVector> buildOutputKeys,
                  List<FieldVector> buildOutputCarryOvers,
                  BatchSchema carryAlongSchema,
                  ImmutableBitSet carryAlongFieldsBitset,
                  NullComparator comparator,
                  PivotDef probeKeyPivot,
                  List<FieldVector> probeIncomingKeys,
                  List<FieldVector> probeOutputs) {

    this.options = options;
    this.opAllocator = opAllocator;
    this.buildAllocator = buildAllocator;
    this.pivotedFixedBlock = pivotedFixedBlock;
    this.pivotedVariableBlock = pivotedVariableBlock;
    this.joinType = joinType;
    this.right = right;
    this.left = left;
    this.buildKeyPivot = buildKeyPivot;
    this.buildKeyUnpivot = buildKeyUnpivot;
    this.buildOutputKeys = buildOutputKeys;
    this.buildOutputCarryOvers = buildOutputCarryOvers;
    this.carryAlongSchema = carryAlongSchema;
    this.carryAlongFieldsBitset = carryAlongFieldsBitset;
    this.comparator = comparator;
    this.probeKeyPivot = probeKeyPivot;
    this.probeIncomingKeys = probeIncomingKeys;
    this.probeOutputs = probeOutputs;
  }

  public OptionManager getOptions() {
    return options;
  }

  public BufferAllocator getOpAllocator() {
    return opAllocator;
  }

  public BufferAllocator getBuildAllocator() {
    return buildAllocator;
  }

  public FixedBlockVector getPivotedFixedBlock() {
    return pivotedFixedBlock;
  }

  public VariableBlockVector getPivotedVariableBlock() {
    return pivotedVariableBlock;
  }

  public JoinRelType getJoinType() {
    return joinType;
  }

  public VectorAccessible getRight() {
    return right;
  }

  public VectorAccessible getLeft() {
    return left;
  }

  public PivotDef getBuildKeyPivot() {
    return buildKeyPivot;
  }

  public PivotDef getBuildKeyUnpivot() {
    return buildKeyUnpivot;
  }

  public List<FieldVector> getBuildOutputKeys() {
    return buildOutputKeys;
  }

  public List<FieldVector> getBuildOutputCarryOvers() {
    return buildOutputCarryOvers;
  }

  public BatchSchema getCarryAlongSchema() {
    return carryAlongSchema;
  }

  public ImmutableBitSet getCarryAlongFieldsBitset() {
    return carryAlongFieldsBitset;
  }

  public NullComparator getComparator() {
    return comparator;
  }

  public PivotDef getProbeKeyPivot() {
    return probeKeyPivot;
  }

  public List<FieldVector> getProbeIncomingKeys() {
    return probeIncomingKeys;
  }

  public List<FieldVector> getProbeOutputs() {
    return probeOutputs;
  }

  public int getMaxInputBatchSize() {
    return (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
  }
}
