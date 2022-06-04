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
package com.dremio.exec.planner.physical;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.dremio.common.logical.PlanProperties;
import com.dremio.common.logical.PlanProperties.Generator.ResultMode;
import com.dremio.common.logical.PlanProperties.PlanPropertiesBuilder;
import com.dremio.common.logical.PlanProperties.PlanType;
import com.dremio.common.util.Numbers;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.explain.PrelSequencer.OpId;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.LongValidator;
import com.google.common.collect.Lists;


/**
 * Carries common state and utility methods for generating a physical plan.
 */
public class PhysicalPlanCreator {

  private final Map<Prel, OpId> opIdMap;

  private List<PhysicalOperator> popList;
  private final QueryContext context;
  PhysicalPlan plan = null;

  public PhysicalPlanCreator(QueryContext context, Map<Prel, OpId> opIdMap) {
    this.context = context;
    this.opIdMap = opIdMap;
    popList = Lists.newArrayList();
  }

  public QueryContext getContext() {
    return context;
  }

  public FunctionLookupContext getFunctionLookupContext() {
    return context.getFunctionRegistry();
  }

  public OptionManager getOptionManager() {
    return context.getOptions();
  }

  private int getId(Prel originalPrel){
    return opIdMap.get(originalPrel).getAsSingleInt();
  }

  public OpId getOpId(Prel prel) {
    return opIdMap.get(prel);
  }

  public OpProps props(int operatorId, Prel prel, String username, BatchSchema schema, LongValidator reserveOption, LongValidator limitOption, double cost) {
    return props(operatorId, username, schema,
      reserveOption == null ? Prel.DEFAULT_RESERVE : context.getOptions().getOption(reserveOption),
      limitOption, Prel.DEFAULT_LOW_LIMIT, cost,
      prel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE) == DistributionTrait.SINGLETON);
  }

  public OpProps props(Prel prel, String username, BatchSchema schema, LongValidator reserveOption, LongValidator limitOption, double cost) {
    return props(prel, username, schema,
      reserveOption == null ? Prel.DEFAULT_RESERVE : context.getOptions().getOption(reserveOption),
      limitOption, Prel.DEFAULT_LOW_LIMIT, cost);
  }

  public OpProps props(Prel prel, String username, BatchSchema schema, LongValidator reserveOption, LongValidator limitOption) {
    return props(prel, username, schema,
      reserveOption == null ? Prel.DEFAULT_RESERVE : context.getOptions().getOption(reserveOption),
      limitOption, Prel.DEFAULT_LOW_LIMIT, prel.getCostForParallelization());
  }

  public OpProps props(Prel prel, String username, BatchSchema schema, long reservation, LongValidator limitOption, long lowLimit) {
    return props(prel, username, schema, reservation, limitOption, lowLimit, prel.getCostForParallelization());
  }

  private OpProps props(Prel prel, String username, BatchSchema schema, long reservation, LongValidator limitOption, long lowLimit, double cost) {
    return props(getId(prel), username, schema, reservation, limitOption, lowLimit, cost,
      prel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE) == DistributionTrait.SINGLETON);
  }

  private OpProps props(int operatorId,
                        String username,
                        BatchSchema schema,
                        long reservation,
                        LongValidator limitOption,
                        long lowLimit,
                        double cost,
                        boolean singleStream) {
    return new OpProps(
      operatorId,
      username,
      reservation,
      limitOption == null ? Prel.DEFAULT_LIMIT : context.getOptions().getOption(limitOption),
      lowLimit,
      cost,
      singleStream,
      calculateTargetRecordSize(schema),
      schema,
      false,
      0.0d,
      false);
  }

  public OpProps props(Prel prel, String username, BatchSchema schema) {
    return props(prel, username, schema, null, null);
  }

  public PhysicalPlan build(Prel rootPrel, boolean forceRebuild) {

    if (plan != null && !forceRebuild) {
      return plan;
    }

    PlanPropertiesBuilder propsBuilder = PlanProperties.builder();
    propsBuilder.type(PlanType.PHYSICAL);
    propsBuilder.version(1);
    propsBuilder.resultMode(ResultMode.EXEC);
    propsBuilder.generator(PhysicalPlanCreator.class.getName(), "");


    try {
      // invoke getPhysicalOperator on the root Prel which will recursively invoke it
      // on the descendants and we should have a well-formed physical operator tree
      PhysicalOperator rootPOP = rootPrel.getPhysicalOperator(this);
      if (rootPOP != null) {
        assert (popList.size() > 0); //getPhysicalOperator() is supposed to populate this list
        plan = new PhysicalPlan(propsBuilder.build(), popList);
      }

    } catch (IOException e) {
      plan = null;
      throw new UnsupportedOperationException("Physical plan created failed with error : " + e.toString());
    }

    return plan;
  }

  private int estimateRecordSize(BatchSchema schema, OptionManager options) {
    final int listSizeEstimate = (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
    final int varFieldSizeEstimate = (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
    final int estimatedRecordSize = schema.estimateRecordSize(listSizeEstimate, varFieldSizeEstimate);
    return estimatedRecordSize;
  }

  private final int calculateTargetRecordSize(BatchSchema schema) {
    final int estimatedRecordSize = estimateRecordSize(schema, context.getOptions());
    OptionManager options = context.getOptions();
    return calculateBatchCountFromRecordSize(options, estimatedRecordSize);
  }

  public static int calculateBatchCountFromRecordSize(OptionManager options, int estimatedRecordSize) {

    if (estimatedRecordSize == 0) {
      // If the estimated row size is zero (possible when schema is not known initially for queries containing
      // convert_from), fall back to max size
      return optimizeBatchSizeForAllocs((int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX));
    }

    final int minTargetBatchCount = (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MIN);
    final int maxTargetBatchCount = (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);

    final int maxBatchSizeBytes = (int) options.getOption(ExecConstants.TARGET_BATCH_SIZE_BYTES);

    final int targetBatchSize = max(minTargetBatchCount,
      min(maxTargetBatchCount, maxBatchSizeBytes / estimatedRecordSize));

    return optimizeBatchSizeForAllocs(targetBatchSize);
  }

  public static int optimizeBatchSizeForAllocs(final int batchSize) {
    /*
     * The allocators anyway round-up to a power of 2. So, no point in allocating less.
     */
    final int targetCount = Numbers.nextPowerOfTwo(batchSize);

    /*
     * To reduce the heap overhead, we allocate the data-buffer and bitmap-buffer in a single
     * allocation. However, the combined allocation should be close (<=) to a power of 2, to avoid
     * wastage. The overhead of the bitmap is 1-bit for each element. The smallest element is an
     * int which is 4-bytes (other than booleans). So, the overhead would be 1 in every 32 elements.
     *
     * eg. if the batch-size is 4096, we will trim it by 4096/32, making it 3968.
     */
    return max(1, targetCount - targetCount / 32);
  }

}
