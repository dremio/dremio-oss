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
package com.dremio.partitionstats.cache;

import static com.dremio.proto.model.PartitionStats.PartitionStatsKey;
import static com.dremio.proto.model.PartitionStats.PartitionStatsValue;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNamedInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNormalize;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.transientstore.TransientStore;

/**
 * Class to cache partition stats i.e. file counts and row counts.
 */
public class PartitionStatsCache {

  private final TransientStore<PartitionStatsKey, PartitionStatsValue> transientStore;

  /**
   * Initialize the store for caching the stats.
   * @param transientStore store for cache.
   */
  public PartitionStatsCache(TransientStore<PartitionStatsKey, PartitionStatsValue> transientStore){
    this.transientStore = transientStore;
  }

  /**
   * To get stats from cache.
   * @param partitionStatsKey a key which is a combination of file name and prune condition
   * @return partition stats for given inputs.
   */
  public PartitionStatsValue get(PartitionStatsKey partitionStatsKey) {
    Document<PartitionStatsKey, PartitionStatsValue> document = transientStore.get(partitionStatsKey);

    if (document != null) {
      return document.getValue();
    }
    return null;
  }

  /**
   * Insert entry to the cache.
   * @param partitionStatsKey a key which is a combination of file name and prune condition
   * @param value Partition stats
   */
  public void put(PartitionStatsKey partitionStatsKey, PartitionStatsValue value) {
    transientStore.put(partitionStatsKey, value);
  }

  /**
   * creates a partition stats cache key from the given inputs.
   * @param fileName name of the partition stats file
   * @param condition prune condition
   * @param projectedColumns name of the projected columns
   * @return a key which is a combination of file name and prune condition.
   */
  public PartitionStatsKey computeKey(RexBuilder rexB, String fileName, RexNode condition, List<String> projectedColumns){

    return PartitionStatsKey.newBuilder()
      .setPartitionFilter(normalize(rexB, condition, projectedColumns).toString())
      .setPartitionStatsFileName(fileName)
      .build();
  }

  private RexNode normalize(RexBuilder rexB, RexNode condition, List<String> projectedColumns){
    return condition.accept(new RexShuttle(){
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        return RexNamedInputRef.of(inputRef, projectedColumns.get(inputRef.getIndex()));
      }

      @Override
      public RexNode visitCall(RexCall call) {
        List<RexNode> operands = call.getOperands();
        List<RexNode> newOperands = new ArrayList<>();
        for(RexNode op : operands){
          newOperands.add(op.accept(this));
        }
        Pair<SqlOperator, List<RexNode>> normalized = RexNormalize.normalize(call.getOperator(), newOperands);
        return rexB.makeCall(call.getType(), normalized.left, normalized.right);
      }
    });
  }
}
