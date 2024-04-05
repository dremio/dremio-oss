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
package com.dremio.exec.planner.logical;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.common.TableOptimizeRelBase;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.sql.handlers.query.OptimizeOptions;
import com.dremio.exec.store.dfs.FilterableScan;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/** Drel for 'OPTIMIZE TABLE'. */
public class TableOptimizeRel extends TableOptimizeRelBase implements Rel {

  private final PruneFilterCondition partitionFilter;

  public TableOptimizeRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelOptTable table,
      CreateTableEntry createTableEntry,
      OptimizeOptions optimizeOptions,
      PruneFilterCondition partitionFilter) {
    super(LOGICAL, cluster, traitSet, input, table, createTableEntry, optimizeOptions);
    this.partitionFilter = partitionFilter;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    RelNode relNode = sole(inputs);
    PruneFilterCondition pruneFilterCondition =
        partitionFilter == null ? computePartitionFilter(relNode) : partitionFilter;
    return new TableOptimizeRel(
        getCluster(),
        traitSet,
        relNode,
        getTable(),
        getCreateTableEntry(),
        getOptimizeOptions(),
        pruneFilterCondition);
  }

  /** Rule for filters with ICEBERG Compaction. */
  private PruneFilterCondition computePartitionFilter(RelNode relNode) {
    PruneFilterCondition pruneFilterCondition = null;
    if (relNode instanceof FilterableScan) {
      // All filters have been resolved by pruning; In this case, there is nothing to push down for
      // select scan
      pruneFilterCondition = ((FilterableScan) relNode).getPartitionFilter();
    } else if (relNode instanceof FilterRel) {
      // Things have been pruned; In this case, select scan does parquet push down too.
      FilterableScan scan = (FilterableScan) ((FilterRel) relNode).getInput();
      pruneFilterCondition = scan.getPartitionFilter();
      if ((pruneFilterCondition == null || pruneFilterCondition.isEmpty())) {
        // Nothing was pruned, In this case, select scan does parquet push down only.
        throw UserException.unsupportedError()
            .message(
                String.format(
                    "OPTIMIZE command is only supported on the partition columns - %s",
                    scan.getTableMetadata().getReadDefinition().getPartitionColumnsList()))
            .buildSilently();
      }
    }
    return pruneFilterCondition;
  }

  public PruneFilterCondition getPartitionFilter() {
    return partitionFilter;
  }
}
