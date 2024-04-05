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

import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.store.TableMetadata;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;

public class DmlPositionalMergeOnReadPlanGenerator {
  public DmlPositionalMergeOnReadPlanGenerator() {
    throw new UnsupportedOperationException(
        "Merge On Read DML Not Supported At This Time. "
            + "Please configure your iceberg table properties to 'copy-on-write'");
  }

  public DmlPositionalMergeOnReadPlanGenerator(
      RelOptTable table,
      RelOptCluster cluster,
      RelTraitSet plus,
      RelNode convert,
      TableMetadata tableMetadata,
      CreateTableEntry createTableEntry,
      TableModify.Operation operation,
      List<String> strings,
      boolean b,
      OptimizerRulesContext context) {
    this();
  }

  public RelNode getPlan() {
    throw new UnsupportedOperationException(
        "Merge On Read DML Not Supported At This Time. "
            + "Please configure your iceberg table properties to 'copy-on-write'");
    // Builds the physical plan
  }
}
