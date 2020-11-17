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
package com.dremio.exec.planner.common;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.store.RecordWriter;

/** Base class for logical and physical Writer implemented in Dremio. */
public abstract class WriterRelBase extends SingleRel {

  private final CreateTableEntry createTableEntry;

  public WriterRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      CreateTableEntry createTableEntry) {
    super(cluster, traitSet, input);
    assert input.getConvention() == convention;
    this.createTableEntry = createTableEntry;

    rowType = CalciteArrowHelper.wrap(RecordWriter.SCHEMA).toCalciteRecordType(getCluster().getTypeFactory(), PrelUtil.getPlannerSettings(getCluster()).isFullNestedSchemaSupport());
  }

  public CreateTableEntry getCreateTableEntry() {
    return createTableEntry;
  }

}
