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

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.google.common.base.Preconditions;

/**
 * Base class for 'COPY INTO'
 */
public abstract class CopyIntoTableRelBase extends AbstractRelNode implements Rel {

  private final RelOptTable table;
  private final CopyIntoTableContext context;

  protected CopyIntoTableRelBase(Convention convention,
                                 RelOptCluster cluster,
                                 RelTraitSet traitSet,
                                 RelOptTable table,
                                 CopyIntoTableContext config) {
    super(cluster, traitSet);
    assert getConvention() == convention;
    this.table = table;
    this.context = Preconditions.checkNotNull(config, "CopyInto context can't be null!");
    rowType = deriveRowType();
  }

  @Override
  public RelDataType deriveRowType() {
    return table.getRowType();
  }

  @Override
  public RelOptTable getTable() {
    return table;
  }

  public CopyIntoTableContext getContext() {
    return context;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (table != null) {
      pw.item("table", table.getQualifiedName());
    }
    pw.item("operation", "COPY INTO");
    pw.item("columns", deriveRowType().getFieldNames());
    pw.item("storage location", context.getStorageLocation());
    if (context.getFilePattern().isPresent()) {
      pw.item("file pattern", context.getFilePattern().get());
    }
    if (!context.getFiles().isEmpty()) {
      pw.item("files", context.getFiles());
    }
    if (!context.getFormatOptions().isEmpty()) {
      pw.item("format options", context.getFormatOptions());
    }
    if (!context.getCopyOptions().isEmpty()) {
      pw.item("copy options", context.getCopyOptions());
    }
    return pw;
  }

  /**
   * Subclasses must override copy to avoid problems where duplicate scan operators are
   * created due to the same (reference-equality) Prel being used multiple times in the plan.
   * The copy implementation in AbstractRelNode just returns a reference to "this".
   */
  @Override
  public abstract RelNode copy(RelTraitSet traitSet, List<RelNode> inputs);
}
