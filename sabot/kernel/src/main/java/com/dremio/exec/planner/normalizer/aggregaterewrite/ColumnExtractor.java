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
package com.dremio.exec.planner.normalizer.aggregaterewrite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;

/** Extracts a column from the RelNode */
public final class ColumnExtractor extends HepRelPassthroughShuttle {
  private final int index;
  private RexNode node;

  private ColumnExtractor(int index) {
    this.index = index;
  }

  @Override
  public RelNode visit(LogicalProject project) {
    node = project.getChildExps().get(index);
    return project;
  }

  public static RexNode extract(RelNode node, int columnIndex) {
    ColumnExtractor extractor = new ColumnExtractor(columnIndex);
    node.accept(extractor);
    if (extractor.node == null) {
      // Failed to extract the columns.
      // Visitor will have to be extended to support new RelNode types.
      throw new UnsupportedOperationException("Failed to extract columns from the RelNode.");
    }

    return extractor.node;
  }
}
