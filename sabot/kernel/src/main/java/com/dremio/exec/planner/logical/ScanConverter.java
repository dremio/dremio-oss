/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import com.dremio.exec.planner.StatelessRelShuttleImpl;

/**
 * A visitor that converts every {@link ConvertibleScan} seen to its concrete representation.
 *
 * Past this visitor the plan is not guaranteed to be serializable.
 */
public class ScanConverter extends StatelessRelShuttleImpl {

  public static final ScanConverter INSTANCE = new ScanConverter();
  private static final SubQueryConverter SUBQUERY_CONVERTER = new SubQueryConverter();

  @Override
  public RelNode visit(final TableScan scan) {
    if (scan instanceof ConvertibleScan) {
      return ((ConvertibleScan) scan).convert();
    }
    return super.visit(scan);
  }

  @Override
  public RelNode visit(final LogicalFilter filter) {
    final LogicalFilter newFilter = (LogicalFilter) filter.accept(SUBQUERY_CONVERTER);
    return super.visit(newFilter);
  }

  @Override
  public RelNode visit(final LogicalProject project) {
    final LogicalProject newProject = (LogicalProject) project.accept(SUBQUERY_CONVERTER);
    return super.visit(newProject);
  }

  @Override
  public RelNode visit(final LogicalJoin join) {
    final LogicalJoin newJoin = (LogicalJoin) join.accept(SUBQUERY_CONVERTER);
    return super.visit(newJoin);
  }

  private static class SubQueryConverter extends RexShuttle {

    @Override
    public RexNode visitSubQuery(final RexSubQuery subQuery) {
      final RexSubQuery rex = (RexSubQuery) super.visitSubQuery(subQuery);
      final RelNode converted = subQuery.rel.accept(INSTANCE);
      if (converted == subQuery.rel) {
        return rex;
      }

      return rex.clone(converted);
    }

  }

}
