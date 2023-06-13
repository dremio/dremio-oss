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
package com.dremio.exec.catalog.udf;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

public final class CorrelatedUdfDetector extends RexShuttle {
  private static final class RexDetector extends RexShuttle {
    private boolean hasRexSubQuery;

    private RexDetector() {
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      this.hasRexSubQuery = true;
      return subQuery;
    }
  }

  private static final class RelDetector extends RelHomogeneousShuttle {
    private final RexDetector rexDetector;

    public RelDetector() {
      rexDetector = new RexDetector();
    }

    @Override
    public RelNode visit(RelNode other) {
      other.accept(rexDetector);
      if (rexDetector.hasRexSubQuery) {
        return other;
      }

      return visitChildren(other);
    }
  }

  public static boolean hasCorrelatedUdf(RexNode rexNode) {
    RexDetector detector = new RexDetector();
    rexNode.accept(detector);

    return detector.hasRexSubQuery;
  }

  public static boolean hasCorrelatedUdf(RelNode relNode) {
    RelDetector detector = new RelDetector();
    relNode.accept(detector);
    return detector.rexDetector.hasRexSubQuery;
  }
}
