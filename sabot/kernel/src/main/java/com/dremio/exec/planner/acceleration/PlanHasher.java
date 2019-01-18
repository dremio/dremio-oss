/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.acceleration;

import java.nio.charset.StandardCharsets;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.hash.Hashing;

/**
 * A tool used for hashing relnode plans to confirm that reflection normalization is the same before
 * and after persistence.
 */
public class PlanHasher {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanHasher.class);

  public static long hash(RelNode node) {
    RelNode cleansed = node.accept(new RoutingShuttle() {

      @Override
      public RelNode visit(RelNode other) {
        if( !(other instanceof ScanCrel) ) {
          return super.visit(other);
        }

        ScanCrel sc = (ScanCrel) other;
        return new GenericScan(sc.getTableMetadata().getName(), sc.getRowType(), sc.getCluster(), sc.getTraitSet());
      }});
    long hash = Hashing.murmur3_128().hashBytes(RelOptUtil.toString(cleansed).getBytes(StandardCharsets.UTF_8)).asLong();

    if(logger.isDebugEnabled()) {
      logger.debug("Hashed Plan {} to value {}", RelOptUtil.toString(cleansed), hash);
    }

    return hash;
  }

  private static class GenericScan extends AbstractRelNode {

    private final NamespaceKey path;

    public GenericScan(NamespaceKey path, RelDataType rowType, RelOptCluster cluster, RelTraitSet traitSet) {
      super(cluster, traitSet);
      this.path = path;
      this.rowType = rowType;
    }
    @Override
    public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw).item("path", path);
    }


  }
}
