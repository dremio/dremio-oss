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
package com.dremio.exec.planner.tablefunctions;

import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.tablefunctions.ExternalQueryScanCrel;
import com.dremio.exec.tablefunctions.ExternalQueryScanDrel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/** Rule to convert ExternalQueryScanCrel nodes to ExternalQueryScanDrel nodes. */
public final class ExternalQueryScanRule extends ConverterRule {
  public static final ExternalQueryScanRule INSTANCE = new ExternalQueryScanRule();

  private ExternalQueryScanRule() {
    super(
        ExternalQueryScanCrel.class,
        Convention.NONE,
        Rel.LOGICAL,
        "ExternalQueryScanCrelConverter");
  }

  @Override
  public RelNode convert(RelNode relNode) {
    final ExternalQueryScanCrel node = (ExternalQueryScanCrel) relNode;
    return new ExternalQueryScanDrel(
        node.getCluster(),
        node.getTraitSet().replace(Rel.LOGICAL),
        node.getRowType(),
        node.getPluginId(),
        node.getSql(),
        node.getBatchSchema());
  }
}
