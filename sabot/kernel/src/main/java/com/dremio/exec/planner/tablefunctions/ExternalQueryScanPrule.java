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
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.tablefunctions.ExternalQueryScanDrel;
import com.dremio.exec.tablefunctions.ExternalQueryScanPrel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/** Rule to convert ExternalQuerynScanDrel nodes to ExternalQueryIntermediateScanPrel nodes */
public final class ExternalQueryScanPrule extends ConverterRule {
  public static final RelOptRule INSTANCE = new ExternalQueryScanPrule();

  private ExternalQueryScanPrule() {
    super(
        ExternalQueryScanDrel.class,
        Rel.LOGICAL,
        Prel.PHYSICAL,
        "ExternalQueryScanDrel_To_ExternalQueryScanPrel_Converter_");
  }

  @Override
  public RelNode convert(RelNode relNode) {
    final ExternalQueryScanDrel scan = (ExternalQueryScanDrel) relNode;
    final RelTraitSet physicalTraits =
        scan.getTraitSet().replace(getOutTrait()).replace(DistributionTrait.SINGLETON);

    return new ExternalQueryScanPrel(
        scan.getCluster(),
        physicalTraits,
        scan.getRowType(),
        scan.getPluginId(),
        scan.getSql(),
        scan.getBatchSchema());
  }
}
