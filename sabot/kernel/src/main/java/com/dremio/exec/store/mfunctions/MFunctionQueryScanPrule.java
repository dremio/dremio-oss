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
package com.dremio.exec.store.mfunctions;

import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.Prel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/** Prule for metadata functions, covert MFunctionQueryScanDrel to MFunctionQueryScanPrel */
public final class MFunctionQueryScanPrule extends ConverterRule {
  public static RelOptRule INSTANCE = new MFunctionQueryScanPrule();

  public MFunctionQueryScanPrule() {
    super(MFunctionQueryScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, "MFunctionQueryScanPrule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    MFunctionQueryScanDrel drel = (MFunctionQueryScanDrel) rel;
    return new MFunctionQueryScanPrel(
        drel.getCluster(),
        drel.getTraitSet().replace(Prel.PHYSICAL).replace(DistributionTrait.SINGLETON),
        drel.getRowType(),
        drel.getTableMetadata(),
        drel.getUser(),
        drel.metadataLocation);
  }
}
