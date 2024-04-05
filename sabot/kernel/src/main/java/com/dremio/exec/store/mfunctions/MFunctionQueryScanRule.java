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
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/** Rule to convert MFunctionQueryScanCrel nodes to MFunctionQueryScanDrel nodes. */
public final class MFunctionQueryScanRule extends ConverterRule {

  public static MFunctionQueryScanRule INSTANCE = new MFunctionQueryScanRule();

  public MFunctionQueryScanRule() {
    super(MFunctionQueryScanCrel.class, Convention.NONE, Rel.LOGICAL, "MFunctionQueryScanRule");
  }

  @Override
  public RelNode convert(RelNode relNode) {
    final MFunctionQueryScanCrel node = (MFunctionQueryScanCrel) relNode;
    return new MFunctionQueryScanDrel(
        node.getCluster(),
        node.getTraitSet().replace(Rel.LOGICAL),
        node.getRowType(),
        node.getTableMetadata(),
        node.getUser(),
        node.metadataLocation);
  }
}
