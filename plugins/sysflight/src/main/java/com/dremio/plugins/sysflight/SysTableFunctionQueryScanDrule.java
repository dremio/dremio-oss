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
package com.dremio.plugins.sysflight;

import com.dremio.exec.planner.logical.Rel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class SysTableFunctionQueryScanDrule extends ConverterRule {

  public static final SysTableFunctionQueryScanDrule INSTANCE =
      new SysTableFunctionQueryScanDrule();

  public SysTableFunctionQueryScanDrule() {
    super(
        SysTableFunctionQueryScanCrel.class,
        Convention.NONE,
        Rel.LOGICAL,
        "SysTableFunctionQueryScanRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final SysTableFunctionQueryScanCrel node = (SysTableFunctionQueryScanCrel) rel;
    return new SysTableFunctionQueryScanDrel(
        node.getCluster(),
        node.getTraitSet().replace(Rel.LOGICAL),
        node.getRowType(),
        node.getMetadata(),
        node.getUser());
  }
}
