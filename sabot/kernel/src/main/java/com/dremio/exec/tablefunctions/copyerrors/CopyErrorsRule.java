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
package com.dremio.exec.tablefunctions.copyerrors;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import com.dremio.exec.planner.logical.Rel;

/**
 * Rule to convert {@link CopyErrorsCrel} nodes to {@link CopyErrorsDrel} nodes.
 */
public final class CopyErrorsRule extends ConverterRule {

  public static final CopyErrorsRule INSTANCE = new CopyErrorsRule();

  public CopyErrorsRule() {
    super(CopyErrorsCrel.class, Convention.NONE, Rel.LOGICAL, "CopyErrorsRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final CopyErrorsCrel node = (CopyErrorsCrel) rel;
    return new CopyErrorsDrel(
      node.getCluster(),
      node.getTraitSet().replace(Rel.LOGICAL),
      node.getContext(),
      node.getMetadata());
  }
}
