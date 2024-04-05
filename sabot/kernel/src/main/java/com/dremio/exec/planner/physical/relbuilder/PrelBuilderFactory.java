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
package com.dremio.exec.planner.physical.relbuilder;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilderFactory;

public class PrelBuilderFactory implements RelBuilderFactory {
  private static final Context CONTEXT = Contexts.of(PrelFactories.FILTER, PrelFactories.PROJECT);
  public static final PrelBuilderFactory INSTANCE = new PrelBuilderFactory();

  @Override
  public PrelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
    return new PrelBuilder(CONTEXT, cluster, schema);
  }
}
