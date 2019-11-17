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

package com.dremio.exec.planner.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import com.dremio.exec.calcite.logical.JdbcCrel;

/**
 * Converts JdbcCrel into logical.
 */
public class JdbcDrule extends ConverterRule {

  public static final JdbcDrule INSTANCE = new JdbcDrule();

  private JdbcDrule() {
    super(JdbcCrel.class, Convention.NONE, Rel.LOGICAL, "JdbcDrule");
  }

  @Override
  public RelNode convert(RelNode in) {
    JdbcCrel rel = (JdbcCrel) in;
    return new JdbcCrel(rel.getCluster(), rel.getTraitSet().replace(Rel.LOGICAL),
            rel.getInput(), rel.getPluginId());
  }
}