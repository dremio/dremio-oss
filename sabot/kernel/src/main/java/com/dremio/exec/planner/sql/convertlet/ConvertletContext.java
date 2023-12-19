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
package com.dremio.exec.planner.sql.convertlet;

import java.util.function.Supplier;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.tools.RelBuilder;

public final class ConvertletContext {
  private final Supplier<RexCorrelVariable> rexCorrelVariable;
  private final RelBuilder relBuilder;
  private final RexBuilder rexBuilder;

  public ConvertletContext(
    Supplier<RexCorrelVariable> rexCorrelVariable,
    RelBuilder relBuilder,
    RexBuilder rexBuilder) {
    this.rexCorrelVariable = rexCorrelVariable;
    this.relBuilder = relBuilder;
    this.rexBuilder = rexBuilder;
  }

  public RexCorrelVariable getRexCorrelVariable() {
    return rexCorrelVariable.get();
  }

  public RelBuilder getRelBuilder() {
    return relBuilder;
  }

  public RexBuilder getRexBuilder() {
    return rexBuilder;
  }
}
