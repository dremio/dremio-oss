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
package com.dremio.exec.planner.acceleration.substitution;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RuleSet;

import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.google.common.base.Preconditions;

/**
 * A base {@link SubstitutionProvider} that maintains a {@link MaterializationProvider}.
 */
public abstract class AbstractSubstitutionProvider implements SubstitutionProvider {
  private final MaterializationProvider provider;
  protected RelTransformer postSubstitutionTransformer;

  protected AbstractSubstitutionProvider(
    final MaterializationProvider materializations) {
    this.provider = Preconditions.checkNotNull(materializations,
      "materialization provider is required");
  }

  public MaterializationProvider getMaterializationProvider() {
    return provider;
  }

  public List<RelOptMaterialization> getMaterializations() {
    return getMaterializationProvider().getMaterializations();
  }

  @Override
  public void setPostSubstitutionTransformer(RelTransformer transformer) {
    this.postSubstitutionTransformer = transformer;
  }

}

