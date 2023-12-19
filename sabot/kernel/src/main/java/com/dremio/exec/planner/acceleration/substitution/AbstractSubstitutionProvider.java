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
package com.dremio.exec.planner.acceleration.substitution;


import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.acceleration.DremioMaterialization;
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

  public List<DremioMaterialization> buildConsideredMaterializations(final RelNode origRoot) {
    return getMaterializationProvider().buildConsideredMaterializations(origRoot);
  }

  @Override
  public void setPostSubstitutionTransformer(RelTransformer transformer) {
    this.postSubstitutionTransformer = transformer;
  }
}
