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
import java.util.Optional;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;

/**
 * A base {@link SubstitutionProvider} that maintains a {@link MaterializationProvider}.
 */
public abstract class AbstractSubstitutionProvider implements SubstitutionProvider {
  private final MaterializationProvider provider;
  protected List<RelTransformer> postSubstitutionTransformers;
  protected RelNode currentPlan;

  protected AbstractSubstitutionProvider(
    final MaterializationProvider materializations) {
    this.provider = Preconditions.checkNotNull(materializations,
      "materialization provider is required");
  }

  public MaterializationProvider getMaterializationProvider() {
    return provider;
  }

  public List<DremioMaterialization> getMaterializations() {
    return getMaterializationProvider().getApplicableMaterializations();
  }

  public List<DremioMaterialization> buildApplicableMaterializations(final RelNode origRoot) {
    return getMaterializationProvider().buildApplicableMaterializations(origRoot);
  }

  public Optional<DremioMaterialization> getDefaultRawMaterialization(NamespaceKey path,
                                                                      TableVersionContext versionContext, List<String> vdsFields, Catalog catalog) {
    return getMaterializationProvider().getDefaultRawMaterialization(path, versionContext, vdsFields, catalog);
  }

  @Override
  public void setPostSubstitutionTransformers(List<RelTransformer> transformers) {
    this.postSubstitutionTransformers = transformers;
  }

  @Override
  public void setCurrentPlan(RelNode currentPlan) {
    this.currentPlan = currentPlan;
  }


}
