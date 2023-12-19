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

import java.util.Optional;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;

public class AccelerationAwareSubstitutionProvider implements SubstitutionProvider, Observable {
  private static final Logger logger = LoggerFactory.getLogger(AccelerationAwareSubstitutionProvider.class);

  private boolean enabled = false;
  private final SubstitutionProvider delegate;

  public AccelerationAwareSubstitutionProvider(final SubstitutionProvider delegate) {
    this(delegate, false);
  }

  public AccelerationAwareSubstitutionProvider(final SubstitutionProvider delegate, final boolean enabled) {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate is required");
    this.enabled = enabled;
  }

  @Override
  public void setObserver(AttemptObserver observer) {
    if (delegate instanceof Observable) {
      ((Observable)delegate).setObserver(observer);
    }
  }

  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public void setCurrentPlan(RelNode currentPlan) {
    delegate.setCurrentPlan(currentPlan);
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public SubstitutionStream findSubstitutions(final RelNode query) {
    if (isEnabled()) {
      return delegate.findSubstitutions(query);
    } else {
      logger.debug("Acceleration is disabled. No substitutions...");
      return SubstitutionStream.empty();
    }
  }

  @Override
  public void setPostSubstitutionTransformer(RelTransformer transformer) {
    delegate.setPostSubstitutionTransformer(transformer);
  }

  @Override
  public RelNode wrapDefaultExpansionNode(NamespaceKey path, final RelNode query, DremioMaterialization materialization, RelDataType rowType,
                                   boolean contextSensitive, TableVersionContext versionContext, SqlConverter converter) {
    return delegate.wrapDefaultExpansionNode(path, query, materialization, rowType, contextSensitive, versionContext, converter);
  }

  @Override
  public Optional<DremioMaterialization> getDefaultRawMaterialization(ViewTable table) {
    return delegate.getDefaultRawMaterialization(table);
  }

  @Override
  public boolean isDefaultRawReflectionEnabled() {
    return delegate.isDefaultRawReflectionEnabled();
  }

  @Override
  public void disableDefaultRawReflection() {
    delegate.disableDefaultRawReflection();
  }

  @Override
  public void resetDefaultRawReflection() {
    delegate.resetDefaultRawReflection();
  }

  @Override
  public Set<String> getMatchedReflections() {
    return delegate.getMatchedReflections();
  }

  public static AccelerationAwareSubstitutionProvider of(final SubstitutionProvider delegate) {
    return new AccelerationAwareSubstitutionProvider(delegate);
  }
}
