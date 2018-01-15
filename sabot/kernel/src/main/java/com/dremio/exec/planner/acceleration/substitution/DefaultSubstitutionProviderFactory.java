/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.plan.RelOptMaterialization;

import com.dremio.exec.server.options.OptionManager;

/**
 * Factory class to create default (calcite) {@code org.apache.calcite.plan.substitution.UnifyingSubstitutionProvider}
 *
 */
public class DefaultSubstitutionProviderFactory implements SubstitutionProviderFactory {

  public DefaultSubstitutionProviderFactory() {
  }

  @Override
  public SubstitutionProvider getSubstitutionProvider(
      MaterializationProvider materializationProvider, OptionManager options) {
    return new UnifyingSubstitutionProvider(materializationProvider);
  }
}
