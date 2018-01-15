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
package com.dremio.exec.server;

import java.util.List;

import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

/**
 * A materialization provider
 */
public interface MaterializationDescriptorProvider {
  /**
   * Provides a list of materialization instances.
   *
   * @return a list of {@code Materialization} instances. Might be empty.
   */
  List<MaterializationDescriptor> get();

  /**
   * returns materialization for a given id
   */
   Optional<MaterializationDescriptor> get(String materializationId);

  @VisibleForTesting
  /**
   * enables debug mode
   */
  void setDebug(boolean debug);

  /**
   * add or update with a new materialization
   * @param materializationDescriptor
   */

  void update(MaterializationDescriptor materializationDescriptor);

  /**
   * Provides a list of latest materialization instances.
   * includes or excludes incomplete (expired or not on active hosts) based on the boolean argument
   *
   * @return a list of {@code Materialization} instances. Might be empty.
   */
  List<MaterializationDescriptor> get(boolean includeInComplete);

  /**
   * remove a materialization
   */
  void remove(String materializationId);

  /**
   * Empty materialization provider.
   */
  MaterializationDescriptorProvider EMPTY = new MaterializationDescriptorProvider() {

    @Override
    public List<MaterializationDescriptor> get() {
      return ImmutableList.<MaterializationDescriptor>of();
    }

    @Override
    public void setDebug(boolean debug) {}

    @Override
    public Optional<MaterializationDescriptor> get(String materializationId) {
      return Optional.absent();
    }

    @Override
    public void update(MaterializationDescriptor materializationDescriptor) {
    }

    @Override
    public List<MaterializationDescriptor> get(boolean includeInComplete) {
      return get();
    }

    @Override
    public void remove(String materializationId) {
    }
  };
}
