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
package com.dremio.exec.store.sys.accel;

import java.util.List;

import com.dremio.exec.ops.ReflectionContext;

public interface AccelerationManager {
  void dropAcceleration(List<String> path, boolean raiseErrorIfNotFound);
  void addLayout(List<String> path, LayoutDefinition definition, ReflectionContext reflectionContext);
  void addExternalReflection(String name, List<String> table, List<String> targetTable, ReflectionContext reflectionContext);
  void dropLayout(List<String> path, String layoutId, ReflectionContext reflectionContext);
  void toggleAcceleration(List<String> path, LayoutDefinition.Type type, boolean enable, ReflectionContext reflectionContext);
  void replanlayout(String layoutId);
  <T> T unwrap(Class<T> clazz);

  ExcludedReflectionsProvider getExcludedReflectionsProvider();

  AccelerationDetailsPopulator newPopulator();

  AccelerationManager NO_OP = new AccelerationManager() {
    @Override
    public void dropAcceleration(List<String> path, boolean raiseErrorIfNotFound) {
      throw new UnsupportedOperationException("AccelerationManager.dropAcceleration() called on a non-coordinator node");
    }

    @Override
    public void addLayout(List<String> path, LayoutDefinition definition, ReflectionContext reflectionContext) {
      throw new UnsupportedOperationException("AccelerationManager.addLayout() called on a non-coordinator node");
    }

    @Override
    public void addExternalReflection(String name, List<String> table, List<String> targetTable, ReflectionContext reflectionContext) {
      throw new UnsupportedOperationException("AccelerationManager.addExternalReflection() called on a non-coordinator node");
    }

    @Override
    public void dropLayout(List<String> path, String layoutId, ReflectionContext reflectionContext) {
      throw new UnsupportedOperationException("AccelerationManager.dropLayout() called on a non-coordinator node");
    }

    @Override
    public void toggleAcceleration(List<String> path, LayoutDefinition.Type type, boolean enable, ReflectionContext reflectionContext) {
      throw new UnsupportedOperationException("AccelerationManager.toggleAcceleration() called on a non-coordinator node");
    }

    @Override
    public void replanlayout(String layoutId) {
      throw new UnsupportedOperationException("AccelerationManager.replanlayout() called on a non-coordinator node");
    }

    @Override
    public ExcludedReflectionsProvider getExcludedReflectionsProvider() {
      throw new UnsupportedOperationException("AccelerationManager.getExcludedReflectionsProvider() called on a non-coordinator node");
    }

    @Override
    public AccelerationDetailsPopulator newPopulator() {
      return AccelerationDetailsPopulator.NO_OP;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      return null;
    }
  };

  /**
   * Provides excluded reflections lookup.
   */
  public interface ExcludedReflectionsProvider {
    List<String> getExcludedReflections(String rId);
  }
}
