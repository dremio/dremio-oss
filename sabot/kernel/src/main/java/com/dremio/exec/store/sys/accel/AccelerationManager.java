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
package com.dremio.exec.store.sys.accel;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface AccelerationManager {
  Logger logger = LoggerFactory.getLogger(AccelerationManager.class);

  void dropAcceleration(List<String> path, boolean raiseErrorIfNotFound);
  void addLayout(List<String> path, LayoutDefinition definition);
  void addExternalReflection(String name, List<String> table, List<String> targetTable);
  void dropLayout(List<String> path, String layoutId);
  void toggleAcceleration(List<String> path, LayoutDefinition.Type type, boolean enable);
  void replanlayout(String layoutId);
  <T> T unwrap(Class<T> clazz);

  AccelerationDetailsPopulator newPopulator();

  AccelerationManager NO_OP = new AccelerationManager() {
    @Override
    public void dropAcceleration(List<String> path, boolean raiseErrorIfNotFound) {
      throw new UnsupportedOperationException("AccelerationManager.dropAcceleration() called on a non-coordinator node");
    }

    @Override
    public void addLayout(List<String> path, LayoutDefinition definition) {
      throw new UnsupportedOperationException("AccelerationManager.addLayout() called on a non-coordinator node");
    }

    @Override
    public void addExternalReflection(String name, List<String> table, List<String> targetTable) {
      throw new UnsupportedOperationException("AccelerationManager.addExternalReflection() called on a non-coordinator node");
    }

    @Override
    public void dropLayout(List<String> path, String layoutId) {
      throw new UnsupportedOperationException("AccelerationManager.dropLayout() called on a non-coordinator node");
    }

    @Override
    public void toggleAcceleration(List<String> path, LayoutDefinition.Type type, boolean enable) {
      throw new UnsupportedOperationException("AccelerationManager.toggleAcceleration() called on a non-coordinator node");
    }

    @Override
    public void replanlayout(String layoutId) {
      throw new UnsupportedOperationException("AccelerationManager.replanlayout() called on a non-coordinator node");
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
