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
package com.dremio.exec.store.sys.accel;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface AccelerationManager {
  Logger logger = LoggerFactory.getLogger(AccelerationManager.class);

  void addAcceleration(List<String> path);
  void dropAcceleration(List<String> path, boolean raiseErrorIfNotFound);
  void addLayout(List<String> path, LayoutDefinition definition);
  void dropLayout(List<String> path, String layoutId);
  void toggleAcceleration(List<String> path, LayoutDefinition.Type type, boolean enable);
  void replanlayout(String layoutId);
  AccelerationDetailsPopulator newPopulator();

  AccelerationManager NO_OP = new AccelerationManager() {
    @Override
    public void addAcceleration(List<String> path) {
      throw new UnsupportedOperationException("AcceleratorManager.addAcceleration() called on a non coordinator node");
    }

    @Override
    public void dropAcceleration(List<String> path, boolean raiseErrorIfNotFound) {
      throw new UnsupportedOperationException("AcceleratorManager.dropAcceleration() called on a non coordinator node");
    }

    @Override
    public void addLayout(List<String> path, LayoutDefinition definition) {
      throw new UnsupportedOperationException("AcceleratorManager.addLayout() called on a non coordinator node");
    }

    @Override
    public void dropLayout(List<String> path, String layoutId) {
      throw new UnsupportedOperationException("AcceleratorManager.dropLayout() called on a non coordinator node");
    }

    @Override
    public void toggleAcceleration(List<String> path, LayoutDefinition.Type type, boolean enable) {
      throw new UnsupportedOperationException("AcceleratorManager.toggleAcceleration() called on a non coordinator node");
    }

    @Override
    public void replanlayout(String layoutId) {
      throw new UnsupportedOperationException("AcceleratorManager.replanlayout() called on a non coordinator node");
    }

    @Override
    public AccelerationDetailsPopulator newPopulator() {
      return AccelerationDetailsPopulator.NO_OP;
    }
  };
}
