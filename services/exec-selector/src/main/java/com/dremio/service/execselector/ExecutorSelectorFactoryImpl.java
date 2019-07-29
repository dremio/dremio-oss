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
package com.dremio.service.execselector;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Factory that creates the universal executor selector
 */
public class ExecutorSelectorFactoryImpl implements ExecutorSelectorFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecutorSelectorFactoryImpl.class);

  /**
   * if the selectorType is default or universal, return a universal based executor selector
   *
   * @param selectorType  What kind of executor selector this factory should create
   * @param rwLock        A read-write lock that the resulting {@link ExecutorSelector} can use to protect
   */
  @Override
  public ExecutorSelector createExecutorSelector(String selectorType, ReentrantReadWriteLock rwLock) {
    if (ExecutorSelectionService.DEFAULT_SELECTOR_TYPE.equals(selectorType) || UniversalExecutorSelector.EXECUTOR_SELECTOR_TYPE.equals(selectorType)) {
      return new UniversalExecutorSelector();
    }
    throw new IllegalArgumentException(String.format("Unsupported executor selector %s", selectorType));
  }
}
