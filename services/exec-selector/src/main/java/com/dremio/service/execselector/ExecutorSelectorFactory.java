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
 * A factory for {@link ExecutorSelector}s
 */
public interface ExecutorSelectorFactory {
  /**
   * Create a {@link ExecutorSelector}
   * @param selectorType  What kind of executor selector this factory should create
   * @param rwLock        A read-write lock that the resulting {@link ExecutorSelector} can use to protect
   *                      entry points that do not come from the executor selector service
   */
  ExecutorSelector createExecutorSelector(String selectorType, ReentrantReadWriteLock rwLock);
}
