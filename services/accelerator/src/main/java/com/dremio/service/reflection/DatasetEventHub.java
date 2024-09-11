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
package com.dremio.service.reflection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DatasetEventHub {
  private final List<DatasetRemovedHandler> datasetRemovedHandlers = new ArrayList<>();
  private final ReadWriteLock datasetRemovedHandlersLock = new ReentrantReadWriteLock();

  public void addDatasetRemovedHandler(DatasetRemovedHandler handler) {
    datasetRemovedHandlersLock.writeLock().lock();
    try {
      datasetRemovedHandlers.add(handler);
    } finally {
      datasetRemovedHandlersLock.writeLock().unlock();
    }
  }

  public void fireRemoveDatasetEvent(DatasetRemovedEvent event) {
    datasetRemovedHandlersLock.readLock().lock();
    try {
      for (DatasetRemovedHandler handler : datasetRemovedHandlers) {
        handler.handle(event);
      }
    } finally {
      datasetRemovedHandlersLock.readLock().unlock();
    }
  }
}
