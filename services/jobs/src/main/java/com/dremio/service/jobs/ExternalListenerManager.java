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
package com.dremio.service.jobs;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages status listeners for a particular job. Is thread safe, ensuring that
 * any register listener is informed of a completion event, whether or not there
 * is a race.
 */
class ExternalListenerManager {
  private final List<ExternalStatusListener> statusListeners = new CopyOnWriteArrayList<>();

  private volatile boolean active = true;
  private volatile Job job;

  public synchronized void register(ExternalStatusListener listener) {
    if (!active) {
      listener.queryCompleted(job);
    } else {
      statusListeners.add(listener);
    }
  }

  public synchronized void queryUpdate(Job job) {
    if (active) {
      for (ExternalStatusListener listener : statusListeners) {
        listener.profileUpdated(job);
      }
    }
  }

  public synchronized void close(Job job) {
    active = false;
    this.job = job;
    for (ExternalStatusListener listener : statusListeners) {
      listener.queryCompleted(job);
    }
  }
}
