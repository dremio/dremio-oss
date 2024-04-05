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
package com.dremio.sabot.threads.sharedres;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks a particular resource and whether it is available. Note that rule is currently:
 *
 * <p>- Only the inside thread (single) may cause a blocked state. - The outside threads (many) may
 * cause an unblocked state.
 *
 * <p>To avoid any concurrency issues, any operations that are doing something depending on this
 * much also synchronize on this object so that no race conditions are introduced.
 */
public class SharedResource {
  private final SharedResourceGroup resourceGroup;
  private final AtomicBoolean disabled = new AtomicBoolean(false);
  private final AtomicBoolean available = new AtomicBoolean(true);
  private final String name;
  private final SharedResourceType resourceType;

  SharedResource(SharedResourceGroup resourceGroup, String name, SharedResourceType resourceType) {
    this.resourceGroup = resourceGroup;
    this.name = name;
    this.resourceType = resourceType;
  }

  void disableBlocking() {
    synchronized (this) {
      disabled.set(true);
      markAvailable();
    }
  }

  public void markBlocked() {
    synchronized (this) {
      if (!disabled.get() && available.compareAndSet(true, false)) {
        resourceGroup.addBlocked();
      }
    }
  }

  public void markAvailable() {
    synchronized (this) {
      if (available.compareAndSet(false, true)) {
        resourceGroup.removeBlocked();
      }
    }
  }

  public boolean isAvailable() {
    return available.get();
  }

  public String getName() {
    return name;
  }

  public SharedResourceType getType() {
    return resourceType;
  }
}
