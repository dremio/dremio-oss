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
package com.dremio.sabot.threads.sharedres;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.dremio.sabot.threads.AvailabilityCallback;
import com.google.common.base.Preconditions;

/**
 * Manages whether or not resources are available to do work. Provides a nonblocking way to
 * register a collection of shared resources and determine whether the resources are available.
 * Resources are organized in groups, the manager is only blocked if all groups are.
 */
public class SharedResourceManager {

  private final Object nextCallbackLock = new Object();

  // this map is not expected to be modified once the builder is done constructing the resource manager
  private final Map<String, SharedResourceGroup> groups = new HashMap<>();
  private final AtomicInteger availableGroups = new AtomicInteger();
  private volatile AvailabilityCallback nextCallback;

  private SharedResourceManager() {
  }

  private SharedResourceGroup createGroup(String name) {
    SharedResourceGroup group = new SharedResourceGroup(this, name);
    groups.put(name, group);
    return group;
  }

  public SharedResourceGroup getGroup(String name) {
    return Preconditions.checkNotNull(groups.get(name), "group not found: " + name);
  }

  /**
   * Set the callback the next time this shared resource manager becomes
   * available. Note that this will be called immediately (in thread) if the
   * manager is currently unblocked.
   *
   * @param callback
   *          The callback to inform.
   */
  public void setNextCallback(AvailabilityCallback callback) {
    synchronized(nextCallbackLock){
      Preconditions.checkArgument(nextCallback == null, "Can't add a callback when one is already pending.");
      nextCallback = callback;
      if (availableGroups.get() > 0) {
        executeNextCallback();
      }
    }
  }

  private void executeNextCallback() {
    synchronized(nextCallbackLock) {
      // ensure blockedResources == 0 as another thread might have marked a resource as blocked
      if (nextCallback != null && availableGroups.get() > 0) {
        nextCallback.nowAvailable();
        nextCallback = null;
      }
    }
  }

  void removeAvailable() {
    availableGroups.decrementAndGet();
  }

  void addAvailable() {
    if (availableGroups.getAndIncrement() == 0) {
      executeNextCallback();
    }
  }

  public boolean isAvailable(){
    return availableGroups.get() > 0;
  }

  public String toString(){
    StringBuilder sb = new StringBuilder();
    int unavailable = 0;
    int available = 0;
    for (SharedResourceGroup g : groups.values()) {
      if (g.isAvailable()) {
        available++;
      }else{
        sb.append(g.toString());
        if(unavailable != 0){
          sb.append(", ");
        }
        unavailable++;
      }

    }

    return String.format("Available: %s, Unavailable: %d/%d. [%s]", unavailable == 0, unavailable, unavailable + available, sb.toString());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final SharedResourceManager manager;

    private Builder() {
      this.manager = new SharedResourceManager();
    }

    public Builder addGroup(String groupName) {
      manager.createGroup(groupName);
      return this;
    }

    public SharedResourceManager build() {
      return manager;
    }
  }
}
