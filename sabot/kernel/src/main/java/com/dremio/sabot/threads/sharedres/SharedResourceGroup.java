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

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks availability of a group of resources. The group is marked available when all its resources are available
 */
public class SharedResourceGroup {
  private SharedResourceManager sharedResourceManager;
  private final String name;
  // maintain a list so we can report what resources are unavailable.
  private final CopyOnWriteArrayList<SharedResource> resources = new CopyOnWriteArrayList<>();
  private final AtomicInteger blockedResources = new AtomicInteger();
  /**
   * enforces ordering between creating and disabling resources: all resources created before {@link #markAllAvailable()}
   * can no longer block
   */
  private final Object disableLock = new Object();

  SharedResourceGroup(SharedResourceManager sharedResourceManager, String name) {
    this.sharedResourceManager = sharedResourceManager;
    this.name = name;
  }

  public SharedResource createResource(String name) {
    synchronized(disableLock) {
      SharedResource resource = new SharedResource(this, name);
      if (resources.isEmpty()) {
        sharedResourceManager.addAvailable();
      }
      resources.add(resource);
      return resource;
    }
  }

  void addBlocked() {
    if (blockedResources.getAndIncrement() == 0) {
      sharedResourceManager.removeAvailable();
    }
  }

  void removeBlocked() {
    if(blockedResources.decrementAndGet() == 0){
      sharedResourceManager.addAvailable();
    }
  }

  public boolean isAvailable(){
    return !resources.isEmpty() && blockedResources.get() == 0;
  }

  /**
   * Disable future blocking on existing shared resources. New resources can still block this.
   */
  public void markAllAvailable() {
    synchronized(disableLock) {
      for(SharedResource r : resources) {
        r.disableBlocking();
      }
    }
  }

  public String toString(){
    StringBuilder sb = new StringBuilder();
    int unavailable = 0;
    int available = 0;
    for(SharedResource r : resources){
      if(r.isAvailable()){
        available++;
      }else{
        sb.append(r.getName());
        if(unavailable != 0){
          sb.append(", ");
        }
        unavailable++;
      }

    }

    return String.format("{Group: %s, Available: %s, blocked: %d, Unavailable: %d/%d. [%s]}", name, unavailable == 0, blockedResources.get(), unavailable, unavailable + available, sb.toString());
  }

}
