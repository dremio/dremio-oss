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

/**
 * Wrapper over SharedResource. supports a resource that can be activated, but not deactivated
 */
public class ActivableResource {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ActivableResource.class);

  // local variable to avoid locking/synchronization on every call.
  private volatile boolean activated = false;
  private final SharedResource resource;

  public ActivableResource(SharedResource resource) {
    this.resource = resource;
    this.resource.markBlocked();
  }

  // idempotent operation.
  public void activate(String trigger) {
    if (!activated) {
      synchronized (this) {
        logger.debug("resource {} activated by trigger {}", resource.getName(), trigger);
        resource.markAvailable();
        activated = true;
      }
    }
  }

  public boolean isActivated() {
    return activated;
  }
}
