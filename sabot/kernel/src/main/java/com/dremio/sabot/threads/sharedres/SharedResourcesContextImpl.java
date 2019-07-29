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

import com.dremio.sabot.exec.context.SharedResourcesContext;

public class SharedResourcesContextImpl implements SharedResourcesContext {

  private final SharedResourceManager manager;

  public SharedResourcesContextImpl(SharedResourceManager manager) {
    super();
    this.manager = manager;
  }

  @Override
  public boolean isRunnable() {
    return manager.isAvailable();
  }

  @Override
  public SharedResourceType getFirstBlockedResource(String groupName) {
    return manager.getFirstBlockedResource(groupName);
  }
}
