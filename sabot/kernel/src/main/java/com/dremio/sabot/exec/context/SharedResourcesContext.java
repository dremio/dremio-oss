/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.exec.context;

import com.dremio.sabot.threads.sharedres.SharedResourceType;

/**
 * Exposes the state (runnable vs blocked) of shared resources
 */
public interface SharedResourcesContext {

  /**
   * @return true if no shared resource is blocked
   */
  boolean isRunnable();

  /**
   * @return the first shared resource that is blocked, if any, in the specified group, null if the specified
   * group is available.
   *
   * There can be a race between the call to isRunnable() and the call to this function. Used only for stats reporting
   */
  SharedResourceType getFirstBlockedResource(String groupName);
}
