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
package com.dremio.exec.maestro;

import com.dremio.resource.common.RoutingQueueManager;

/**
 * Default version of routing queue manager.
 * Provides no functionality.
 */
public class DefaultRoutingQueueManager implements RoutingQueueManager {

  public static final DefaultRoutingQueueManager INSTANCE = new DefaultRoutingQueueManager();

  @Override
  public boolean checkQueueExists(String queueName) {
    return false;
  }

  @Override
  public String getQueueIdByName(String queueName) {
    return null;
  }

  @Override
  public String getQueueNameById(String queueId) {
    return null;
  }
}
