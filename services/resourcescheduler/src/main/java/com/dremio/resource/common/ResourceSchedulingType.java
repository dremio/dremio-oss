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
package com.dremio.resource.common;

/** Properties to decide on Resource Allocations */
public enum ResourceSchedulingType {
  USER("User"),
  USER_INFO("User Info"),
  QUERY_COST("Query Cost"),
  CLIENT_TYPE("Client"),
  WORKLOAD_TYPE("Workload"),
  QUEUE_NAME("Queue Name"),
  TAG("Tag"),
  RESOURCE_DATA("");

  private final String displayName;

  ResourceSchedulingType(String displayName) {
    this.displayName = displayName;
  }

  public String getDisplayName() {
    return displayName;
  }
}
