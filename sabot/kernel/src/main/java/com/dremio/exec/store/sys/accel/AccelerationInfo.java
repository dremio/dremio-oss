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
package com.dremio.exec.store.sys.accel;

public class AccelerationInfo {
  public final String acceleration_id;
  public final String table;
  public final String state;
  public final Integer raw_layouts;
  public final Boolean raw_enabled;
  public final Integer aggregate_layouts;
  public final Boolean aggregate_enabled;

  public AccelerationInfo(String accelerationId, String table, String state, Integer raw_layouts, Boolean rawEnabled, Integer aggregate_layouts, Boolean aggregateEnabled) {
    super();
    this.acceleration_id = accelerationId;
    this.table = table;
    this.state = state;
    this.raw_layouts = raw_layouts;
    this.raw_enabled = rawEnabled;
    this.aggregate_layouts = aggregate_layouts;
    this.aggregate_enabled = aggregateEnabled;
  }


}
