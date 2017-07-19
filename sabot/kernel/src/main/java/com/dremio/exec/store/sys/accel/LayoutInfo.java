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

public class LayoutInfo {

  public String acceleration_id;
  public String layout_id;
  public String type;
  public String display;
  public String dimensions;
  public String measures;
  public String partitions;
  public String distributions;
  public String sorts;

  public LayoutInfo(String accelerationId, String layoutId, String type, String display, String dimensions,
      String measures, String partition, String distribution, String sort) {
    super();
    this.acceleration_id = accelerationId;
    this.layout_id = layoutId;
    this.type = type;
    this.display = display;
    this.dimensions = dimensions;
    this.measures = measures;
    this.partitions = partition;
    this.distributions = distribution;
    this.sorts = sort;
  }


}
