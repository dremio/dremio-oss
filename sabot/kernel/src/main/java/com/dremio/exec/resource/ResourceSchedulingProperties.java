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
package com.dremio.exec.resource;

import java.util.Map;


import com.google.common.collect.Maps;

/**
 * To deal with Scheduling related properties
 */
public class ResourceSchedulingProperties {

  public enum SchedulingPropsEnum {
    USER,
    GROUP,
    QUERY_COST,
    CLIENT_TYPE,
    WORKLOAD_TYPE,
    TAG,
    QUEUE_NAME
  }

  private final Map<SchedulingPropsEnum, String> propValues = Maps.newHashMap();

  public ResourceSchedulingProperties(Map<SchedulingPropsEnum, String> propValues) {
    this.propValues.putAll(propValues);
  }

  public String getProperty(Enum propEnum) {
    return propValues.get(propEnum);
  }

 }
