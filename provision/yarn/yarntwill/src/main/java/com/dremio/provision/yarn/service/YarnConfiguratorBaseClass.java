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
package com.dremio.provision.yarn.service;

import java.util.List;
import java.util.Map;

import com.dremio.provision.Property;
import com.google.common.collect.Maps;

/**
 * To implement common methods of YarnConfigurator
 */
public abstract class YarnConfiguratorBaseClass implements YarnConfigurator {

  public YarnConfiguratorBaseClass() {

  }

  @Override
  public void mergeProperties(List<Property> originalProperties) {
    Map<String, String> defaults = getAllToShowDefaults();
    Map<String, Boolean> additionalPropsMap = Maps.newHashMap();
    for (String defaultName : defaults.keySet()) {
      additionalPropsMap.put(defaultName, false);
    }
    for (Property prop : originalProperties) {
      additionalPropsMap.put(prop.getKey(), true);
    }
    // only to show those props on UI/API
    for (Map.Entry<String, Boolean> entry : additionalPropsMap.entrySet()) {
      if (!entry.getValue()) {
        originalProperties.add(new Property(entry.getKey(), defaults.get(entry.getKey())));
      }
    }
  }
}
