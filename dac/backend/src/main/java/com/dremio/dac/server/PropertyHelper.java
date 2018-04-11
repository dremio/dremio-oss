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
package com.dremio.dac.server;

import javax.ws.rs.core.Configuration;

/**
 * Helper class to use with {@code javax.ws.rs.core.Configuration#getProperty(String)}
 */
public  final class PropertyHelper {

  private PropertyHelper() {
  }

  /**
   * Get the property value as boolean
   *
   * @param configuration the web configuration
   * @param name the property name
   * @return
   */
  public static Boolean getProperty(Configuration configuration, String name) {
    Object property = configuration.getProperty(name);

    if (property == null) {
      return null;
    }

    if (property instanceof Boolean && (boolean) property) {
      return (boolean) property;
    }

    return Boolean.parseBoolean(property.toString());
  }

}
