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
package com.dremio.common.logical;

import java.util.Set;

import com.dremio.common.scanner.persistence.ScanResult;


public abstract class FormatPluginConfigBase implements FormatPluginConfig{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatPluginConfigBase.class);


  /**
   * scan for implementations of {@see FormatPlugin}.
   *
   * @param classpathScan - classpath scan result
   * @return - list of classes that implement the interface.
   */
  public static Set<Class<? extends FormatPluginConfig>> getSubTypes(final ScanResult classpathScan) {
    final Set<Class<? extends FormatPluginConfig>> pluginClasses = classpathScan.getImplementations(FormatPluginConfig.class);
    if (logger.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Found ");
      sb.append(pluginClasses.size());
      sb.append(" format plugin configuration classes:\n");
      for (Class<?> c : pluginClasses) {
        sb.append('\t');
        sb.append(c.getName());
        sb.append('\n');
      }
      logger.debug(sb.toString());
    }
    return pluginClasses;
  }

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

}
