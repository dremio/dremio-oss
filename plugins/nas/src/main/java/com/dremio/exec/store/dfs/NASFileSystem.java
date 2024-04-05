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
package com.dremio.exec.store.dfs;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Provider;

/** File system plugin for NAS source */
public class NASFileSystem extends FileSystemPlugin<NASConf> {
  public NASFileSystem(
      NASConf config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  @Override
  protected List<Property> getProperties() {
    List<Property> properties = new ArrayList<>();
    NASConf conf = getConfig();
    if (conf.getProperties() != null) {
      properties.addAll(conf.getProperties());
    }
    return properties;
  }
}
