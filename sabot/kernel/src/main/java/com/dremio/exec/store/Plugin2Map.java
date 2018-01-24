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
package com.dremio.exec.store;

import java.util.Map;

import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * A typed map that holds plugins case-insensitively. Thread safe.
 */
public class Plugin2Map {

  private final Map<String, StoragePlugin> map = Maps.newConcurrentMap();

  public StoragePlugin get(NamespaceKey key) {
    Preconditions.checkNotNull(key);
    return map.get(key.getRoot().toLowerCase());
  }

  public StoragePlugin get(String pluginName) {
    Preconditions.checkNotNull(pluginName);
    return map.get(pluginName.toLowerCase());
  }

  public void put(NamespaceKey key, StoragePlugin plugin2) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(plugin2);
    map.put(key.getRoot().toLowerCase(), plugin2);
  }

  public void remove(NamespaceKey key) {
    Preconditions.checkNotNull(key);
    map.remove(key.getRoot().toLowerCase());
  }
}
