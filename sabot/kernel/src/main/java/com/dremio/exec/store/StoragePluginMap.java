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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.dremio.common.AutoCloseables;
import com.dremio.common.store.StoragePluginConfig;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * Holds maps to storage plugins. Supports name => plugin and config => plugin mappings.
 *
 * This is inspired by ConcurrentMap but provides a secondary key mapping that allows an alternative lookup mechanism.
 * The class is responsible for internally managing consistency between the two maps. This class is threadsafe.
 */
class StoragePluginMap implements Iterable<Entry<String, StoragePlugin>>, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginMap.class);

  private final ConcurrentMap<String, StoragePlugin> nameMap = Maps.newConcurrentMap();

  private final Multimap<StoragePluginConfig, StoragePlugin> configMap =
      Multimaps.synchronizedListMultimap(LinkedListMultimap.<StoragePluginConfig, StoragePlugin>create());

  public void putAll(Map<String, StoragePlugin> mapOfPlugins) {
    for (Entry<String, StoragePlugin> entry : mapOfPlugins.entrySet()) {
      StoragePlugin plugin = entry.getValue();
      nameMap.put(entry.getKey(), plugin);
      // this possibly overwrites items in a map.
      configMap.put(plugin.getId().getConfig(), plugin);
    }
  }

  public boolean replace(String name, StoragePlugin oldPlugin, StoragePlugin newPlugin) {
    boolean ok = nameMap.replace(name, oldPlugin, newPlugin);
    if (ok) {
      configMap.put(newPlugin.getId().getConfig(), newPlugin);
      configMap.remove(oldPlugin.getId().getConfig(), oldPlugin);
    }

    return ok;
  }

  public boolean remove(String name, StoragePlugin oldPlugin) {
    boolean ok = nameMap.remove(name, oldPlugin);
    if (ok) {
      configMap.remove(oldPlugin.getId().getConfig(), oldPlugin);
    }
    return ok;
  }

  public void put(String name, StoragePlugin plugin) {
    StoragePlugin oldPlugin = nameMap.put(name, plugin);
    configMap.put(plugin.getId().getConfig(), plugin);
    if (oldPlugin != null) {
      try {
        oldPlugin.close();
      } catch (Exception e) {
        logger.warn("Failure while closing plugin replaced by injection.", e);
      }
    }
  }

  public StoragePlugin putIfAbsent(String name, StoragePlugin plugin) {
    StoragePlugin oldPlugin = nameMap.putIfAbsent(name, plugin);
    if (oldPlugin == null) {
      configMap.put(plugin.getId().getConfig(), plugin);
    }
    return oldPlugin;
  }

  public StoragePlugin remove(String name) {
    StoragePlugin plugin = nameMap.remove(name);
    if (plugin != null) {
      configMap.remove(plugin.getId().getConfig(), plugin);
    }
    return plugin;
  }

  public StoragePlugin get(String name) {
    return nameMap.get(name);
  }

  @Override
  public Iterator<Entry<String, StoragePlugin>> iterator() {
    return nameMap.entrySet().iterator();
  }

  public Set<String> names() {
    return nameMap.keySet();
  }

  public StoragePlugin get(StoragePluginConfig config) {
    Collection<StoragePlugin> plugins = configMap.get(config);
    if (plugins == null || plugins.isEmpty()) {
      return null;
    } else {
      // return first one since it doesn't matter which plugin we use for ephemeral purposes (since they are all the
      // same, they just have different names.
      return plugins.iterator().next();
    }
  }

  public Iterable<StoragePlugin> plugins() {
    return nameMap.values();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(configMap.values());
  }

}
