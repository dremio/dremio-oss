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
package com.dremio.exec.store.hive;

import javax.inject.Provider;

import org.pf4j.ExtensionPoint;
import org.pf4j.PluginManager;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.hive.proxy.HiveProxiedOrcScanFilter;
import com.dremio.exec.store.hive.proxy.HiveProxiedScanBatchCreator;
import com.dremio.exec.store.hive.proxy.HiveProxiedSubScan;

/**
 * PF4J extension for creating storage plugin instances.
 */
public interface StoragePluginCreator extends ExtensionPoint {

  /**
   * A storage plugin that can construct objects from a classloader
   * isolated by PF4J.
   */
  interface PF4JStoragePlugin extends StoragePlugin {
    /**
     * Gets the class definition for the HiveProxiedSubScan implementation that is used
     * with this StoragePlugin.
     */
    Class<? extends HiveProxiedSubScan> getSubScanClass();

    /**
     * Creates the plugin-specific scan batch creator.
     */
    HiveProxiedScanBatchCreator createScanBatchCreator();

    /**
     * Gets the class definition for the HiveProxiedOrcScanFilter implementation that is used
     * with this StoragePlugin.
     */
    Class<? extends HiveProxiedOrcScanFilter> getOrcScanFilterClass();
  }

  PF4JStoragePlugin createStoragePlugin(PluginManager pf4jManager, HiveStoragePluginConfig config,
                                    SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider);
}
