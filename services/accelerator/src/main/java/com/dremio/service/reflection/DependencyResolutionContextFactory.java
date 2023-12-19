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
package com.dremio.service.reflection;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.reflection.proto.RefreshRequest;
import com.dremio.service.reflection.store.RefreshRequestsStore;
import com.google.common.base.Preconditions;

/**
 * DependencyResolutionContextFactory creates either a caching or non-caching context
 * used to reduce KVstore queries when performing a ReflectionManager.sync.  In particular,
 * the handleEntries method can issue up to 7-8K KV store queries for 300 reflections.  By caching,
 * we can bring this down by a factor of 10.
 *
 */
public class DependencyResolutionContextFactory {

  private final RefreshRequestsStore requestsStore;
  private final ReflectionSettings reflectionSettings;
  private final OptionManager optionManager;

  // Reflection settings by dataset.  These rarely change and so we can mostly reuse them between syncs.
  private Map<CatalogEntityKey, AccelerationSettings> settingsMap;
  private int lastSettingsHash;
  DependencyResolutionContextFactory(ReflectionSettings reflectionSettings, RefreshRequestsStore requestsStore,
                                     OptionManager optionManager) {
    this.requestsStore = Preconditions.checkNotNull(requestsStore, "refresh requests store required");
    this.reflectionSettings = Preconditions.checkNotNull(reflectionSettings, "reflection settings required");
    this.optionManager = Preconditions.checkNotNull(optionManager, "optionManager required");
  }

  DependencyResolutionContext create() {

    if (!optionManager.getOption(ReflectionOptions.REFLECTION_MANAGER_SYNC_CACHE)) {
      settingsMap = null; // Release cache to GC in case option was changed
      lastSettingsHash = 0;
      return new DependencyResolutionContextNoCache();
    }

    // Reflection settings are stored on sources and only set on datasets when they are actually overridden.
    int currentHash = reflectionSettings.getAllHash();
    boolean hasAccelerationSettingsChanged = false;
    if (settingsMap == null || currentHash != lastSettingsHash) {
      // If hash over all reflection settings changes, then we need to re-build the
      // namespace to resolved reflection settings cache
      settingsMap = new HashMap<>();
      lastSettingsHash = currentHash;
      hasAccelerationSettingsChanged = true;
    }
    return new DependencyResolutionContextCached(hasAccelerationSettingsChanged);
  }

  private final class DependencyResolutionContextNoCache implements DependencyResolutionContext {
    @Override
    public AccelerationSettings getReflectionSettings(CatalogEntityKey key) {
      return reflectionSettings.getReflectionSettings(key);
    }
    @Override
    public RefreshRequest getRefreshRequest(String datasetId) {
      return requestsStore.get(datasetId);
    }

    @Override
    public boolean hasAccelerationSettingsChanged() {
      return true;
    }

    @Override
    public void close() { }
  }

  private class DependencyResolutionContextCached implements DependencyResolutionContext {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DependencyResolutionContextCached.class);

    private Map<String, Optional<RefreshRequest>> requestMap;
    private boolean hasAccelerationSettingsChanged;

    private long settingsCacheRequests;
    private long settingsCacheMisses;
    private long requestCacheRequests;
    private long requestCacheMisses;
    public DependencyResolutionContextCached(final boolean hasAccelerationSettingsChanged) {
      requestMap = new HashMap<>();
      this.hasAccelerationSettingsChanged = hasAccelerationSettingsChanged;
      settingsCacheRequests = 0;
      settingsCacheMisses = 0;
      requestCacheRequests = 0;
      requestCacheMisses = 0;
    }
    @Override
    public AccelerationSettings getReflectionSettings(CatalogEntityKey key) {
      settingsCacheRequests++;
      return settingsMap.computeIfAbsent(key, k -> {
        settingsCacheMisses++;
        return reflectionSettings.getReflectionSettings(k);
      } );
    }
    @Override
    public RefreshRequest getRefreshRequest(String datasetId) {
      requestCacheRequests++;
      Optional<RefreshRequest> request = requestMap.get(datasetId);
      if (request == null) {
        requestCacheMisses++;
        request = Optional.ofNullable(requestsStore.get(datasetId));
        requestMap.put(datasetId, request);
      }
      return request.orElse(null);
    }

    @Override
    public boolean hasAccelerationSettingsChanged() {
      return hasAccelerationSettingsChanged;
    }

    @Override
    public void close() {
      logger.debug("Completed sync on handleEntries. Cache stats: accelerationSettingsHash={}" +
          " datasetReflectionSettings:size={},requests={},misses={} datasetRefreshRequests:size={},requests={},misses={}",
        lastSettingsHash,
        settingsMap.size(), settingsCacheRequests, settingsCacheMisses,
        requestMap.size(), requestCacheRequests, requestCacheMisses);
      requestMap = null;
    }
  }
}
