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
package com.dremio.service.reflection;

import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.datastore.KVStoreProvider;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.store.ReflectionSettingsStore;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Manages datasets/sources acceleration settings.
 */
public class ReflectionSettings {
  private static final long DEFAULT_REFRESH_PERIOD = TimeUnit.HOURS.toMillis(3);
  private static final long DEFAULT_GRACE_PERIOD = TimeUnit.HOURS.toMillis(9);

  private final Provider<NamespaceService> namespace;
  private final ReflectionSettingsStore store;

  public ReflectionSettings(Provider<NamespaceService> namespace, Provider<KVStoreProvider> storeProvider) {
    this.namespace = Preconditions.checkNotNull(namespace, "namespace service required");
    this.store = new ReflectionSettingsStore(storeProvider);
  }

  // only returns a AccelerationSettings if one is specifically defined for the specified key
  public Optional<AccelerationSettings> getStoredReflectionSettings(NamespaceKey key) {
    return Optional.fromNullable(store.get(key));
  }

  public AccelerationSettings getReflectionSettings(NamespaceKey key) {
    // first check if the settings have been set at the dataset level
    AccelerationSettings settings = store.get(key);
    if (settings != null) {
      return settings;
    }

    // no settings found, try to retrieve the source's settings
    final NamespaceKey rootKey = new NamespaceKey(key.getRoot());
    if (!rootKey.equals(key)) {
      try {
        namespace.get().getSource(new NamespaceKey(key.getRoot()));
        // root parent is a source, return its settings from the store
        return getReflectionSettings(rootKey);
      } catch (NamespaceException e) {
        // root is not a source, fallback and return the default acceleration settings
      }
    }

    // otherwise, return the default settings, they depend if the dataset is a home dataset or not
    boolean homeDataset = false;
    try {
      DatasetConfig config = namespace.get().getDataset(key);
      homeDataset = ReflectionUtils.isHomeDataset(config.getType());
    } catch (NamespaceException e) {
      // no dataset found, probably a source. In all cases it's not a home pds :)
    }

    if (homeDataset) {
      return new AccelerationSettings()
        .setMethod(RefreshMethod.FULL)
        .setNeverRefresh(true)
        .setNeverExpire(true);
    } else {
      return new AccelerationSettings()
        .setMethod(RefreshMethod.FULL)
        .setGracePeriod(DEFAULT_REFRESH_PERIOD)
        .setRefreshPeriod(DEFAULT_GRACE_PERIOD);
    }
  }

  public void setReflectionSettings(NamespaceKey key, AccelerationSettings settings) {
    // if some settings already exist just override them, otherwise remove the version as the passed settings may be
    // coming from the parent source
    AccelerationSettings previous = store.get(key);
    settings.setTag(previous != null ? previous.getTag() : null);
    // version is deprecated but may exist after an upgrade so we need to ensure that it is nulled out when we null out
    // the tag or else the inline upgrade code for OCC will get confused
    settings.setVersion(previous != null ? previous.getVersion() : null);
    if (settings.getRefreshPeriod() == null) {
      settings.setRefreshPeriod(DEFAULT_REFRESH_PERIOD);
    }
    if (settings.getGracePeriod() == null) {
      settings.setGracePeriod(DEFAULT_GRACE_PERIOD);
    }
    store.save(key, settings);
  }

  public void removeSettings(NamespaceKey key) {
    store.delete(key);
  }

}
