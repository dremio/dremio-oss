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
package com.dremio.dac.server;

import java.util.HashMap;
import java.util.Map;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.model.common.DACRuntimeException;
import com.dremio.dac.model.sources.Source;
import com.dremio.service.namespace.source.proto.SourceType;

/**
 * Configure a single source type
 * @param <S> the source type being configured
 */
public abstract class SingleSourceToStoragePluginConfig<S extends Source> extends SourceToStoragePluginConfig {

  private final SourceType type;

  public SingleSourceToStoragePluginConfig(SourceType type) {
    super();
    this.type = type;
  }

  @SuppressWarnings("unchecked")
  @Override
  public final StoragePluginConfig configure(Source source) {
    if (source.getSourceType() != type) {
      throw new BadSourceConfigurationException("Expected " + type + " but got " + source.getSourceType());
    }
    return configureSingle((S)source);
  }

  public abstract StoragePluginConfig configureSingle(S source);

  public static SourceToStoragePluginConfig of(SingleSourceToStoragePluginConfig<?>... configurers) {
    final Map<SourceType, SourceToStoragePluginConfig> map = new HashMap<>();
    for (SingleSourceToStoragePluginConfig<?> configurer : configurers) {
      map.put(configurer.type, configurer);
    }
    return new SourceToStoragePluginConfig() {
      public StoragePluginConfig configure(Source source) {
        SourceToStoragePluginConfig sourceToStoragePluginConfig = map.get(source.getSourceType());
        if (sourceToStoragePluginConfig == null) {
          throw new BadSourceConfigurationException("no configurer for " + source.getSourceType() + ". supported: " + map.keySet());
        }
        return sourceToStoragePluginConfig.configure(source);
      }

      @Override
      public String toString() {
        return "SourceToStoragePluginConfig{ " + map + " }";
      }
    };
  }

  /**
   * Thrown when the sources passed are invalid
   */
  public static class BadSourceConfigurationException extends DACRuntimeException {
    private static final long serialVersionUID = 1L;

    public BadSourceConfigurationException(String message) {
      super(message);
    }

    public BadSourceConfigurationException(Throwable cause) {
      super(cause);
    }
  }

}
