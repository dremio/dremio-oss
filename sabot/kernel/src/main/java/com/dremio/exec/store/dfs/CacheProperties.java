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

import com.dremio.options.OptionManager;

/** Interface for plugin implementations to communicate cache properties to underlying system. */
public interface CacheProperties {
  /**
   * Indicates that the plugin requests caching whenever possible. (Note that even if plugin
   * requests caching, underlying Dremio system must support caching and the source must have async
   * reads enabled.)
   *
   * @return {@code true} if caching is requested.
   */
  default boolean isCachingEnabled(final OptionManager optionManager) {
    return false;
  }

  /**
   * If caching is enabled and this feature is supported by underlying Dremio system, this controls
   * the max amount of disk space that can be used to cache data for this source.
   *
   * @return {@code percentage} of max disk space to be used for this source, default is 100%.
   */
  default int cacheMaxSpaceLimitPct() {
    return 100;
  }
}
