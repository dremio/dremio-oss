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
package com.dremio.plugins;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

/** System options for NessieClient. */
@Options
public final class NessieClientOptions {

  public static final TypeValidators.PositiveLongValidator NESSIE_CONTENT_CACHE_SIZE_ITEMS =
      new TypeValidators.PositiveLongValidator(
          "plugins.dataplane.nessie_content_cache.size_items", 1_000_000_000, 10_000);

  public static final TypeValidators.PositiveLongValidator NESSIE_CONTENT_CACHE_TTL_MINUTES =
      new TypeValidators.PositiveLongValidator(
          "plugins.dataplane.nessie_content_cache.ttl_minutes", 1_000_000_000, 60 /* One hour */);

  public static final TypeValidators.BooleanValidator BYPASS_CONTENT_CACHE =
      new TypeValidators.BooleanValidator(
          "plugins.dataplane.nessie_content_cache.bypass_cache", false);

  private NessieClientOptions() {}
}
