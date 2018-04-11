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
package com.dremio.exec.catalog;

import java.util.concurrent.TimeUnit;

import com.dremio.exec.server.options.Options;
import com.dremio.exec.server.options.TypeValidators.BooleanValidator;
import com.dremio.exec.server.options.TypeValidators.LongValidator;
import com.dremio.exec.server.options.TypeValidators.PositiveLongValidator;

/**
 * Options related to catalog settings.
 */
@Options
public interface CatalogOptions {

  // Check for storage plugin status at time of creation of the plugin
  BooleanValidator STORAGE_PLUGIN_CHECK_STATE = new BooleanValidator("store.plugin.check_state", true);

  // Maximum time to wait when creating a storage plugin before failing.
  LongValidator STORAGE_PLUGIN_CREATE_MAX = new PositiveLongValidator("store.plugin.wait_millis", TimeUnit.HOURS.toMillis(4), TimeUnit.SECONDS.toMillis(120));

  // Maximum time to wait when creating a storage plugin before failing.
  LongValidator STARTUP_WAIT_MAX = new PositiveLongValidator("store.start.wait_millis", TimeUnit.HOURS.toMillis(4), TimeUnit.SECONDS.toMillis(15));

  // When metadata impacting configuration parameter is changed, if old metadata should be kept
  BooleanValidator STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE =
      new BooleanValidator("store.plugin.keep_metadata_on_replace", false);
}
