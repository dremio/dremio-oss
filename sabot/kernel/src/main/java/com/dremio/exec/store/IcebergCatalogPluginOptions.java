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
package com.dremio.exec.store;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

@Options
public final class IcebergCatalogPluginOptions {

  public static final TypeValidators.BooleanValidator RESTCATALOG_PLUGIN_ENABLED =
      new TypeValidators.BooleanValidator("plugins.restcatalog.enabled", false);

  public static final TypeValidators.BooleanValidator RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED =
      new TypeValidators.BooleanValidator("plugins.restcatalog.table_cache.enabled", true);

  public static final TypeValidators.RegexStringValidator RESTCATALOG_ALLOWED_NS_SEPARATOR =
      new TypeValidators.RegexStringValidator("plugins.restcatalog.allowed.ns.separator", "\\.");

  public static final TypeValidators.PositiveLongValidator
      RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS =
          new TypeValidators.PositiveLongValidator(
              "plugins.restcatalog.table_cache.size_items", 1_000_000_000, 10_000);

  public static final TypeValidators.PositiveLongValidator
      RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS =
          new TypeValidators.PositiveLongValidator(
              "plugins.restcatalog.table_cache.expire_after_write_seconds", 600 /* 10 mins */, 3);

  public static final TypeValidators.PositiveLongValidator
      RESTCATALOG_PLUGIN_FILE_SYSTEM_EXPIRE_AFTER_WRITE_MINUTES =
          new TypeValidators.PositiveLongValidator(
              "plugins.restcatalog.file_system.expire_after_write_minutes", 60 /* an hour */, 5);

  public static final TypeValidators.PositiveLongValidator
      RESTCATALOG_PLUGIN_CATALOG_EXPIRE_SECONDS =
          new TypeValidators.PositiveLongValidator(
              "plugins.restcatalog.catalog.expire_seconds", 3600 /* an hour */, 1800);

  private IcebergCatalogPluginOptions() {}
}
