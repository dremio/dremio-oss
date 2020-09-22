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
package com.dremio.exec.catalog;

import java.util.concurrent.TimeUnit;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.service.namespace.NamespaceService;

/**
 * Options related to catalog settings.
 */
@Options
public final class CatalogOptions {

  // Maximum time to wait when creating a storage plugin before failing.
  public static final LongValidator STORAGE_PLUGIN_CREATE_MAX = new PositiveLongValidator("store.plugin.wait_millis", TimeUnit.HOURS.toMillis(4), TimeUnit.SECONDS.toMillis(120));

  // Maximum time to wait when creating a storage plugin before failing.
  public static final LongValidator STARTUP_WAIT_MAX = new PositiveLongValidator("store.start.wait_millis", TimeUnit.HOURS.toMillis(4), TimeUnit.SECONDS.toMillis(15));

  // When metadata impacting configuration parameter is changed, if old metadata should be kept
  public static final BooleanValidator STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE =
      new BooleanValidator("store.plugin.keep_metadata_on_replace", false);

  // Maximum number of leaf columns allowed for metadata
  public static final LongValidator METADATA_LEAF_COLUMN_MAX = new PositiveLongValidator("store.plugin.max_metadata_leaf_columns", Integer.MAX_VALUE, 800);

  // Maximum nested levels allowed for a column
  public static final LongValidator MAX_NESTED_LEVELS = new PositiveLongValidator("store.plugin.max_nested_levels", 64, 16);

  // ORC ACID table factor for leaf columns
  public static final LongValidator ORC_DELTA_LEAF_COLUMN_FACTOR = new PositiveLongValidator("store.hive.orc_delta_leaf_column_factor", Integer.MAX_VALUE, 5);

  // Maximum number of single split partitions allowed to be saved together
  public static final LongValidator SINGLE_SPLIT_PARTITION_MAX = new PositiveLongValidator("store.plugin.max_single_split_partitions", Long.MAX_VALUE, 500);

  // How should (multi-)splits be compressed in the K/V store
  public static final TypeValidators.EnumValidator<NamespaceService.SplitCompression> SPLIT_COMPRESSION_TYPE = new TypeValidators.EnumValidator<>(
    "store.plugin.split_compression", NamespaceService.SplitCompression.class, NamespaceService.SplitCompression.SNAPPY);
  // Disable cross source select
  public static final BooleanValidator DISABLE_CROSS_SOURCE_SELECT = new BooleanValidator("planner.cross_source_select.disable", false);

  // Do not instantiate
  private CatalogOptions() {
  }
}
