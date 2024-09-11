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

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.service.namespace.NamespaceService;
import java.util.concurrent.TimeUnit;

/** Options related to catalog settings. */
@Options
public final class CatalogOptions {

  // Maximum time to wait when creating a storage plugin before failing.
  public static final LongValidator STORAGE_PLUGIN_CREATE_MAX =
      new PositiveLongValidator(
          "store.plugin.wait_millis", TimeUnit.HOURS.toMillis(4), TimeUnit.SECONDS.toMillis(120));

  // Maximum time to wait when starting/creating storage plugin during startup before failing.
  public static final LongValidator STARTUP_WAIT_MAX =
      new PositiveLongValidator(
          "store.start.wait_millis", TimeUnit.HOURS.toMillis(4), TimeUnit.SECONDS.toMillis(120));

  // When metadata impacting configuration parameter is changed, if old metadata should be kept
  public static final BooleanValidator STORAGE_PLUGIN_KEEP_METADATA_ON_REPLACE =
      new BooleanValidator("store.plugin.keep_metadata_on_replace", false);

  // Maximum number of leaf columns allowed for metadata
  public static final LongValidator METADATA_LEAF_COLUMN_MAX =
      new PositiveLongValidator("store.plugin.max_metadata_leaf_columns", Integer.MAX_VALUE, 6400);
  public static final LongValidator METADATA_LEAF_COLUMN_SCANNED_MAX =
      new PositiveLongValidator("store.plugin.max_leaf_columns_scanned", Integer.MAX_VALUE, 800);

  // Maximum nested levels allowed for a column
  public static final LongValidator MAX_NESTED_LEVELS =
      new PositiveLongValidator("store.plugin.max_nested_levels", 64, 16);

  // Timeout in seconds for plugin.getState() call.
  public static final TypeValidators.LongValidator GET_STATE_TIMEOUT_SECONDS =
      new TypeValidators.LongValidator("store.plugin.get_state_timeout_seconds", 10);
  public static final TypeValidators.BooleanValidator ENABLE_ASYNC_GET_STATE =
      new TypeValidators.BooleanValidator("store.plugin.enable_async_get_state", true);

  // How long catalog service waits when communicating to other coordinator(s). This
  // must be greater than the getState timeout as otherwise the interrupted wait on one
  // coordinator may leave the other coordinator in a bad state.
  public static final TypeValidators.LongValidator CHANGE_COMMUNICATION_WAIT_SECONDS =
      new TypeValidators.LongValidator("catalog.change_communication_wait_seconds", 30);

  // ORC ACID table factor for leaf columns
  public static final LongValidator ORC_DELTA_LEAF_COLUMN_FACTOR =
      new PositiveLongValidator("store.hive.orc_delta_leaf_column_factor", Integer.MAX_VALUE, 5);

  // Maximum number of single split partitions allowed to be saved together
  public static final LongValidator SINGLE_SPLIT_PARTITION_MAX =
      new PositiveLongValidator("store.plugin.max_single_split_partitions", Long.MAX_VALUE, 500);

  // How should (multi-)splits be compressed in the K/V store
  public static final TypeValidators.EnumValidator<NamespaceService.SplitCompression>
      SPLIT_COMPRESSION_TYPE =
          new TypeValidators.EnumValidator<>(
              "store.plugin.split_compression",
              NamespaceService.SplitCompression.class,
              NamespaceService.SplitCompression.SNAPPY);
  // Disable cross source select
  public static final BooleanValidator DISABLE_CROSS_SOURCE_SELECT =
      new BooleanValidator("planner.cross_source_select.disable", false);
  // Disable inline refresh
  public static final BooleanValidator SHOW_METADATA_VALIDITY_CHECKBOX =
      new BooleanValidator("store.plugin.show_metadata_validity_checkbox", false);
  // Enable reflection tab in any versioned source dialogs
  public static final BooleanValidator REFLECTION_VERSIONED_SOURCE_ENABLED =
      new BooleanValidator("reflection.arctic.enabled", true);

  // Enable Wiki and Label support in default branch for Versioned sources.
  public static final BooleanValidator WIKILABEL_ENABLED_FOR_VERSIONED_SOURCE_DEFAULT_BRANCH =
      new TypeValidators.BooleanValidator("arctic.wikilabel.defaultbranch.enabled", true);
  // User-Defined Functions for versioned sources
  public static final BooleanValidator VERSIONED_SOURCE_UDF_ENABLED =
      new BooleanValidator("versioned.source.udf.enabled", false);
  // View delegation for a versioned source
  public static final BooleanValidator VERSIONED_SOURCE_VIEW_DELEGATION_ENABLED =
      new BooleanValidator("versioned.source.view_delegation.enabled", false);

  public static final BooleanValidator RETRY_CONNECTION_ON_FAILURE =
      new TypeValidators.BooleanValidator("store.plugin.retry_connection_on_failure", true);

  // Support key to use by test only for doing V0 writes. Used mainly  for testing.
  public static final BooleanValidator V0_ICEBERG_VIEW_WRITES =
      new BooleanValidator("versioned.iceberg.view.write.v0", false);

  public static final BooleanValidator SUPPORT_V1_ICEBERG_VIEWS =
      new BooleanValidator("versioned.iceberg.view_v1.enabled", true);

  // Support key for UDFs API.
  public static final BooleanValidator SUPPORT_UDF_API =
      new BooleanValidator("catalog.udf.api.enabled", true);

  // Do not instantiate
  private CatalogOptions() {}
}
