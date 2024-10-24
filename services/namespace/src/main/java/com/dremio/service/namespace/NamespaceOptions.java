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
package com.dremio.service.namespace;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

@Options
public final class NamespaceOptions {
  public static final TypeValidators.BooleanValidator DATASET_METADATA_CONSISTENCY_VALIDATE =
      new TypeValidators.BooleanValidator("store.dataset.metadata_consistency.validate", false);
  public static final TypeValidators.BooleanValidator DATASET_METADATA_USE_SMART_EXPIRY =
      new TypeValidators.BooleanValidator("store.dataset.metadata.use_smart_expiry", true);

  /**
   * Support option to control when split orphans should be automatically expired. The policy around
   * split deletion is as follows: 1. Metadata that is older than N hours is deleted - N is defined
   * by this support option 2. The latest copy of the metadata is not deleted
   */
  public static final TypeValidators.RangeLongValidator DATASET_METADATA_AUTO_EXPIRE_AFTER_HOURS =
      new TypeValidators.RangeLongValidator(
          "store.dataset.metadata.auto_expire_splits_after_hours", 1, 12, 3);

  /**
   * Maximum number of dataset versions to keep per dataset. The versions are trimmed in a periodic
   * scan of the dataVersions collection.
   */
  public static final TypeValidators.LongValidator DATASET_VERSIONS_LIMIT =
      new TypeValidators.LongValidator("store.dataset.versions.limit", 50);

  /** How often to run dataset version trimmer in seconds. */
  public static final TypeValidators.RangeLongValidator DATASET_VERSIONS_TRIMMER_SCHEDULE_SECONDS =
      new TypeValidators.RangeLongValidator(
          "store.dataset.versions.trimmer.schedule.seconds", 30, 10 * 24 * 3600, 24 * 3600);

  private NamespaceOptions() {}
}
