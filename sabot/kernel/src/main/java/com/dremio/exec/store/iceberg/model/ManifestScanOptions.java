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
package com.dremio.exec.store.iceberg.model;

import org.apache.iceberg.ManifestContent;
import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(builder = ImmutableManifestScanOptions.Builder.class)
@Value.Immutable
public interface ManifestScanOptions {

  /**
   * ManifestContent describes type of files the manifest handles.
   *
   * @return
   */
  @Value.Default
  default ManifestContent getManifestContent() {
    return ManifestContent.DATA;
  }

  /**
   * SplitGen generates splits for data scan. If turned OFF, the scan will generate abstract output such as path, size etc
   * for the entries in the manifest.
   *
   * @return
   */
  @Value.Default
  default boolean includesSplitGen() {
    return false;
  }

  /**
   * Includes the serialized DataFile object as IcebergMetadata in the output schema.
   * Applicable only when splitgen is turned OFF.
   *
   * @return
   */
  @Value.Default
  default boolean includesIcebergMetadata() {
    return false;
  }

  /**
   * Returns an instance with all defaults
   *
   * @return
   */
  static ManifestScanOptions withDefaults() {
    return new ImmutableManifestScanOptions.Builder().build();
  }
}
