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
package com.dremio.plugins.elastic;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.EncryptionValidationMode;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.store.StoragePlugin;

import io.protostuff.Tag;

/**
 * Base configuration for the Elasticseach storage plugin.
 */
public abstract class BaseElasticStoragePluginConfig<T extends ConnectionConf<T, P>, P extends StoragePlugin> extends ConnectionConf<T, P> {

  //  optional reserved for upgrade of regular Elasticsearch
  //  optional reserved for upgrade of regular Elasticsearch
  //  optional reserved for upgrade of regular Elasticsearch
  //  optional reserved for upgrade of regular Elasticsearch
  //  optional bool scriptsEnabled = 5 [default = true];
  //  optional bool showHiddenIndices = 6 [default = false];
  //  optional reserved for upgrade of regular Elasticsearch
  //  optional bool showIdColumn = 8 [default = false];
  //  optional int32 readTimeoutMillis = 9;
  //  optional int32 scrollTimeoutMillis = 10;
  //  optional bool usePainless = 11 [default = true];
  //  optional reserved for upgrade of regular Elasticsearch
  //  optional int32 scrollSize = 13 [default = 4000];
  //  optional bool allowPushdownOnNormalizedOrAnalyzedFields = 14 [default = false];
  //  optional bool warnOnRowCountMismatch = 15 [default = false];
  //  optional EncryptionVerificationMode sslMode = 16;

  @Tag(5)
  @DisplayMetadata(label = "Use scripts for query pushdown")
  public boolean scriptsEnabled = true;

  @Tag(6)
  @DisplayMetadata(label = "Show hidden indices that start with a dot (.)")
  public boolean showHiddenIndices = false;

  @Tag(8)
  @DisplayMetadata(label = "Show _id columns")
  public boolean showIdColumn = false;

  @Min(1)
  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Read timeout (milliseconds)")
  public int readTimeoutMillis = 60000;

  @Min(1)
  @Tag(10)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Scroll timeout (milliseconds)")
  public int scrollTimeoutMillis = 300000;

  @Tag(11)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Use Painless scripting with Elasticsearch 5.0+")
  public boolean usePainless = true;

  @Tag(13)
  @Min(127)
  @Max(65535)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Scroll size")
  public int scrollSize = 4000;

  @Tag(14)
  @DisplayMetadata(label = "Use index/doc fields when pushing down aggregates and filters on analyzed and normalized fields (may produce unexpected results)")
  public boolean allowPushdownOnNormalizedOrAnalyzedFields = false;

  @Tag(15)
  @NotMetadataImpacting
  @DisplayMetadata(label = "If the number of records returned from Elasticsearch is less than the expected number, warn instead of failing the query")
  public boolean warnOnRowCountMismatch = false;

  @Tag(16)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Validation Mode") // Should be under Encryption section
  public EncryptionValidationMode encryptionValidationMode = EncryptionValidationMode.CERTIFICATE_AND_HOSTNAME_VALIDATION;

  @Tag(19)
  @DisplayMetadata(label = "Force Double Precision")
  public boolean forceDoublePrecision = false;

  public BaseElasticStoragePluginConfig() {
  }

  public BaseElasticStoragePluginConfig(
      boolean scriptsEnabled,
      boolean showHiddenIndices,
      boolean showIdColumn,
      int readTimeoutMillis,
      int scrollTimeoutMillis,
      boolean usePainless,
      int scrollSize,
      boolean allowPushdownOnNormalizedOrAnalyzedFields,
      boolean warnOnRowCountMismatch,
      EncryptionValidationMode encryptionValidationMode,
      boolean forceDoublePrecision) {
    this.scriptsEnabled = scriptsEnabled;
    this.showHiddenIndices = showHiddenIndices;
    this.showIdColumn = showIdColumn;
    this.readTimeoutMillis = readTimeoutMillis;
    this.scrollTimeoutMillis = scrollTimeoutMillis;
    this.usePainless = usePainless;
    this.scrollSize = scrollSize;
    this.allowPushdownOnNormalizedOrAnalyzedFields = allowPushdownOnNormalizedOrAnalyzedFields;
    this.warnOnRowCountMismatch = warnOnRowCountMismatch;
    this.encryptionValidationMode = encryptionValidationMode;
    this.forceDoublePrecision = forceDoublePrecision;
  }
}
