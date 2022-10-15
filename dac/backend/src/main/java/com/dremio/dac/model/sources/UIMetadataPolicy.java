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
package com.dremio.dac.model.sources;

import static com.dremio.exec.store.CatalogService.DEFAULT_AUTHTTLS_MILLIS;
import static com.dremio.exec.store.CatalogService.DEFAULT_EXPIRE_MILLIS;
import static com.dremio.exec.store.CatalogService.DEFAULT_REFRESH_MILLIS;

import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class that exposes metadata refresh policy information to REST APIs.
 */
public class UIMetadataPolicy {

  public static final UIMetadataPolicy DEFAULT_UIMETADATA_POLICY = UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY);

  private final UIUpdateMode updateMode;
  private final long namesRefreshMillis;
  private final long authTTLMillis;
  private final long datasetDefinitionRefreshAfterMillis;
  private final long datasetDefinitionExpireAfterMillis;

  private final boolean deleteUnavailableDatasets;
  private final boolean autoPromoteDatasets;

  /**
   * Enumeration describing the types of updates available.
   */
  enum UIUpdateMode {
    PREFETCH,
    PREFETCH_QUERIED,
    INLINE;

    public static UIUpdateMode of(UpdateMode mode){
      switch(mode){
      case PREFETCH:
        return PREFETCH;
      case INLINE: // Note: DX-16127: INLINE is obsolete, and should not be produced by the backend code
      case PREFETCH_QUERIED:
      case UNKNOWN:
      default:
        return PREFETCH_QUERIED;
      }
    }

    public UpdateMode asUpdateMode(){
      switch(this){
      case INLINE:
        // Note: DX-16127: INLINE is obsolete, and is replaced with PREFETCH_QUERIED
        return UpdateMode.PREFETCH_QUERIED;
      case PREFETCH:
        return UpdateMode.PREFETCH;
      case PREFETCH_QUERIED:
      default:
        return UpdateMode.PREFETCH_QUERIED;
      }
    }
  }

  @JsonCreator
  public UIMetadataPolicy(
      @JsonProperty("updateMode") UIUpdateMode updateMode,
      @JsonProperty("namesRefreshMillis") long namesRefreshMillis,
      @JsonProperty("authTTLMillis") long authTTLMillis,
      @JsonProperty("datasetDefinitionRefreshAfterMillis") long datasetDefinitionRefreshAfterMillis,
      @JsonProperty("datasetDefinitionExpireAfterMillis") long datasetDefinitionExpireAfterMillis,
      @JsonProperty("deleteUnavailableDatasets") Boolean deleteUnavailableDatasets,
      @JsonProperty("autoPromoteDatasets") Boolean autoPromoteDatasets) {
    super();
    this.updateMode = updateMode;
    if (namesRefreshMillis == 0) {
      this.namesRefreshMillis = DEFAULT_REFRESH_MILLIS;
    } else {
      this.namesRefreshMillis = namesRefreshMillis;
    }

    if (authTTLMillis == 0) {
     this.authTTLMillis = DEFAULT_AUTHTTLS_MILLIS;
    } else {
      this.authTTLMillis = authTTLMillis;
    }

    if (datasetDefinitionRefreshAfterMillis == 0) {
      this.datasetDefinitionRefreshAfterMillis = DEFAULT_REFRESH_MILLIS;
    } else {
      this.datasetDefinitionRefreshAfterMillis = datasetDefinitionRefreshAfterMillis;
    }

    if (datasetDefinitionExpireAfterMillis == 0) {
      this.datasetDefinitionExpireAfterMillis = DEFAULT_EXPIRE_MILLIS;
    } else {
      this.datasetDefinitionExpireAfterMillis = datasetDefinitionExpireAfterMillis;
    }

    this.deleteUnavailableDatasets = !Boolean.FALSE.equals(deleteUnavailableDatasets); // true, unless 'false'
    this.autoPromoteDatasets = Boolean.TRUE.equals(autoPromoteDatasets); // false, unless 'true'
  }

  public static UIMetadataPolicy getDefaultUimetadataPolicy() {
    return DEFAULT_UIMETADATA_POLICY;
  }

  public UIUpdateMode getUpdateMode() {
    return updateMode;
  }

  public long getNamesRefreshMillis() {
    return namesRefreshMillis;
  }

  public long getAuthTTLMillis() {
    return authTTLMillis;
  }

  public long getDatasetDefinitionRefreshAfterMillis() {
    return datasetDefinitionRefreshAfterMillis;
  }

  public long getDatasetDefinitionExpireAfterMillis() {
    return datasetDefinitionExpireAfterMillis;
  }

  public boolean getDeleteUnavailableDatasets() { return deleteUnavailableDatasets; }

  public boolean getAutoPromoteDatasets() { return autoPromoteDatasets; }

  public static UIMetadataPolicy of(MetadataPolicy policy){
    if(policy == null){
      return DEFAULT_UIMETADATA_POLICY;
    }
    return new UIMetadataPolicy(
        UIUpdateMode.of(policy.getDatasetUpdateMode()),
        policy.getNamesRefreshMs(),
        policy.getAuthTtlMs(),
        policy.getDatasetDefinitionRefreshAfterMs(),
        policy.getDatasetDefinitionExpireAfterMs(),
        policy.getDeleteUnavailableDatasets(),
        policy.getAutoPromoteDatasets()
    );
  }

  public MetadataPolicy asMetadataPolicy() {
    return new MetadataPolicy()
        .setDatasetUpdateMode(updateMode.asUpdateMode())
        .setNamesRefreshMs(namesRefreshMillis)
        .setAuthTtlMs(authTTLMillis)
        .setDeleteUnavailableDatasets(deleteUnavailableDatasets)
        .setDatasetDefinitionRefreshAfterMs(datasetDefinitionRefreshAfterMillis)
        .setDatasetDefinitionExpireAfterMs(datasetDefinitionExpireAfterMillis)
        .setAutoPromoteDatasets(autoPromoteDatasets);
  }
}
