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
package com.dremio.dac.api;

import static com.dremio.exec.store.CatalogService.DEFAULT_AUTHTTLS_MILLIS;
import static com.dremio.exec.store.CatalogService.DEFAULT_EXPIRE_MILLIS;
import static com.dremio.exec.store.CatalogService.DEFAULT_REFRESH_MILLIS;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.dremio.service.namespace.source.proto.UpdateMode;

/**
 * Source MetadataPolicy for the public REST API.
 */
public class MetadataPolicy {

  public static final long ONE_MINUTE_IN_MS = 60000L;

  // The minimum for this field is one minute as defined in MetadataRefresh.js
  @Min(ONE_MINUTE_IN_MS)
  private long authTTLMs;

  // The minimum for this field is one minute as defined in MetadataRefresh.js
  @Min(ONE_MINUTE_IN_MS)
  private long namesRefreshMs;

  // The minimum for this field is one minute as defined in MetadataRefresh.js
  @Min(ONE_MINUTE_IN_MS)
  private long datasetRefreshAfterMs;

  // The minimum for this field is one minute as defined in MetadataRefresh.js
  @Min(ONE_MINUTE_IN_MS)
  private long datasetExpireAfterMs;

  private String datasetUpdateMode;

  private boolean deleteUnavailableDatasets;

  private boolean autoPromoteDatasets;

  public MetadataPolicy() { }

  public MetadataPolicy(com.dremio.service.namespace.source.proto.MetadataPolicy policy) {
    setAuthTTLMs(policy.getAuthTtlMs());
    setNamesRefreshMs(policy.getNamesRefreshMs());
    setDatasetRefreshAfterMs(policy.getDatasetDefinitionRefreshAfterMs());
    setDatasetExpireAfterMs(policy.getDatasetDefinitionExpireAfterMs());
    setDatasetUpdateMode(policy.getDatasetUpdateMode().name());
    setDeleteUnavailableDatasets(policy.getDeleteUnavailableDatasets());
    setAutoPromoteDatasets(policy.getAutoPromoteDatasets());
  }

  // Set default values if not provided.
  public void setMetadataPolicyDefaults() {
    setAuthTTLMs(this.authTTLMs);
    setNamesRefreshMs(this.namesRefreshMs);
    setDatasetRefreshAfterMs(this.datasetRefreshAfterMs);
    setDatasetExpireAfterMs(this.datasetExpireAfterMs);
  }

  public Long getAuthTTLMs() {
    return authTTLMs;
  }

  public void setAuthTTLMs(Long authTTLMs) {
    // Set to default if not provided in request.
    if (authTTLMs == 0) {
      this.authTTLMs = DEFAULT_AUTHTTLS_MILLIS;
    } else {
      this.authTTLMs = authTTLMs;
    }
  }

  public long getNamesRefreshMs() {
    return namesRefreshMs;
  }

  public void setNamesRefreshMs(long namesRefreshMs) {
    // Set to default if not provided in request.
    if (namesRefreshMs == 0) {
      this.namesRefreshMs = DEFAULT_REFRESH_MILLIS;
    } else {
      this.namesRefreshMs = namesRefreshMs;
    }
  }

  public long getDatasetRefreshAfterMs() {
    return datasetRefreshAfterMs;
  }

  public void setDatasetRefreshAfterMs(long datasetRefreshAfterMs) {
    // Set to default if not provided in request.
    if (datasetRefreshAfterMs == 0) {
      this.datasetRefreshAfterMs = DEFAULT_REFRESH_MILLIS;
    } else {
      this.datasetRefreshAfterMs = datasetRefreshAfterMs;
    }
  }

  public long getDatasetExpireAfterMs() {
    return datasetExpireAfterMs;
  }

  public void setDatasetExpireAfterMs(long datasetExpireAfterMs) {
    // Set to default if not provided in request.
    if (datasetExpireAfterMs == 0) {
      this.datasetExpireAfterMs = DEFAULT_EXPIRE_MILLIS;
    } else {
      this.datasetExpireAfterMs = datasetExpireAfterMs;
    }
  }

  @NotNull
  public String getDatasetUpdateMode() {
    return datasetUpdateMode;
  }

  public void setDatasetUpdateMode(String datasetUpdateMode) {
    this.datasetUpdateMode = datasetUpdateMode;
  }

  public boolean isDeleteUnavailableDatasets() {
    return deleteUnavailableDatasets;
  }

  public void setDeleteUnavailableDatasets(boolean deleteUnavailableDatasets) {
    this.deleteUnavailableDatasets = deleteUnavailableDatasets;
  }

  public boolean isAutoPromoteDatasets() {
    return autoPromoteDatasets;
  }

  public void setAutoPromoteDatasets(boolean autoPromoteDatasets) {
    this.autoPromoteDatasets = autoPromoteDatasets;
  }

  public com.dremio.service.namespace.source.proto.MetadataPolicy toMetadataPolicy() {
    com.dremio.service.namespace.source.proto.MetadataPolicy metadataPolicy = new com.dremio.service.namespace.source.proto.MetadataPolicy();
    metadataPolicy.setAuthTtlMs(this.getAuthTTLMs());
    metadataPolicy.setNamesRefreshMs(this.getNamesRefreshMs());
    metadataPolicy.setDatasetDefinitionExpireAfterMs(this.getDatasetExpireAfterMs());
    metadataPolicy.setDatasetDefinitionRefreshAfterMs(this.getDatasetRefreshAfterMs());
    metadataPolicy.setDatasetUpdateMode(UpdateMode.valueOf(this.getDatasetUpdateMode()));
    metadataPolicy.setDeleteUnavailableDatasets(this.isDeleteUnavailableDatasets());
    metadataPolicy.setAutoPromoteDatasets(this.isAutoPromoteDatasets());
    return metadataPolicy;
  }

  @Override
  public String toString() {
    return "MetadataPolicy{" +
      "authTTLMs=" + authTTLMs +
      ", namesRefreshMs=" + namesRefreshMs +
      ", datasetRefreshAfterMs=" + datasetRefreshAfterMs +
      ", datasetExpireAfterMs=" + datasetExpireAfterMs +
      ", datasetUpdateMode='" + datasetUpdateMode + "'" +
      ", deleteUnavailableDatasets=" + deleteUnavailableDatasets +
      ", autoPromoteDatasets=" + autoPromoteDatasets +
      '}';
  }
}
