/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.service.namespace.source.proto.UpdateMode;

/**
 * Source MetadataPolicy for the public REST API.
 */
public class MetadataPolicy {
  private long authTTLMs;
  private long namesRefreshMs;
  private long datasetRefreshAfterMs;
  private long datasetExpireAfterMs;
  private String datasetUpdateMode;

  public MetadataPolicy() { }

  public MetadataPolicy(com.dremio.service.namespace.source.proto.MetadataPolicy policy) {
    this.authTTLMs = policy.getAuthTtlMs();
    this.namesRefreshMs = policy.getNamesRefreshMs();
    this.datasetRefreshAfterMs = policy.getDatasetDefinitionRefreshAfterMs();
    this.datasetExpireAfterMs = policy.getDatasetDefinitionExpireAfterMs();
    this.datasetUpdateMode = policy.getDatasetUpdateMode().name();
  }

  public Long getAuthTTLMs() {
    return authTTLMs;
  }

  public void setAuthTTLMs(Long authTTLMs) {
    this.authTTLMs = authTTLMs;
  }

  public long getNamesRefreshMs() {
    return namesRefreshMs;
  }

  public void setNamesRefreshMs(long namesRefreshMs) {
    this.namesRefreshMs = namesRefreshMs;
  }

  public long getDatasetRefreshAfterMs() {
    return datasetRefreshAfterMs;
  }

  public void setDatasetRefreshAfterMs(long datasetRefreshAfterMs) {
    this.datasetRefreshAfterMs = datasetRefreshAfterMs;
  }

  public long getDatasetExpireAfterMs() {
    return datasetExpireAfterMs;
  }

  public void setDatasetExpireAfterMs(long datasetExpireAfterMs) {
    this.datasetExpireAfterMs = datasetExpireAfterMs;
  }

  public String getDatasetUpdateMode() {
    return datasetUpdateMode;
  }

  public void setDatasetUpdateMode(String datasetUpdateMode) {
    this.datasetUpdateMode = datasetUpdateMode;
  }

  public com.dremio.service.namespace.source.proto.MetadataPolicy toMetadataPolicy() {
    com.dremio.service.namespace.source.proto.MetadataPolicy metadataPolicy = new com.dremio.service.namespace.source.proto.MetadataPolicy();
    metadataPolicy.setAuthTtlMs(this.getAuthTTLMs());
    metadataPolicy.setNamesRefreshMs(this.getNamesRefreshMs());
    metadataPolicy.setDatasetDefinitionExpireAfterMs(this.getDatasetExpireAfterMs());
    metadataPolicy.setDatasetDefinitionRefreshAfterMs(this.getDatasetRefreshAfterMs());
    String datasetUpdateMode = this.getDatasetUpdateMode();
    metadataPolicy.setDatasetUpdateMode(UpdateMode.valueOf(this.getDatasetUpdateMode()));
    return metadataPolicy;
  }

  @Override
  public String toString() {
    return "MetadataPolicy{" +
      "authTTLMs=" + authTTLMs +
      ", namesRefreshMs=" + namesRefreshMs +
      ", datasetRefreshAfterMs=" + datasetRefreshAfterMs +
      ", datasetExpireAfterMs=" + datasetExpireAfterMs +
      ", datasetUpdateMode='" + datasetUpdateMode + '\'' +
      '}';
  }
}
