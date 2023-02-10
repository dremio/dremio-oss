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

import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

public class VersionedDatasetId {
  /**
   * Composite DatasetId for a versioned dataset from Nessie
   */
  private static final Logger logger = LoggerFactory.getLogger(VersionedDatasetId.class);
  private List<String> tableKey;
  private String contentId;
  private TableVersionContext versionContext;

  @JsonCreator
  VersionedDatasetId (@JsonProperty("tableKey") List<String>  tableKey,
                      @JsonProperty("contentId") String contentId,
                      @JsonProperty("versionContext") TableVersionContext versionContext) {
    this.tableKey = tableKey;
    this.contentId = contentId;
    this.versionContext = versionContext;
  }
  public List<String> getTableKey() { return tableKey; };

  public String getContentId() { return contentId; };

  public TableVersionContext getVersionContext() { return versionContext; };

  public String asString()  {
    ObjectMapper om = new ObjectMapper();
    try {
      return om.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      logger.debug("Could not map VersinedDatasetId to String", e);
      return null;
    }
  }

  public static VersionedDatasetId fromString(String idAsString) throws JsonProcessingException {
    //try lookup in external catalog
    //parser the dataset id
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(idAsString, VersionedDatasetId.class);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    VersionedDatasetId other = (VersionedDatasetId) obj;
    return Objects.equals(tableKey, other.tableKey) &&
      Objects.equals(contentId, other.contentId) &&
      Objects.equals(versionContext, other.versionContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableKey, contentId, versionContext);
  }

  public static VersionedDatasetId.Builder newBuilder() {
    return new VersionedDatasetId.Builder();
  }

  public static class Builder {
    private List<String> tableKey;
    private String contentId;
    private TableVersionContext versionContext;
    public Builder() {
    }
    public Builder setTableKey(List<String> key) {
      this.tableKey = key;
      return this;
    }

    public Builder setContentId(String contentId) {
      this.contentId = contentId;
      return this;
    }

    public Builder setTableVersionContext(TableVersionContext tableVersionContext) {
      this.versionContext = tableVersionContext;
      return this;
    }

    public VersionedDatasetId build() {
      Preconditions.checkNotNull(tableKey);
      Preconditions.checkState(tableKey.size() > 0);
      Preconditions.checkNotNull(contentId);
      Preconditions.checkNotNull(versionContext);
      if (!(versionContext instanceof  TableVersionContext)) {
        throw new IllegalArgumentException("Illegal TableVersionContext");
      }
      if ((versionContext.getType() == TableVersionType.SNAPSHOT_ID) || (versionContext.getType() == TableVersionType.TIMESTAMP)) {
        //TODO (DX-58588) Needs to be revisited to support snapshot id and timestamp
        throw new UnsupportedOperationException("No support yet for Snapshot and Timestamp");
      }
      return new VersionedDatasetId(tableKey, contentId, versionContext);
    }

  }
}
