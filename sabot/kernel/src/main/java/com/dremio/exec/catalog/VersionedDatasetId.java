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

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
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
  private static final  ObjectMapper objectMapper = new ObjectMapper();

  @JsonCreator
  VersionedDatasetId(@JsonProperty("tableKey") List<String> tableKey,
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
    try {
      return objectMapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      logger.debug("Could not map VersionedDatasetId to String", e);
      return null;
    }
  }

  public static VersionedDatasetId fromString(String idAsString) throws JsonProcessingException {
    //parse the dataset id
    return objectMapper.readValue(idAsString, VersionedDatasetId.class);
  }


  public static VersionedDatasetId tryParse(String idAsString) {
    try {
      return idAsString == null ? null : fromString(idAsString);
    } catch (JsonProcessingException e) {
      return null;
    }
  }


  public static boolean isVersionedDatasetId(String idAsString) {
    try {
      VersionedDatasetId versionedDatasetId = fromString(idAsString);
      return true;
    } catch (JsonProcessingException j) {
      return false;
    }
  }

  public static boolean isTimeTravelDatasetId(String idAsString) {
    try {
      VersionedDatasetId versionedDatasetId = fromString(idAsString);
      return isTimeTravelDatasetId(versionedDatasetId);
    } catch (JsonProcessingException j) {
      return false;
    }
  }

  public static boolean isTimeTravelDatasetId(VersionedDatasetId versionedDatasetId) {
    return versionedDatasetId.getVersionContext().getType() == TableVersionType.TIMESTAMP ||
      versionedDatasetId.getVersionContext().getType() == TableVersionType.SNAPSHOT_ID;
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

  public static boolean isVersioned(String datasetId) {
    return datasetId.indexOf("versionContext") >= 0;
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
      Preconditions.checkNotNull(versionContext);
      if (versionContext.getType() != TableVersionType.TIMESTAMP && versionContext.getType() != TableVersionType.SNAPSHOT_ID) {
        Preconditions.checkNotNull(contentId);
      }
      if (!(versionContext instanceof TableVersionContext)) {
        throw new IllegalArgumentException("versionContext must be of type TableVersionContext");
      }
      return new VersionedDatasetId(tableKey, contentId, versionContext);
    }

  }
}
