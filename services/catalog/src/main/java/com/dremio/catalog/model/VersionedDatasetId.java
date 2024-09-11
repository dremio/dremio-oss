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
package com.dremio.catalog.model;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Composite DatasetId for a versioned dataset from Nessie */
public class VersionedDatasetId {
  private static final Logger logger = LoggerFactory.getLogger(VersionedDatasetId.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final List<String> tableKey;
  private final @Nullable String contentId;
  private final TableVersionContext versionContext;

  @JsonCreator
  public VersionedDatasetId(
      @JsonProperty("tableKey") List<String> tableKey,
      @JsonProperty("contentId") @Nullable String contentId,
      @JsonProperty("versionContext") TableVersionContext versionContext) {
    this.tableKey = tableKey;
    this.contentId = contentId;
    this.versionContext = versionContext;
  }

  public List<String> getTableKey() {
    return tableKey;
  }

  public @Nullable String getContentId() {
    return contentId;
  }

  public TableVersionContext getVersionContext() {
    return versionContext;
  }

  public @Nullable String asString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      logger.debug("Could not map VersionedDatasetId to String", e);
      return null;
    }
  }

  public static VersionedDatasetId fromString(String idAsString) throws JsonProcessingException {
    // parse the dataset id
    return OBJECT_MAPPER.readValue(idAsString, VersionedDatasetId.class);
  }

  public static @Nullable VersionedDatasetId tryParse(String idAsString) {
    try {
      return idAsString == null ? null : fromString(idAsString);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  public static boolean isVersionedDatasetId(String idAsString) {
    return tryParse(idAsString) != null;
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
    return Objects.equals(tableKey, other.tableKey)
        && Objects.equals(contentId, other.contentId)
        && Objects.equals(versionContext, other.versionContext);
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

    public Builder() {}

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
      Preconditions.checkState(!tableKey.isEmpty());
      Preconditions.checkNotNull(versionContext);
      if (!versionContext.isTimeTravelType()) {
        Preconditions.checkNotNull(contentId);
      }
      return new VersionedDatasetId(tableKey, contentId, versionContext);
    }
  }
}
