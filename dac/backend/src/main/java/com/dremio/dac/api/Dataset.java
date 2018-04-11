/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.file.FileFormat;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Dataset model
 */
@JsonIgnoreProperties(value={ "fields" }, allowGetters=true, ignoreUnknown = true)
public class Dataset implements CatalogEntity {
  /**
   * Dateset Type
   */
  public enum DatasetType {
    VIRTUAL_DATASET, PHYSICAL_DATASET
  };

  private final String id;
  private final DatasetType type;
  private final List<String> path;
  @JsonSerialize(using = DatasetFieldSerializer.class)
  private List<Field> fields;
  @JsonISODateTime
  private final Long createdAt;
  private final String tag;
  private final RefreshSettings accelerationRefreshPolicy;

  // for VDS
  private final String sql;
  private final List<String> sqlContext;

  // for promoted PDS
  private final FileFormat format;

  public Dataset(
    String id,
    DatasetType type,
    List<String> path,
    List<Field> fields,
    Long createdAt,
    String tag,
    RefreshSettings accelerationRefreshPolicy,
    String sql,
    List<String> sqlContext,
    FileFormat format) {
    this.id = id;
    this.type = type;
    this.path = path;
    this.fields = fields;
    this.createdAt = createdAt;
    this.tag = tag;
    this.accelerationRefreshPolicy = accelerationRefreshPolicy;
    this.sql = sql;
    this.sqlContext = sqlContext;
    this.format = format;
  }

  @JsonCreator
  public Dataset(
    @JsonProperty("id") String id,
    @JsonProperty("type") DatasetType type,
    @JsonProperty("path") List<String> path,
    @JsonProperty("createdAt") Long createdAt,
    @JsonProperty("tag") String tag,
    @JsonProperty("accelerationRefreshPolicy") RefreshSettings accelerationRefreshPolicy,
    @JsonProperty("sql") String sql,
    @JsonProperty("sqlContext") List<String> sqlContext,
    @JsonProperty("format") FileFormat format) {
    // we don't want to deserialize fields ever since they are immutable anyways
    this(id, type, path, null, createdAt, tag, accelerationRefreshPolicy, sql, sqlContext, format);
  }

  public String getId() {
    return id;
  }

  public DatasetType getType() {
    return type;
  }

  public List<String> getPath() {
    return path;
  }

  public List<Field> getFields() {
    return fields;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public String getTag() {
    return tag;
  }

  public String getSql() {
    return sql;
  }

  public List<String> getSqlContext() {
    return sqlContext;
  }

  public FileFormat getFormat() {
    return format;
  }

  public RefreshSettings getAccelerationRefreshPolicy() {
    return accelerationRefreshPolicy;
  }

  /**
   * Dataset acceleration refresh settings
   */
  public static class RefreshSettings {
    private final String refreshField;
    private final Long refreshPeriodMs;
    private final Long gracePeriodMs;
    private final RefreshMethod method;

    @JsonCreator
    public RefreshSettings(
      @JsonProperty("refreshField") String refreshField,
      @JsonProperty("refreshPeriodMs") Long refreshPeriodMs,
      @JsonProperty("gracePeriodMs") Long gracePeriodMs,
      @JsonProperty("method") RefreshMethod method
    ) {
      this.refreshField = refreshField;
      this.refreshPeriodMs = refreshPeriodMs;
      this.gracePeriodMs = gracePeriodMs;
      this.method = method;
    }

    public RefreshSettings(AccelerationSettings settings) {
      refreshField = settings.getRefreshField();
      method = settings.getMethod();
      refreshPeriodMs = settings.getRefreshPeriod();
      gracePeriodMs = settings.getGracePeriod();
    }

    public String getRefreshField() {
      return refreshField;
    }

    public Long getRefreshPeriodMs() {
      return refreshPeriodMs;
    }

    public Long getGracePeriodMs() {
      return gracePeriodMs;
    }

    public RefreshMethod getMethod() {
      return method;
    }

    public AccelerationSettings toAccelerationSettings() {
      AccelerationSettings settings = new AccelerationSettings();

      settings.setRefreshPeriod(getRefreshPeriodMs());
      settings.setGracePeriod(getGracePeriodMs());
      settings.setMethod(getMethod());
      settings.setRefreshField(getRefreshField());

      return settings;
    }
  }
}
