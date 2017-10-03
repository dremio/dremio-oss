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
package com.dremio.plugins.elastic;

import java.util.Objects;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.store.StoragePluginConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Storage plugin config for elasticsearch.
 */
@JsonTypeName(ElasticsearchStoragePluginConfig.NAME)
@JsonIgnoreProperties({
  // DX-6687
  "enable_project_pushdown",
  "enable_edge_project_pushdown",
  "enable_filter_pushdown",
  "enable_aggregate_pushdown",
  "enable_limit_pushdown",
  "enable_sample_pushdown"
})
public class ElasticsearchStoragePluginConfig extends StoragePluginConfig {

  public static final String NAME = "elasticsearch";

  private final String hosts;
  private final int batchSize;
  // not final to handle legacy property name with jackson, should be handled as if it is final
  // in all code that interacts with it
  private int scrollTimeoutMillis;
  private final int readTimeoutMillis;
  private final boolean showHiddenSchemas;
  private final boolean enableScripts;
  private final boolean enablePainless;
  private final boolean enableIdColumn;
  private final String username;
  private final String password;
  private final boolean enableSSL;
  private final boolean enableWhitelist;

  /**
   * Construct a new elasticsearch storage plugin configuration.
   * @param hosts
   * @param batchSize
   * @param scrollTimeoutMillis
   * @param showHiddenSchemas
   * @param enableScripts
   * @param enableIdColumn
   * @param username used to connect to a secure cluster, can be null for anonymous connection
   * @param password used to connect to a secure cluster, can be null for anonymous connection
   * @param enableSSL use SSL to connect to elasticsearch
   */
  @JsonCreator
  public ElasticsearchStoragePluginConfig(@JsonProperty("hosts") String hosts,
                                          @JsonProperty("batch_size") int batchSize,
                                          // note there is a setter below for handling a legacy name of this property
                                          @JsonProperty("scroll_timeout_millis") int scrollTimeoutMillis,
                                          @JsonProperty("read_timeout_millis") int readTimeoutMillis,
                                          @JsonProperty("show_hidden_schemas") boolean showHiddenSchemas,
                                          @JsonProperty("enable_scripts") boolean enableScripts,
                                          @JsonProperty("enable_painless") boolean enablePainless,
                                          @JsonProperty("enable_id_column") boolean enableIdColumn,
                                          @JsonProperty("username") String username,
                                          @JsonProperty("password") String password,
                                          @JsonProperty("enable_ssl") boolean enableSSL,
                                          @JsonProperty("enable_whitelist") boolean enableWhitelist) {
    if (batchSize < 128 || batchSize > 65535) {
      throw UserException.validationError()
        .message("Failure creating ElasticSearch connection. The valid range for scroll size is 128 - 65535.")
        .build();
    }
    this.hosts = hosts;
    this.batchSize = batchSize;
    this.scrollTimeoutMillis = scrollTimeoutMillis;
    this.readTimeoutMillis = readTimeoutMillis;
    this.showHiddenSchemas = showHiddenSchemas;
    this.enableScripts = enableScripts;
    this.enablePainless = enablePainless;
    this.enableIdColumn = enableIdColumn;
    this.username = username;
    this.password = password;
    this.enableSSL = enableSSL;
    this.enableWhitelist = enableWhitelist;
  }

  @JsonProperty("hosts")
  public String getHosts() {
    return hosts;
  }

  @JsonProperty("username")
  public String getUsername() {
    return username;
  }

  @JsonProperty("password")
  public String getPassword() {
    return password;
  }

  @JsonProperty("batch_size")
  public int getBatchSize() {
    return batchSize;
  }

  /**
   * Do not use in code, this is only to handle a legacy property name.
   *
   * This field should be treated like it is final.
   *
   * @param scrollTimeoutMillis
   */
  @Deprecated
  @JsonProperty("scroll_timeout")
  public void setScrollTimeoutMillis(int scrollTimeoutMillis) {
    this.scrollTimeoutMillis = scrollTimeoutMillis;
  }

  @JsonProperty("scroll_timeout_millis")
  public int getScrollTimeoutMillis() {
    return scrollTimeoutMillis;
  }

  @JsonIgnore
  public String getScrollTimeoutFormatted() {
    return scrollTimeoutMillis + "ms";
  }

  @JsonProperty("read_timeout_millis")
  public int getReadTimeoutMillis() {
    return readTimeoutMillis;
  }

  @JsonIgnore
  public String getReadTimeoutFormatted() {
    return readTimeoutMillis + "ms";
  }

  @JsonProperty("show_hidden_schemas")
  public boolean isShowHiddenSchemas() {
    return showHiddenSchemas;
  }

  @JsonProperty("enable_scripts")
  public boolean isEnableScripts() {
    return enableScripts;
  }

  @JsonProperty("enable_painless")
  public boolean isEnablePainless() {
    return enablePainless;
  }

  @JsonProperty("enable_id_column")
  public boolean isIdColumnEnabled() {
    return enableIdColumn;
  }

  @JsonProperty("enable_ssl")
  public boolean isEnableSSL() {
    return enableSSL;
  }

  @JsonProperty("enable_whitelist")
  public boolean isEnableWhitelist() {
    return enableWhitelist;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ElasticsearchStoragePluginConfig that = (ElasticsearchStoragePluginConfig) o;
    return batchSize == that.batchSize &&
        scrollTimeoutMillis == that.scrollTimeoutMillis &&
        readTimeoutMillis == that.readTimeoutMillis &&
        enableScripts == that.enableScripts &&
        enableIdColumn == that.enableIdColumn &&
        Objects.equals(hosts, that.hosts) &&
        Objects.equals(username, that.username) &&
        Objects.equals(password, that.password) &&
        enableSSL == that.enableSSL &&
        enableWhitelist == this.enableWhitelist;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      hosts, batchSize, scrollTimeoutMillis, readTimeoutMillis, enableScripts, enableIdColumn, username, password, enableSSL, enableWhitelist);
  }
}
