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

package com.dremio.dac.model.scripts;

import com.dremio.dac.api.JsonISODateTime;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.validation.constraints.NotEmpty;

/** The script entity used in the Scripts API request/response */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScriptEntity {
  private final String id;
  private final String name;
  private final String content;
  private final List<@NotEmpty String> context;
  @JsonISODateTime private final Long createdAt;
  private final String createdBy;
  @JsonISODateTime private final Long modifiedAt;
  private final String modifiedBy;

  @JsonCreator
  public ScriptEntity(
      @JsonProperty("id") String id,
      @JsonProperty("name") String name,
      @JsonProperty("content") String content,
      @JsonProperty("context") List<String> context,
      @JsonProperty("createdAt") Long createdAt,
      @JsonProperty("createdBy") String createdBy,
      @JsonProperty("modifiedAt") Long modifiedAt,
      @JsonProperty("modifiedBy") String modifiedBy) {

    this.id = id;
    this.name = name;
    this.content = content;
    this.context = context;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.modifiedAt = modifiedAt;
    this.modifiedBy = modifiedBy;
  }

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public String getContent() {
    return content;
  }

  public List<String> getContext() {
    return context;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public Long getModifiedAt() {
    return modifiedAt;
  }

  public String getModifiedBy() {
    return modifiedBy;
  }
}
