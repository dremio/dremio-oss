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

import java.util.List;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import com.dremio.dac.api.User;
import com.dremio.service.script.proto.ScriptProto;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ScriptData to format json response
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScriptData {

  private final String scriptId;

  @NotEmpty
  private final String name;
  private final Long createdAt;
  private final User createdBy;
  private final String description;
  private final Long modifiedAt;
  private final User modifiedBy;

  @NotNull
  private final List<@NotEmpty String> context;

  @NotEmpty
  private final String content;

  @JsonCreator
  public ScriptData(
    @JsonProperty("scriptId") String scriptId,
    @JsonProperty("name") String name,
    @JsonProperty("createdAt") Long createdAt,
    @JsonProperty("createdBy") User createdBy,
    @JsonProperty("description") String description,
    @JsonProperty("modifiedAt") Long modifiedAt,
    @JsonProperty("modifiedBy") User modifiedBy,
    @JsonProperty("context") List<String> context,
    @JsonProperty("content") String content) {

    this.scriptId = scriptId;
    this.name = name;
    this.createdAt = createdAt;
    this.createdBy = createdBy;
    this.description = description;
    this.modifiedAt = modifiedAt;
    this.modifiedBy = modifiedBy;
    this.context = context;
    this.content = content;
  }

  public static ScriptProto.ScriptRequest toScriptRequest(ScriptData script) {
    return ScriptProto.ScriptRequest.newBuilder()
      .setName(script.getName())
      .setDescription(script.getDescription())
      .addAllContext(script.getContext())
      .setContent(script.getContent())
      .build();
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    if (description == null) {
      return "";
    }
    return description;
  }

  public List<String> getContext() {
    return context;
  }

  public String getContent() {
    return content;
  }

  public String getId() {
    return scriptId;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public User getCreatedBy() {
    return createdBy;
  }

  public Long getModifiedAt() {
    return modifiedAt;
  }

  public User getModifiedBy() {
    return modifiedBy;
  }

  public static ScriptData fromScriptWithUserInfo(ScriptProto.Script script,
                                                  User createdBy,
                                                  User modifiedBy) {
    return new ScriptData(script.getScriptId(),
                          script.getName(),
                          script.getCreatedAt(),
                          createdBy,
                          script.getDescription(),
                          script.getModifiedAt(),
                          modifiedBy,
                          script.getContextList(),
                          script.getContent());
  }

}
