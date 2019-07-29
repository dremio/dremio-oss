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
package com.dremio.common.store;


import com.dremio.exec.store.dfs.FileSystemConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property="type")
@JsonSubTypes({
  // DX-7316
  @Type(value = FileSystemConfig.class, name = "__delegate"),
  @Type(value = FileSystemConfig.class, name = "pdfs")
})
public abstract class StoragePluginConfig {

  private transient boolean previouslyDisabled;

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();


  @JsonProperty("enabled")
  public void setDeprecatedEnabled(boolean enabled){
    if(!enabled){
      previouslyDisabled = true;
    }
  }

  @JsonIgnore
  public boolean wasPreviouslyDisabled(){
    return previouslyDisabled;
  }
}
