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
package com.dremio.dac.model.resourcetree;

import java.util.Collections;
import java.util.List;

import com.dremio.dac.model.resourcetree.ResourceTreeEntity.ResourceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

/**
 * Resources listed under a folder/space/source/home.
 */
public class ResourceList {

  @JsonUnwrapped
  private final List<ResourceTreeEntity> resources;

  public ResourceList() {
    resources = Collections.emptyList();
  }

  @JsonCreator
  public ResourceList(
    @JsonProperty("resources") List<ResourceTreeEntity> resources) {
    this.resources = resources;
  }

  public List<ResourceTreeEntity> getResources() {
    return resources;
  }

  @JsonIgnore
  public int count(ResourceType type) {
    int count = 0;
    for (ResourceTreeEntity resource : resources) {
      if (resource.getType() == type) {
        ++count;
      }
    }
    return count;
  }

  @JsonIgnore
  public ResourceTreeEntity find(String name, ResourceType type) {
    for (ResourceTreeEntity resource : resources) {
      if (name.equals(resource.getName()) && resource.getType() == type) {
        return resource;
      }
    }
    return null;
  }
}
