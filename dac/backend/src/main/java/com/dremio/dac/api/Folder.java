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
package com.dremio.dac.api;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterables;

/**
 * Folder
 */
public class Folder implements CatalogEntity {
  private final String id;
  private final List<String> path;
  private final String tag;
  private final List<CatalogItem> children;

  @JsonCreator
  public Folder(
    @JsonProperty("id") String id,
    @JsonProperty("path") List<String> path,
    @JsonProperty("tag") String tag,
    @JsonProperty("children") List<CatalogItem> children
  ) {
    this.id = id;
    this.path = path;
    this.tag = tag;
    this.children = children;
  }

  @JsonIgnore
  public String getName() {
    return Iterables.getLast(getPath());
  }

  public String getId() {
    return id;
  }

  public List<String> getPath() {
    return path;
  }

  public String getTag() {
    return tag;
  }

  public List<CatalogItem> getChildren() {
    return children;
  }
}
