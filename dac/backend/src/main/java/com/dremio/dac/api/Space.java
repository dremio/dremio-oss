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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.Pattern;

/** Space */
public class Space implements CatalogEntity {
  private final String id;
  private final String name;
  private final String tag;
  @JsonISODateTime private final Long createdAt;
  private final List<CatalogItem> children;
  private final String nextPageToken;

  public Space(String id, String name, String tag, Long createdAt, List<CatalogItem> children) {
    this(id, name, tag, createdAt, children, null);
  }

  @JsonCreator
  public Space(
      @JsonProperty("id") String id,
      @JsonProperty("name") String name,
      @JsonProperty("tag") String tag,
      @JsonProperty("createdAt") Long createdAt,
      @JsonProperty("children") List<CatalogItem> children,
      @JsonProperty("nextPageToken") @Nullable String nextPageToken) {
    this.id = id;
    this.name = name;
    this.tag = tag;
    this.createdAt = createdAt;
    this.children = children;
    this.nextPageToken = nextPageToken;
  }

  public List<CatalogItem> getChildren() {
    return children;
  }

  @Pattern(
      regexp = "^[^.\"@]+$",
      message = "Space name can not contain periods, double quotes or @.")
  public String getName() {
    return name;
  }

  @Override
  public String getId() {
    return id;
  }

  public String getTag() {
    return tag;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  @Nullable
  @Override
  public String getNextPageToken() {
    return nextPageToken;
  }
}
