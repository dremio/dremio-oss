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

package com.dremio.dac.model.userpreferences;

import com.dremio.dac.api.JsonISODateTime;
import java.util.List;
import javax.annotation.Nullable;

/** Class Entity */
public class Entity {
  private final String id;
  private final String name;
  private final List<String> fullPath;
  private final String type;
  @JsonISODateTime private final Long starredAt;

  public Entity(String id, String name, List<String> fullPath, String type, Long starredAt) {
    this.id = id;
    this.name = name;
    this.fullPath = fullPath;
    this.type = type;
    this.starredAt = starredAt;
  }

  public String getId() {
    return id;
  }

  @Nullable
  public String getName() {
    return name;
  }

  @Nullable
  public List<String> getFullPath() {
    return fullPath;
  }

  @Nullable
  public String getType() {
    return type;
  }

  public Long getStarredAt() {
    return starredAt;
  }
}
