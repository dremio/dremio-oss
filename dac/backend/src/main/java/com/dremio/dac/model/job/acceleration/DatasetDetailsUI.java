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
package com.dremio.dac.model.job.acceleration;

import java.util.List;

import com.dremio.service.accelerator.proto.DatasetDetails;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * UI wrapper for {@link DatasetDetails}
 */
public class DatasetDetailsUI {
  private final String id;
  private final List<String> path;
  private final DatasetType type;

  @JsonCreator
  DatasetDetailsUI(
    @JsonProperty("id") String id,
    @JsonProperty("path") List<String> path,
    @JsonProperty("type") DatasetType type) {
    this.id = id;
    this.path = path;
    this.type = type;
  }

  DatasetDetailsUI(DatasetDetails details) {
    this.id = details.getId();
    this.path = details.getPathList();
    this.type = details.getType();
  }

  public String getId() {
    return id;
  }

  public List<String> getPath() {
    return path;
  }

  public DatasetType getType() {
    return type;
  }
}
