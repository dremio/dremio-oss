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
package com.dremio.dac.explore.model;

import java.util.List;

import javax.validation.constraints.NotNull;

import com.dremio.dac.util.JSONUtil;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * this class is used to pass info about target dataset from ui
 */
public class ReapplyDataset {
  @NotNull
  private final List<String> datasetPathElements;
  @NotNull
  private final DatasetVersion version;

  @JsonCreator
  public ReapplyDataset(
      @JsonProperty("dataset") List<String> datasetPathElements,
      @JsonProperty("version") DatasetVersion version) {
    this.datasetPathElements = datasetPathElements;
    this.version = version;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }

  public List<String> getDatasetPathElements() {
    return datasetPathElements;
  }

  public DatasetVersion getVersion() {
    return version;
  }
}
