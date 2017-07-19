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
package com.dremio.dac.explore.model;

import java.util.Collections;
import java.util.List;

import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Descendants of a dataset/source.
 */
public class Descendants {

  private final List<DatasetPath>  datasetPaths;

  private final List<PhysicalDatasetPath> physicalDatasetPaths;

  private final int descendantCount;

  @JsonCreator
  public Descendants(@JsonProperty("descendantCount") Integer descendantCount) {
    this.descendantCount = descendantCount;
    datasetPaths = Collections.emptyList();
    physicalDatasetPaths = Collections.emptyList();
  }

  public List<DatasetPath> getDatasetPaths() {
    return datasetPaths;
  }

  public List<PhysicalDatasetPath> getPhysicalDatasetPaths() {
    return physicalDatasetPaths;
  }

  public int getDescendantCount() {
    return descendantCount;
  }
}
