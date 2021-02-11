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

package com.dremio.exec.planner.physical.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/*
 *  runtime filter information for each join column, including join field name from both
 *  probe side and build side.
 */
@JsonTypeName("runtime-filter-entry")
public class RuntimeFilterEntry {

  private final String probeFieldName;
  private final String buildFieldName;
  int probeScanMajorFragmentId;
  int probeScanOperatorId;

  @JsonCreator
  public RuntimeFilterEntry(@JsonProperty("probeFieldName") String probeFieldName, @JsonProperty("buildFieldName") String buildFieldName,
                            @JsonProperty("probeScanMajorFragmentId") int probeScanMajorFragmentId, @JsonProperty("probeScanOperatorId") int probeScanOperatorId) {
    this.probeFieldName = probeFieldName;
    this.buildFieldName = buildFieldName;
    this.probeScanMajorFragmentId = probeScanMajorFragmentId;
    this.probeScanOperatorId = probeScanOperatorId;
  }

  public String getProbeFieldName() {
    return probeFieldName;
  }
  public String getBuildFieldName() {
    return buildFieldName;
  }
  public int getProbeScanMajorFragmentId() {
    return probeScanMajorFragmentId;
  }
  public int getProbeScanOperatorId() {
    return probeScanOperatorId;
  }


  @Override
  public String toString() {
    return String.format("%02d-%02d %s", probeScanMajorFragmentId, probeScanOperatorId & 0xFF, probeFieldName);
  }
}
