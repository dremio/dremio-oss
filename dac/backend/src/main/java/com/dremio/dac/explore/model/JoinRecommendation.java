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
package com.dremio.dac.explore.model;

import com.dremio.dac.proto.model.dataset.JoinType;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;

/** Recommendation for join */
@JsonIgnoreProperties(
    value = {"links"},
    allowGetters = true)
public class JoinRecommendation {

  private final JoinType joinType;
  private final List<String> rightTableFullPathList;

  /** Map from left key to right key. */
  private final Map<String, String> matchingKeys;

  @JsonCreator
  public JoinRecommendation(
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("fullPathList") List<String> rightTableFullPathList,
      @JsonProperty("matchingKeys") Map<String, String> matchingKeys) {
    this.joinType = joinType;
    this.rightTableFullPathList = rightTableFullPathList;
    this.matchingKeys = matchingKeys;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public List<String> getRightTableFullPathList() {
    return rightTableFullPathList;
  }

  public Map<String, String> getMatchingKeys() {
    return matchingKeys;
  }

  public Map<String, String> getLinks() {
    return ImmutableMap.of("data", new DatasetPath(rightTableFullPathList).getPreviewDataUrlPath());
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((joinType == null) ? 0 : joinType.hashCode());
    result = prime * result + ((matchingKeys == null) ? 0 : matchingKeys.hashCode());
    result =
        prime * result + ((rightTableFullPathList == null) ? 0 : rightTableFullPathList.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    JoinRecommendation other = (JoinRecommendation) obj;
    if (joinType != other.joinType) {
      return false;
    }
    if (matchingKeys == null) {
      if (other.matchingKeys != null) {
        return false;
      }
    } else if (!matchingKeys.equals(other.matchingKeys)) {
      return false;
    }
    if (rightTableFullPathList == null) {
      if (other.rightTableFullPathList != null) {
        return false;
      }
    } else if (!rightTableFullPathList.equals(other.rightTableFullPathList)) {
      return false;
    }
    return true;
  }
}
