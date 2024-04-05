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

import com.dremio.dac.util.JSONUtil;
import java.util.ArrayList;
import java.util.List;

/** Wrapper for list of join recommendations */
public class JoinRecommendations {
  private List<JoinRecommendation> recommendations;

  public JoinRecommendations() {
    recommendations = new ArrayList<>();
  }

  public List<JoinRecommendation> getRecommendations() {
    return recommendations;
  }

  public void add(JoinRecommendation recommendation) {
    recommendations.add(recommendation);
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
