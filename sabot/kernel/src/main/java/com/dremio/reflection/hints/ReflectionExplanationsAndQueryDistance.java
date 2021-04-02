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
package com.dremio.reflection.hints;

import java.util.List;

import com.dremio.sabot.kernel.proto.ReflectionExplanation;

public class ReflectionExplanationsAndQueryDistance implements Comparable<ReflectionExplanationsAndQueryDistance> {
  final String reflectionId;
  final double queryDistance;
  List<ReflectionExplanation> displayHintMessageList;

  public ReflectionExplanationsAndQueryDistance(String reflectionId, double queryDistance) {
    this.reflectionId = reflectionId;
    this.queryDistance = queryDistance;
  }

  public String getReflectionId() {
    return reflectionId;
  }

  public double getQueryDistance() {
    return queryDistance;
  }

  public List<ReflectionExplanation> getDisplayHintMessageList() {
    return displayHintMessageList;
  }

  @Override
  public int compareTo(ReflectionExplanationsAndQueryDistance that) {
    int scoreComp = Double.compare(this.queryDistance, that.queryDistance);
    if (0 != scoreComp) {
      return scoreComp;
    }
    return this.reflectionId.compareTo(that.reflectionId);
  }
}
