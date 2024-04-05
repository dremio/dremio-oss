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

import com.dremio.sabot.kernel.proto.ReflectionExplanation;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ReflectionExplanationsAndQueryDistance
    implements Comparable<ReflectionExplanationsAndQueryDistance> {
  final String reflectionId;
  final double queryDistance;
  boolean hintHidden = false;
  List<ReflectionExplanation> displayHintMessageList;

  public ReflectionExplanationsAndQueryDistance(String reflectionId, double queryDistance) {
    this(reflectionId, queryDistance, ImmutableList.of());
  }

  public ReflectionExplanationsAndQueryDistance(
      String reflectionId,
      double queryDistance,
      List<ReflectionExplanation> displayHintMessageList) {
    this.reflectionId = reflectionId;
    this.queryDistance = queryDistance;
    this.displayHintMessageList = displayHintMessageList;
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

  public boolean isHintHidden() {
    return hintHidden;
  }

  @Override
  public int compareTo(ReflectionExplanationsAndQueryDistance that) {
    int scoreComp = Double.compare(this.queryDistance, that.queryDistance);
    if (0 != scoreComp) {
      return scoreComp;
    }
    return this.reflectionId.compareTo(that.reflectionId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReflectionExplanationsAndQueryDistance that = (ReflectionExplanationsAndQueryDistance) o;
    return Double.compare(that.queryDistance, queryDistance) == 0
        && reflectionId.equals(that.reflectionId)
        && displayHintMessageList.equals(that.displayHintMessageList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(reflectionId, queryDistance, displayHintMessageList);
  }

  @Override
  public String toString() {
    return ""
        + "ReflectionExplanationsAndQueryDistance{"
        + "reflectionId='"
        + reflectionId
        + '\''
        + ", queryDistance="
        + queryDistance
        + ", displayHintMessageList={\n\t"
        + displayHintMessageList.stream()
            .map(ExplanationUtil::toString)
            .collect(Collectors.joining("\n\t"))
        + "\n}}";
  }
}
