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
package com.dremio.exec.planner.acceleration.substitution;

import com.dremio.exec.planner.acceleration.descriptor.MaterializationDescriptor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;

/** An abstraction that represents a single substitution made during acceleration. */
public class SubstitutionInfo {

  private final double acceleratedCost;
  private final List<Substitution> substitutions;

  private SubstitutionInfo(final double acceleratedCost, final List<Substitution> substitutions) {
    this.acceleratedCost = acceleratedCost;
    this.substitutions = Preconditions.checkNotNull(substitutions);
  }

  public double getAcceleratedCost() {
    return acceleratedCost;
  }

  public List<Substitution> getSubstitutions() {
    return substitutions;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private double cost;
    private final List<Substitution> substitutions = Lists.newArrayList();

    private Builder() {}

    public Builder setCost(final double cost) {
      this.cost = cost;
      return this;
    }

    public Builder addSubstitution(final Substitution substitution) {
      substitutions.add(Preconditions.checkNotNull(substitution));
      return this;
    }

    public SubstitutionInfo build() {
      return new SubstitutionInfo(cost, ImmutableList.copyOf(substitutions));
    }
  }

  public static class Substitution {
    private final MaterializationDescriptor materialization;
    private final double speedup;

    @JsonCreator
    public Substitution(
        @JsonProperty("materialization") final MaterializationDescriptor materialization,
        @JsonProperty("speedup") final double speedup) {
      this.materialization = Preconditions.checkNotNull(materialization);
      this.speedup = speedup;
    }

    public MaterializationDescriptor getMaterialization() {
      return materialization;
    }

    public double getSpeedup() {
      return speedup;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Substitution that = (Substitution) o;
      return Double.compare(that.speedup, speedup) == 0
          && Objects.equals(materialization, that.materialization);
    }

    @Override
    public int hashCode() {
      return Objects.hash(materialization, speedup);
    }
  }
}
