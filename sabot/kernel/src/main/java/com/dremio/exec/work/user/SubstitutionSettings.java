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
package com.dremio.exec.work.user;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;

/** Transfer Object for substitution related settings */
public class SubstitutionSettings {

  private static final SubstitutionSettings EMPTY =
      new SubstitutionSettings(ImmutableList.<String>of());

  private List<String> exclusions;
  private List<String> inclusions = ImmutableList.of();
  private boolean excludeFileBasedIncremental;

  public SubstitutionSettings(List<String> exclusions) {
    this.exclusions = ImmutableList.copyOf(exclusions);
  }

  public SubstitutionSettings(SubstitutionSettings substitutionSettings) {
    this.exclusions =
        substitutionSettings.exclusions != null
            ? ImmutableList.copyOf(substitutionSettings.exclusions)
            : ImmutableList.of();
    this.inclusions =
        substitutionSettings.inclusions != null
            ? ImmutableList.copyOf(substitutionSettings.inclusions)
            : ImmutableList.of();
    this.excludeFileBasedIncremental = substitutionSettings.excludeFileBasedIncremental;
  }

  public void setExclusions(List<String> exclusions) {
    Preconditions.checkArgument(inclusions.isEmpty());
    this.exclusions = ImmutableList.copyOf(exclusions);
  }

  public void setInclusions(List<String> inclusions) {
    Preconditions.checkArgument(exclusions.isEmpty());
    this.inclusions = inclusions;
  }

  public static SubstitutionSettings of() {
    return EMPTY;
  }

  /**
   * list of materialization identifiers that shall be excluded from acceleration. used to exclude
   * self and/or stale dependencies.
   *
   * @return list of materialization ids
   */
  public List<String> getExclusions() {
    return exclusions;
  }

  /**
   * list of materialization identifiers that should be included for acceleration. If this is set,
   * exclusions cannot be set.
   *
   * @return
   */
  public List<String> getInclusions() {
    return inclusions;
  }

  /**
   * true if file based incrementally refreshed materializations should be excluded for
   * acceleration. This is set to true when we are incrementally refreshing a materialization and
   * Iceberg table format is used for materialization storage. We currently do not support
   * incremental refreshes on Iceberg tables.
   *
   * @return
   */
  public boolean isExcludeFileBasedIncremental() {
    return excludeFileBasedIncremental;
  }

  public void setExcludeFileBasedIncremental(boolean excludeFileBasedIncremental) {
    this.excludeFileBasedIncremental = excludeFileBasedIncremental;
  }
}
