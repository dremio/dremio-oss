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
package com.dremio.exec.work.user;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Transfer Object for substitution related settings
 *
 */
public class SubstitutionSettings {

  private static final SubstitutionSettings EMPTY = new SubstitutionSettings(ImmutableList.<String>of(), false);

  private final List<String> exclusions;
  private final boolean includeIncompleteDatasets;

  public SubstitutionSettings(List<String> exclusions, boolean includeIncompleteDatasets) {
    this.exclusions = ImmutableList.copyOf(exclusions);
    this.includeIncompleteDatasets = includeIncompleteDatasets;
  }

  public static SubstitutionSettings of() {
    return EMPTY;
  }

  /**
   * list of materialization identifiers that shall be excluded from acceleration.
   *  used to exclude self and/or stale dependencies.
   * @return list of materialization ids
   */
  public List<String> getExclusions() {
    return exclusions;
  }

  /**
   * can include incomplete datasets
   * @return boolean
   */
  public boolean isIncludingIncompleteDatasets() {
    return includeIncompleteDatasets;
  }

}
