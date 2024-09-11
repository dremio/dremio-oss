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
package com.dremio.exec.planner.sql;

import java.util.Optional;
import java.util.Set;

public final class ReflectionHints {
  private final Optional<Set<String>> optionalConsiderReflections;
  private final Optional<Set<String>> optionalExcludeReflections;
  private final Optional<Set<String>> optionalChooseIfMatched;
  private final Optional<Boolean> optionalNoReflections;
  private final Optional<Boolean> optionalCurrentIcebergDataOnly;

  public ReflectionHints(
      Optional<Set<String>> optionalConsiderReflections,
      Optional<Set<String>> optionalExcludeReflections,
      Optional<Set<String>> optionalChooseIfMatched,
      Optional<Boolean> optionalNoReflections,
      Optional<Boolean> optionalCurrentIcebergDataOnly) {
    this.optionalConsiderReflections = optionalConsiderReflections;
    this.optionalExcludeReflections = optionalExcludeReflections;
    this.optionalChooseIfMatched = optionalChooseIfMatched;
    this.optionalNoReflections = optionalNoReflections;
    this.optionalCurrentIcebergDataOnly = optionalCurrentIcebergDataOnly;
  }

  public Optional<Set<String>> getOptionalConsiderReflections() {
    return optionalConsiderReflections;
  }

  public Optional<Set<String>> getOptionalExcludeReflections() {
    return optionalExcludeReflections;
  }

  public Optional<Set<String>> getOptionalChooseIfMatched() {
    return optionalChooseIfMatched;
  }

  public Optional<Boolean> getOptionalNoReflections() {
    return optionalNoReflections;
  }

  public Optional<Boolean> getOptionalCurrentIcebergDataOnly() {
    return optionalCurrentIcebergDataOnly;
  }
}
