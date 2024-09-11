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

import com.dremio.exec.planner.sql.parser.DremioHint;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;

public final class ReflectionHintsExtractor {
  private ReflectionHintsExtractor() {}

  public static ReflectionHints extract(LogicalProject project) {
    Optional<Set<String>> optionalConsiderReflections = Optional.empty();
    Optional<Set<String>> optionalExcludeReflections = Optional.empty();
    Optional<Set<String>> optionalChooseIfMatched = Optional.empty();
    Optional<Boolean> optionalNoReflections = Optional.empty();
    Optional<Boolean> optionalCurrentIcebergDataOnly = Optional.empty();
    for (RelHint hint : project.getHints()) {
      if (hint.hintName.equalsIgnoreCase(DremioHint.CONSIDER_REFLECTIONS.getHintName())) {
        if (optionalConsiderReflections.isEmpty()) {
          optionalConsiderReflections = Optional.of(new HashSet<>());
        }

        optionalConsiderReflections.get().addAll(hint.listOptions);
      } else if (hint.hintName.equalsIgnoreCase(DremioHint.EXCLUDE_REFLECTIONS.getHintName())) {
        if (optionalExcludeReflections.isEmpty()) {
          optionalExcludeReflections = Optional.of(new HashSet<>());
        }

        optionalExcludeReflections.get().addAll(hint.listOptions);
      } else if (hint.hintName.equalsIgnoreCase(DremioHint.CHOOSE_REFLECTIONS.getHintName())) {
        if (optionalChooseIfMatched.isEmpty()) {
          optionalChooseIfMatched = Optional.of(new HashSet<>());
        }

        optionalChooseIfMatched.get().addAll(hint.listOptions);
      } else if (hint.hintName.equalsIgnoreCase(DremioHint.NO_REFLECTIONS.getHintName())) {
        if (hint.listOptions.size() == 1 && "false".equalsIgnoreCase(hint.listOptions.get(0))) {
          optionalNoReflections = Optional.of(false);
        } else {
          optionalNoReflections = Optional.of(true);
        }
      } else if (hint.hintName.equalsIgnoreCase(
          DremioHint.CURRENT_ICEBERG_DATA_ONLY.getHintName())) {
        if (hint.listOptions.size() == 1 && "false".equalsIgnoreCase(hint.listOptions.get(0))) {
          optionalCurrentIcebergDataOnly = Optional.of(false);
        } else {
          optionalCurrentIcebergDataOnly = Optional.of(true);
        }
      }
    }

    return new ReflectionHints(
        optionalConsiderReflections,
        optionalExcludeReflections,
        optionalChooseIfMatched,
        optionalNoReflections,
        optionalCurrentIcebergDataOnly);
  }
}
