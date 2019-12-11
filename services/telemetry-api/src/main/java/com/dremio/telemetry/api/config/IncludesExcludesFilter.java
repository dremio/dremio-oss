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
package com.dremio.telemetry.api.config;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

/**
 * A metric filter that will only include values that match the provided includes (if any) then excludes any paths
 * identified by excludes.
 */
class IncludesExcludesFilter implements MetricFilter {

  private final List<Pattern> includes;
  private final List<Pattern> excludes;

  public IncludesExcludesFilter(List<String> includes, List<String> excludes) {
    this.includes = asPatterns(includes, "include");
    this.excludes = asPatterns(excludes, "exclude");
  }

  @Override
  public boolean matches(String name, Metric metric) {
    if (includes.isEmpty() || matches(name, includes)) {
      return allowedViaExcludes(name);
    }
    return false;
  }

  private boolean allowedViaExcludes(String name) {
    if (excludes.isEmpty()) {
      return true;
    }

    return !matches(name, excludes);
  }

  private static boolean matches(String name, List<Pattern> patterns) {
    for (Pattern p : patterns) {
      if (p.matcher(name).matches()) {
        return true;
      }
    }

    return false;
  }

  private static List<Pattern> asPatterns(List<String> values, String type) {
    return values.stream().filter(v -> v != null && !v.isEmpty()).map(v -> {
      try {
        return Pattern.compile(v);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException(
            String.format("Failure while trying to parse %s pattern of value %s.", type, v), e);
      }
    }).collect(Collectors.toList());
  }

  @Override
  public int hashCode() {
    return Objects.hash(includes, excludes);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass() != IncludesExcludesFilter.class) {
      return false;
    }

    IncludesExcludesFilter o = (IncludesExcludesFilter) other;
    return Objects.deepEquals(includes, o.includes) && Objects.deepEquals(excludes, o.excludes);
  }
}
