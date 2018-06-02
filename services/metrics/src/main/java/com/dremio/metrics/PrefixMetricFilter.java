/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.google.common.base.Preconditions;

/**
 * matches all metrics with a name that starts with a given prefix
 */
public class PrefixMetricFilter implements MetricFilter {

  private final String prefix;

  public PrefixMetricFilter(String prefix) {
    this.prefix = Preconditions.checkNotNull(prefix, "prefix required");
  }
  @Override
  public boolean matches(String name, Metric metric) {
    return name.startsWith(prefix);
  }
}
