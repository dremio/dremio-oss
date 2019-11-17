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
package com.dremio.metrics;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A interface for configuring and starting metrics. Should include a @JsonTypeName annotation.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class ReporterConfigurator implements AutoCloseable {

  public abstract void configureAndStart(String name, MetricRegistry registry, MetricFilter filter);

  /**
   * Important to implement since this is used to confirm whether to replace an existing configuration.
   */
  @Override
  public abstract int hashCode();

  /**
   * Important to implement since this is used to confirm whether to replace an existing configuration.
   */
  @Override
  public abstract boolean equals(Object other);

  @Override
  public abstract void close();
}
