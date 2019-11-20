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

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.opentracing.Tracer;

/**
 * A interface for configuring and starting metrics. Should include a @JsonTypeName annotation.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class TracerConfigurator {

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

  /**
   * Converts the config into a tracer. Should not register tracer as global tracer.
   */
  public abstract Tracer getTracer();
}
