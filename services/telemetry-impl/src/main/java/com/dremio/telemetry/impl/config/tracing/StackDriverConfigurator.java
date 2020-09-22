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
package com.dremio.telemetry.impl.config.tracing;

import java.io.IOException;
import java.util.Objects;

import com.dremio.telemetry.api.config.ConfigModule;
import com.dremio.telemetry.api.config.TracerConfigurator;
import com.dremio.telemetry.utils.OpenCensusTracerAdapter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Tracing;
import io.opentracing.Tracer;

/**
 * StackDriver config. Only need to set the type name of the tracer config. Then, we will pick up the environment
 * credentials and project id to enable stack driver.
 */
@JsonTypeName("stackdriver")
public class StackDriverConfigurator extends TracerConfigurator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StackDriverConfigurator.class);

  @JsonCreator
  public StackDriverConfigurator() {
    super();
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }

    return other instanceof StackDriverConfigurator;
  }

  @Override
  public Tracer getTracer() {
    try {
      StackdriverTraceExporter.createAndRegister(StackdriverTraceConfiguration.builder().build());
    } catch (IOException e) {
      logger.warn("Could not setup stackdriver tracer", e);
    }
    return new OpenCensusTracerAdapter(Tracing.getTracer());
  }

  /**
   * Module that may be added to a jackson object mapper
   * so it can parse stackdriver config.
   */
  public static class Module extends ConfigModule {
    @Override
    public void setupModule(SetupContext context) {
      context.registerSubtypes(StackDriverConfigurator.class);
    }
  }
}
