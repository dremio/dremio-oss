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

import java.util.Objects;

import com.dremio.telemetry.api.config.ConfigModule;
import com.dremio.telemetry.api.config.TracerConfigurator;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.opentracing.Tracer;

/**
 * Configurator for the jaeger tracing backend.
 *
 * name - service name.
 * type - the trace sampling mode
 * param - the associated parameter. For instance, const and param=1 means all traces are sampled.
 *         ratelimiting and param=2 means 2 traces per second are sampled.
 * logSpans - jaeger logs span finishes. Different than sampling strategy. Sample strategy reports full traces.
 */
@JsonTypeName("jaeger")
public class JaegerConfigurator extends TracerConfigurator {

  private final String name;
  private final String type; // [const | probabilistic | ratelimiting | remote]
  private final Double param;
  private final boolean logSpans; // "Jaeger will simply log the fact that a span was finished, usually by printing the trace and span ID and the operation name."


  @JsonCreator
  public JaegerConfigurator(
    @JsonProperty("serviceName") String name,
    @JsonProperty("samplerType") String type,
    @JsonProperty("samplerParam") Double param,
    @JsonProperty("logSpans") boolean logSpans
  ) {
    super();
    this.name = name;
    this.type = type;
    this.param = param;
    this.logSpans = logSpans;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, param, logSpans);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!other.getClass().equals(this.getClass())) {
      return false;
    }
    JaegerConfigurator j = (JaegerConfigurator) other;
    return Objects.equals(j.logSpans, logSpans)
      && Objects.equals(j.name, name)
      && Objects.equals(j.param, param)
      && Objects.equals(j.type, type);
  }

  @Override
  public Tracer getTracer() {
    SamplerConfiguration sampleConf = SamplerConfiguration.fromEnv()
      .withType(type);

    if (param != null) {
      sampleConf.withParam(param);
    }

    ReporterConfiguration reportConf = ReporterConfiguration.fromEnv()
      .withLogSpans(logSpans);

    Configuration config = new Configuration(name)
      .withReporter(reportConf)
      .withSampler(sampleConf);

    // If the user gets rid of their tracer config and then puts it back, we want to get a fresh tracer since the old
    // one will be closed.
    return config.getTracer();
  }

  /**
   * Module that may be added to a jackson object mapper
   * so it can parse jaeger config.
   */
  public static class Module extends ConfigModule {
    @Override
    public void setupModule(SetupContext context) {
      context.registerSubtypes(JaegerConfigurator.class);
    }
  }
}
