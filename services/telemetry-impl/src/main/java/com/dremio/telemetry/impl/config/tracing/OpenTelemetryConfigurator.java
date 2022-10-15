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

import static com.dremio.telemetry.api.Telemetry.FORCE_SAMPLING_ATTRIBUTE;

import com.dremio.telemetry.api.config.ConfigModule;
import com.dremio.telemetry.api.config.TracerConfigurator;
import com.dremio.telemetry.impl.config.tracing.sampler.SpanAttributeBasedSampler;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.logging.otlp.OtlpJsonLoggingSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
import io.opentelemetry.extension.trace.propagation.JaegerPropagator;
import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.extension.trace.jaeger.sampler.JaegerRemoteSampler;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentracing.Tracer;

/**
 * Configuration class for Tracer with OpenTelemetry
 */
@JsonTypeName("opentelemetry")
public class OpenTelemetryConfigurator extends TracerConfigurator {
  private final String serviceName;
  private final String samplerType;
  private final String samplerEndpoint;
  private final Boolean logSpans;
  private final String collectorEndpoint;
  private final String propagator;

  public OpenTelemetryConfigurator(
    @JsonProperty("serviceName") String serviceName,
    @JsonProperty("samplerType") String samplerType,
    @JsonProperty("samplerEndpoint") String samplerEndpoint,
    @JsonProperty("logSpans") Boolean logSpans,
    @JsonProperty("collectorEndpoint") String collectorEndpoint,
    @JsonProperty("propagator") String propagator
  ) {
    this.serviceName = serviceName;
    this.samplerType = samplerType;
    this.samplerEndpoint = samplerEndpoint;
    this.logSpans = logSpans;
    this.collectorEndpoint = collectorEndpoint;
    this.propagator = propagator;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OpenTelemetryConfigurator that = (OpenTelemetryConfigurator) o;
    return Objects.equal(serviceName, that.serviceName)
      && Objects.equal(samplerType, that.samplerType)
      && Objects.equal(samplerEndpoint, that.samplerEndpoint)
      && Objects.equal(logSpans, that.logSpans)
      && Objects.equal(collectorEndpoint, that.collectorEndpoint)
      && Objects.equal(propagator, that.propagator);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(serviceName, samplerType, samplerEndpoint, logSpans, collectorEndpoint, propagator);
  }

  @Override
  public Tracer getTracer() {

    SdkTracerProviderBuilder sdkTracerProviderBuilder = SdkTracerProvider.builder();
    if (logSpans) {
      sdkTracerProviderBuilder.addSpanProcessor(SimpleSpanProcessor.create(OtlpJsonLoggingSpanExporter.create()));
    }

    if (collectorEndpoint != null && !collectorEndpoint.isEmpty()) {
      sdkTracerProviderBuilder.addSpanProcessor(
        BatchSpanProcessor.builder(
          OtlpGrpcSpanExporter.builder().setEndpoint(collectorEndpoint).build()
        ).build());
    }

    Sampler rootSampler;
    switch (samplerType) {
      case "remote":
        if (samplerEndpoint != null && !samplerEndpoint.isEmpty()) {
          rootSampler = JaegerRemoteSampler.builder()
            .setInitialSampler(Sampler.alwaysOn())
            .setEndpoint(samplerEndpoint)
            .setServiceName(serviceName).build();
        } else {
          rootSampler = Sampler.parentBased(Sampler.alwaysOff());
        }
        break;
      case "on":
        rootSampler = Sampler.parentBased(Sampler.alwaysOn());
        break;
      case "off":
        rootSampler = Sampler.parentBased(Sampler.alwaysOff());
        break;
      case "never":
      default:
        rootSampler = null;
    }
    if (rootSampler == null) {
      sdkTracerProviderBuilder.setSampler(Sampler.alwaysOff());
    } else {
      sdkTracerProviderBuilder.setSampler(createSampler(rootSampler));
    }

    //Set up the servicename.. this will show up in Console.
    sdkTracerProviderBuilder.setResource(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, serviceName)));

    ContextPropagators contextPropagators;
    if (propagator == null) {
      contextPropagators = ContextPropagators.create(W3CTraceContextPropagator.getInstance());
    } else {
      switch(propagator.toLowerCase()) {
        case "b3multi":
          contextPropagators = ContextPropagators.create(B3Propagator.injectingMultiHeaders());
          break;
        case "b3":
          contextPropagators = ContextPropagators.create(B3Propagator.injectingSingleHeader());
          break;
        case "jaeger":
          contextPropagators = ContextPropagators.create(JaegerPropagator.getInstance());
          break;
        case "tracecontext":
        default:
          contextPropagators = ContextPropagators.create(W3CTraceContextPropagator.getInstance());
      }
    }

    OpenTelemetrySdk.builder()
      .setTracerProvider(sdkTracerProviderBuilder.build())
      .setPropagators(contextPropagators)
      .buildAndRegisterGlobal();

    return OpenTracingShim.createTracerShim();
  }

  /**
   * Build the sampler based on the given root sampler.
   * @param rootSampler - the sampler to be based on
   * @return the final sampler to be used.
   */
  protected Sampler createSampler(Sampler rootSampler) {
    return SpanAttributeBasedSampler.builder()
      .setAttributeKey(FORCE_SAMPLING_ATTRIBUTE)
      .setRootSampler(rootSampler)
      .build();
  }

  /**
   * Module that may be added to a jackson object mapper
   * so it can parse OpenTelemetry config.
   */
  public static class Module extends ConfigModule {
    @Override
    public void setupModule(SetupContext context) {
      context.registerSubtypes(OpenTelemetryConfigurator.class);
    }
  }
}
