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
package com.dremio.telemetry.impl.config.tracing.sampler;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;

/** A Simple sampler that bases its decision on the existence of the given Attribute in the Span. */
public class SpanAttributeBasedSampler implements Sampler {
  private AttributeKey<Boolean> attributeKey;
  private Sampler rootSampler;

  SpanAttributeBasedSampler(AttributeKey<Boolean> attributeKey, Sampler rootSampler) {
    this.attributeKey = attributeKey;
    this.rootSampler = rootSampler;
  }

  public static SpanAttributeBasedSamplerBuilder builder() {
    return new SpanAttributeBasedSamplerBuilder();
  }

  @Override
  public SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    Boolean attributeValue = attributes.get(attributeKey);
    if (attributeValue != null && attributeValue) {
      return SamplingResult.create(SamplingDecision.RECORD_AND_SAMPLE);
    }
    return this.rootSampler.shouldSample(
        parentContext, traceId, name, spanKind, attributes, parentLinks);
  }

  @Override
  public String getDescription() {
    return String.format("SpanAttributeBased{root:%s}", rootSampler.getDescription());
  }
}
