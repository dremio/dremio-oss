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
import io.opentelemetry.sdk.trace.samplers.Sampler;

/**
 * Build for SpanAttributeBasedSampler
 */
public class SpanAttributeBasedSamplerBuilder {
  private AttributeKey<Boolean> attributeKey;
  private Sampler rootSampler = Sampler.parentBased(Sampler.alwaysOff());

  SpanAttributeBasedSamplerBuilder() {
  }

  public SpanAttributeBasedSamplerBuilder setAttributeKey(String attributeKey) {
    this.attributeKey = AttributeKey.booleanKey(attributeKey);
    return this;
  }

  public SpanAttributeBasedSamplerBuilder setAttributeKey(AttributeKey<Boolean> attributeKey) {
    this.attributeKey = attributeKey;
    return this;
  }

  public SpanAttributeBasedSamplerBuilder setRootSampler(Sampler rootSampler) {
    this.rootSampler = rootSampler;
    return this;
  }

  public SpanAttributeBasedSampler build() {

    return new SpanAttributeBasedSampler(attributeKey, rootSampler);
  }
}
