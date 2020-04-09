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
package com.dremio.dac.server.tracing;

import java.util.concurrent.atomic.AtomicBoolean;

import io.opentracing.Scope;
import io.opentracing.Span;

/**
 * Holder for the current span and scope.
 */
public final class SpanHolder {
  private final Span span;
  private final Scope scope;
  private final AtomicBoolean finished = new AtomicBoolean();

  public SpanHolder(Span span, Scope scope) {
    this.span = span;
    this.scope = scope;
  }

  public Span getSpan() {
    return span;
  }

  public Scope getScope() {
    return scope;
  }

  public void finish() {
    if(finished.compareAndSet(false, true)) {
      span.finish();
      scope.close();
    }
  }
}
