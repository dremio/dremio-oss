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
package com.dremio.common.tracing;

import static org.junit.Assert.assertEquals;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.noop.NoopSpan;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Test;

/**
 * Tests TracingUtils. Only tests the trace function wrapper since all other trace methods depend on
 * it.
 */
public class TestTracingUtils {

  private MockTracer tracer = new MockTracer();

  @After
  public void cleanup() {
    tracer.reset();
  }

  private void testIsChildSpanCreatedWithParent(Span parent, boolean createsChild) {

    try (Scope scope = tracer.activateSpan(parent)) {
      TracingUtils.buildChildSpan(tracer, "test-child-span").finish();
    }

    assertEquals("Finished span is child span", createsChild, tracer.finishedSpans().size() == 1);
  }

  private void testNoChildSpanWithParent(Span parent) {
    testIsChildSpanCreatedWithParent(parent, false);
  }

  @Test
  public void testNoTracesIfNullParentSpan() {
    testNoChildSpanWithParent(null);
  }

  @Test
  public void testNoTracesIfNoopParentSpan() {
    testNoChildSpanWithParent(NoopSpan.INSTANCE);
  }

  @Test
  public void testChildSpanIfParentSpan() {
    testIsChildSpanCreatedWithParent(tracer.buildSpan("parent").start(), true);
  }

  @Test
  public void testTraceWrapperFunction() {
    MockSpan parent = tracer.buildSpan("parent").start();
    final MockSpan[] child = new MockSpan[1];
    final int ret;
    try (Scope s = tracer.activateSpan(parent)) {
      ret =
          TracingUtils.trace(
              (span) -> {
                span.log("someRunTimeEvent");
                child[0] = ((MockSpan) span);
                return 42;
              },
              tracer,
              "child-work",
              "tag1",
              "val1",
              "tag2",
              "val2");
    }

    assertEquals(42, ret);
    assertEquals(parent.context().spanId(), child[0].parentId());
    assertEquals("child-work", child[0].operationName());
    assertEquals(tracer.finishedSpans().get(0), child[0]);

    final Map<String, Object> expectedTags = new HashMap<>();
    expectedTags.put("tag1", "val1");
    expectedTags.put("tag2", "val2");
    assertEquals(expectedTags, child[0].tags());

    assertEquals(1, child[0].logEntries().size());
  }
}
