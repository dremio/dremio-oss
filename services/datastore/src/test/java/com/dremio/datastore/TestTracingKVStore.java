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
package com.dremio.datastore;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;

/**
 * Test that all calls to the tracing kv store are traced appropriately.
 * We only test 2 methods. One that produces a return value and one that does not.
 * We know that all implementations for tracing fall into one of the two categories.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestTracingKVStore {

  private Span parentSpan;
  private Scope parentScope;
  private final MockTracer tracer = new MockTracer();
  @Mock private KVStore<String, String> delegate;

  private KVStore<String, String> underTest;
  private static final String KV_STORE_NAME = "TABLE_NAME";
  private static final String KV_STORE_CREATOR = "THIS_TEST";

  public void setupLegitParentSpan() {
    parentSpan = tracer.buildSpan("test").start();
    parentScope = tracer.activateSpan(parentSpan);
    underTest = new TracingKVStore<>(KV_STORE_CREATOR, tracer, delegate);
  }

  @Before
  public void setup() {
    when(delegate.getName()).thenReturn(KV_STORE_NAME);
  }

  @After
  public void teardown() {
    parentScope.close();
    tracer.reset();
  }

  private void assertChildSpanMethod(String method) {
    assertEquals("There must be a finished span.",1, tracer.finishedSpans().size());
    MockSpan kvSpan = tracer.finishedSpans().get(0);

    assertEquals(TracingKVStore.OPERATION_NAME, kvSpan.operationName());
    assertEquals(KV_STORE_NAME, kvSpan.tags().get(TracingKVStore.TABLE_TAG));

    long openSpanId = Long.valueOf(parentSpan.context().toSpanId());
    assertEquals("The kv span must be the child of the open span.", openSpanId, kvSpan.parentId());

    assertEquals(method, kvSpan.tags().get(TracingKVStore.METHOD_TAG));
  }

  private static class TestDocument implements Document<String, String> {

    private final String key;
    private final String val;

    TestDocument(String k, String v) {
      key = k;
      val = v;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public String getValue() {
      return val;
    }

    @Override
    public String getTag() {
      return null;
    }
  }

  @Test
  public void testTypedReturnMethod() {
    setupLegitParentSpan();

    when(delegate.get("myKey")).thenReturn(new TestDocument("myKey","yourValue"));

    Document<String, String> ret = underTest.get("myKey");

    assertEquals("yourValue", ret.getValue());
    assertChildSpanMethod("get");
  }

  @Test
  public void testVoidReturnMethod() {
    setupLegitParentSpan();

    underTest.delete("byebye");

    verify(delegate).delete("byebye");
    assertChildSpanMethod("delete");
  }
}
