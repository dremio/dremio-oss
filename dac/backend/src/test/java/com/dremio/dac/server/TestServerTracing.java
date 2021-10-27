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
package com.dremio.dac.server;

import static junit.framework.TestCase.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.daemon.DACDaemonModule;
import com.dremio.service.SingletonRegistry;
import com.dremio.telemetry.api.Telemetry;
import com.dremio.telemetry.impl.config.tracing.sampler.SpanAttributeBasedSampler;
import com.dremio.telemetry.utils.TracerFacade;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;

/**
 * Test tracing for Jersey requests.
 */
public class TestServerTracing extends BaseTestServer {
  private MockTracer currentTracer;
  private static List<SingletonRegistry> singletonRegistries = new ArrayList<>();
  private static Tracer originalTracer;

  @Rule
  public OpenTelemetrySetup openTelemetrySetup = OpenTelemetrySetup.create();

  @BeforeClass
  public static void init() throws Exception {
    initializeCluster(new DACDaemonModule() {
      @Override
      public void bootstrap(final Runnable shutdownHook, final SingletonRegistry bootstrapRegistry, ScanResult scanResult, DACConfig dacConfig, boolean isMaster) {
        super.bootstrap(shutdownHook, bootstrapRegistry, scanResult, dacConfig, isMaster);
        /* When running multiple tests in the same process, we need to create a new instance
           of MockTracer for every run because BaseTestServer closes in between tests, which
           in turn closes the MockTracer.
         */
        singletonRegistries.add(bootstrapRegistry);
        originalTracer = ((TracerFacade) bootstrapRegistry.lookup(Tracer.class)).getTracer();
      }
    });
  }

  @Before
  public void setup() {
    currentTracer = new MockTracer();
    singletonRegistries.forEach(r -> ((TracerFacade) r.lookup(Tracer.class)).setTracer(currentTracer));
  }

  @AfterClass
  public static void cleanUp() {
    // Reset TracerFacade to its original state, arbitrarily using the first singletonRegistry.
    ((TracerFacade) singletonRegistries.get(0).lookup(Tracer.class)).setTracer(originalTracer);
  }

  @Test
  public void testTracingHeaderDisabled() {
    expectSuccess(getBuilder(getAPIv2().path("server_status")).header("x-tracing-enabled", Boolean.FALSE).buildGet());
    assertFinishedSpans(0);
  }

  @Test
  @Ignore
  public void testTracingHeaderEnabled() {
    expectSuccess(getBuilder(getAPIv2().path("server_status")).header("x-tracing-enabled", Boolean.TRUE).buildGet());
    assertFinishedSpans(1);
  }

  @Test
  public void testTracingHeaderMangled() {
    expectSuccess(getBuilder(getAPIv2().path("server_status")).header("x-tracing-enabled", "not-a-valid-value").buildGet());
    assertFinishedSpans(0);
  }

  @Test
  public void testTracingNonExistentEndpointWithTracingHeader() {
    expect(FamilyExpectation.CLIENT_ERROR, getBuilder(getAPIv2().path("does-not-exist")).header("x-tracing-enabled", Boolean.TRUE).buildGet());
    assertFinishedSpans(0);
  }

  /*
   Jetty filters may not be executed before the client side fully receives a response,
   this causes the span to not be complete. We use assertWaitForCondition to wait for
   the expected finished spans.
   */
  private void assertFinishedSpans(long finishedSpanCount) {
    System.out.println(openTelemetrySetup.getSpans().size());
    assertWaitForCondition(String.format("Expected %d finished spans.", finishedSpanCount), () -> (openTelemetrySetup.getSpans().size() == finishedSpanCount), 90, TimeUnit.SECONDS);
  }

  /*
   assertWaitForCondition checks if the checkCondition has been met every 200ms.
   If the checkCondition is not met by the timeout period, assertWaitForCondition fails.
   */
  private static void assertWaitForCondition(String message, Supplier<Boolean> checkCondition, long timeout, TimeUnit unit) {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Thread thread = new Thread(() -> {
      while(true) {
        if(checkCondition.get()) {
          countDownLatch.countDown();
          return;
        }

        // We continually check the state of the checkCondition every 200ms.
        try {
          Thread.sleep(200);
        } catch (InterruptedException ex) {
          // Return immediately if this thread is interrupted.
          return;
        }
      }
    });
    thread.start();

    try {
      assertTrue(message, countDownLatch.await(timeout, unit));
    } catch (InterruptedException ex) {
      Assert.fail("Thread was interrupted waiting for condition.");
    } finally {
      thread.interrupt();
    }
  }

  public static class OpenTelemetrySetup extends ExternalResource {

    /**
     * Returns a {@link OpenTelemetrySetup} with a default SDK initialized with an in-memory span
     * exporter and W3C trace context propagation.
     */
    public static OpenTelemetrySetup create() {
      InMemorySpanExporter spanExporter = InMemorySpanExporter.create();

      SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
          .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
          .setSampler(SpanAttributeBasedSampler.builder().setAttributeKey(Telemetry.FORCE_SAMPLING_ATTRIBUTE).build())
          .build();

      OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder()
          .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
          .setTracerProvider(tracerProvider)
          .build();

      return new OpenTelemetrySetup(openTelemetry, spanExporter);
    }

    private final OpenTelemetrySdk openTelemetry;
    private final InMemorySpanExporter spanExporter;

    private OpenTelemetrySetup(OpenTelemetrySdk openTelemetry, InMemorySpanExporter spanExporter) {
      this.openTelemetry = openTelemetry;
      this.spanExporter = spanExporter;
    }

    /** Returns all the exported {@link SpanData} so far. */
    public List<SpanData> getSpans() {
      return spanExporter.getFinishedSpanItems();
    }

    /**
     * Clears the collected exported {@link SpanData}. Consider making your test smaller instead of
     * manually clearing state using this method.
     */
    public void clearSpans() {
      spanExporter.reset();
    }

    @Override
    protected void before() {
      GlobalOpenTelemetry.resetForTest();
      GlobalOpenTelemetry.set(openTelemetry);
      clearSpans();
    }

    @Override
    protected void after() {
      GlobalOpenTelemetry.resetForTest();
    }
  }
}
