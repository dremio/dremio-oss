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
package com.dremio.telemetry.api.metrics;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer.Context;
import com.codahale.metrics.jetty9.InstrumentedHttpChannelListener;
import com.codahale.metrics.jetty9.InstrumentedQueuedThreadPool;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.dremio.telemetry.api.config.MetricsConfigurator;
import com.github.rollingmetrics.counter.ResetOnSnapshotCounter;
import com.github.rollingmetrics.counter.ResetPeriodicallyCounter;
import com.github.rollingmetrics.counter.SmoothlyDecayingRollingCounter;
import com.github.rollingmetrics.counter.WindowCounter;
import com.github.rollingmetrics.histogram.HdrBuilder;
import com.github.rollingmetrics.hitratio.HitRatio;
import com.github.rollingmetrics.hitratio.ResetOnSnapshotHitRatio;
import com.github.rollingmetrics.hitratio.ResetPeriodicallyHitRatio;
import com.github.rollingmetrics.hitratio.SmoothlyDecayingRollingHitRatio;
import com.github.rollingmetrics.hitratio.UniformHitRatio;
import com.github.rollingmetrics.top.Top;
import com.github.rollingmetrics.top.TopBuilder;
import com.github.rollingmetrics.top.TopMetricSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.StandardExports;

/**
 * Dremio main metrics class
 */
public final class Metrics {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metrics.class);
  private static final int DECAY_CHUNKS = 10;

  /**
   * Container for Metrics Registry holder and the reporter manager.
   */
  @VisibleForTesting
  public static class RegistryHolder {
    private static MetricRegistry REGISTRY;
    private static ReporterManager REPORTERS;

    static {
      initRegistry();
    }

    @VisibleForTesting
    public static void initRegistry() {
      REGISTRY = new MetricRegistry();
      registerSysStats();
      REPORTERS = new ReporterManager(REGISTRY);
    }

    private static void registerSysStats(){
      REGISTRY.registerAll(scoped("gc", new GarbageCollectorMetricSet()));
      REGISTRY.registerAll(scoped("buffer-pool", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer())));
      REGISTRY.registerAll(scoped("memory", new MemoryUsageGaugeSet()));
      REGISTRY.registerAll(scoped("threads", new ThreadStatesGaugeSet()));
    }
  }

  /**
   * Helper class for constructing MetricServlets.
   * This is a separate factory class to avoid introducing a dependency on Servlet
   * to the enclosing Metrics class.
   */
  public static class MetricServletFactory {
    /**
     * Creates a custom Prometheus Collector registry, registers
     * the dropwizard metrics with this registry, and attaches
     * it to a MetricsServlet, which is returned to the caller.
     */
    public static Servlet createMetricsServlet() {
      CollectorRegistry registry = new CollectorRegistry();
      registry.register(new DropwizardExports(RegistryHolder.REGISTRY));
      registry.register(new StandardExports());
      return new MetricsServlet(registry);
    }
  }

  private static synchronized void remove(String name) {
    if (RegistryHolder.REGISTRY.remove(name)) {
      logger.warn("Removing old metric since name matched newly registered metric. Metric name: {}", name);
    }
  }

  private static MetricSet scoped(final String name, final MetricSet metricSet) {
    return new MetricSet() {
      @Override
      public Map<String, Metric> getMetrics() {
        ImmutableMap.Builder<String, Metric> scopedMetrics = ImmutableMap.builder();
        for(Map.Entry<String, Metric> entry: metricSet.getMetrics().entrySet()) {
          scopedMetrics.put(MetricRegistry.name(name, entry.getKey()), entry.getValue());
        }
        return scopedMetrics.build();
      }
    };
  }

  public static void onChange(Collection<MetricsConfigurator> updated) {
    RegistryHolder.REPORTERS.onChange(updated);
  }

  public static void resetMetrics(){
    RegistryHolder.REGISTRY.removeMatching((a,b) -> true);
    RegistryHolder.registerSysStats();
  }

  public static synchronized void unregister(String metric) {
    if (!RegistryHolder.REGISTRY.remove(metric)) {
      logger.warn("Could not find metric to unregister. Metric name: {}", metric);
    }
  }

  @VisibleForTesting
  public static Collection<MetricsConfigurator> getConfigurators() {
    return RegistryHolder.REPORTERS.getParentConfigurators();
  }

  public static synchronized void newGauge(String name, LongSupplier supplier) {
    remove(name);
    RegistryHolder.REGISTRY.register(name, (Gauge<Long>) supplier::getAsLong);
  }

  public static synchronized Counter newCounter(String name, ResetType reset) {
    remove(name);
    switch(reset) {
    case NEVER:
      com.codahale.metrics.Counter inner = RegistryHolder.REGISTRY.counter(name);
      return new Counter() {
        @Override
        public void increment(long value, String... tags) {
          inner.inc(value);
        }
        @Override
        public void decrement(long value, String... tags) {
          inner.dec(value);
        }};

    case ON_SNAPSHOT:
      return registerWindowCounter(name, new ResetOnSnapshotCounter());
    case PERIODIC_15M:
    case PERIODIC_1D:
    case PERIODIC_7D:
      return registerWindowCounter(name, new ResetPeriodicallyCounter(reset.getDuration()));
    case PERIODIC_DECAY:
      return registerWindowCounter(name, new SmoothlyDecayingRollingCounter(reset.getDuration(), DECAY_CHUNKS));
    default:
      throw new UnsupportedOperationException("Unknown type: " + reset);
    }
  }

  private static Counter registerWindowCounter(String name, WindowCounter counter) {
    RegistryHolder.REGISTRY.register(name, (Gauge<Long>) counter::getSum);
    return new Counter() {

      @Override
      public void increment(long value, String... tags) {
        counter.add(value);
      }

      @Override
      public void decrement(long value, String... tags) {
        counter.add(-value);
      }

    };
  }

  /**
   * The type of reset behavior a Counter/Histogram/Etc should have.
   */
  public static enum ResetType {
    /**
     * Never reset value.
     */
    NEVER,

    /**
     * Reset each time value is observed.
     */
    ON_SNAPSHOT,

    /**
     * Reset values every 15 minutes.
     */
    PERIODIC_15M(Duration.ofMinutes(15)),

    /**
     * Reset every day
     */
    PERIODIC_1D(Duration.ofDays(1)),

    /**
     * Reset every week
     */
    PERIODIC_7D(Duration.ofDays(7)),

    /**
     * Do multiple trackers. To be implemented.
     */
    MULTI(Duration.ofDays(7)),

    /**
     * Reset periodically with decay.
     */
    PERIODIC_DECAY(Duration.ofDays(7));

    private Duration duration;

    ResetType() {
      this.duration = null;
    }

    ResetType(Duration duration) {
      this.duration = duration;
    }

    public Duration getDuration() {
      return duration;
    }
  }

  /**
   * Create a new reporter that reports the top slow operations over time.
   * @param name The name of the metric
   * @param count The number of items to keep (eg 10 for top 10)
   * @param latencyThreshold The latency below which observations should be ignored.
   * @param reset The desired reset behavior.
   * @return The reporter to use.
   */
  public static synchronized TopMonitor newTopReporter(String name, int count, Duration latencyThreshold, ResetType reset) {
    TopBuilder builder =  Top.builder(count).withLatencyThreshold(latencyThreshold);
    switch(reset) {
    case NEVER:
      builder.neverResetPositions();
      break;
    case ON_SNAPSHOT:
      builder.resetAllPositionsOnSnapshot();
      break;
    case PERIODIC_15M:
    case PERIODIC_1D:
    case PERIODIC_7D:
      builder.resetAllPositionsPeriodically(reset.getDuration());
      break;
    case PERIODIC_DECAY:
      builder.resetPositionsPeriodicallyByChunks(reset.getDuration(), DECAY_CHUNKS);
      break;
    default:
      throw new UnsupportedOperationException("Unknown type: " + reset);
    }

    Top top = builder.build();
    MetricSet metricSet = new TopMetricSet(name, top, TimeUnit.MILLISECONDS, 2);
    RegistryHolder.REGISTRY.registerAll(metricSet);
    return (latency, desc, tags) -> top.update(System.currentTimeMillis(), latency, TimeUnit.MILLISECONDS, () -> desc.get());
  }

  /**
   * Tracks rate of occurrence. For example: requests per second.
   */
  public static synchronized Meter newMeter(String name) {
    remove(name);
    com.codahale.metrics.Meter meter = RegistryHolder.REGISTRY.meter(name);
    return (tags) -> meter.mark();
  }

  /**
   * Create a histogram of values.
   */
  public static synchronized Histogram newHistogram(String name, ResetType reset) {
    remove(name);
    HdrBuilder builder = new HdrBuilder();

    switch(reset) {
    case NEVER:
      com.codahale.metrics.Histogram histogram = RegistryHolder.REGISTRY.histogram(name);
      return (v, tags) -> histogram.update(v);
    case ON_SNAPSHOT:
      builder.resetReservoirOnSnapshot();
      break;
    case PERIODIC_15M:
    case PERIODIC_1D:
    case PERIODIC_7D:
      builder.resetReservoirPeriodically(reset.getDuration());
      break;
    case PERIODIC_DECAY:
      builder.resetReservoirPeriodicallyByChunks(reset.getDuration(), DECAY_CHUNKS);
      break;
    default:
      throw new UnsupportedOperationException("Unknown type: " + reset);
    }

    com.codahale.metrics.Histogram hist = builder.buildAndRegisterHistogram(RegistryHolder.REGISTRY, name);
    return (v, tags) -> hist.update(v);
  }

  /**
   * Create a timer to observe length of operations.
   */
  public static synchronized Timer newTimer(String name, ResetType reset) {
    remove(name);

    HdrBuilder builder = new HdrBuilder();
    switch(reset) {
    case NEVER:
      return asPublicTimer(RegistryHolder.REGISTRY.timer(name));
    case ON_SNAPSHOT:
      builder.resetReservoirOnSnapshot();
      break;
    case PERIODIC_15M:
    case PERIODIC_1D:
    case PERIODIC_7D:
      builder.resetReservoirPeriodically(reset.getDuration());
      break;
    case PERIODIC_DECAY:
      builder.resetReservoirPeriodicallyByChunks(reset.getDuration(), DECAY_CHUNKS);
      break;
    default:
      throw new UnsupportedOperationException("Unknown type: " + reset);
    }

    return asPublicTimer(builder.buildAndRegisterTimer(RegistryHolder.REGISTRY, name));
  }

  private static Timer asPublicTimer(com.codahale.metrics.Timer timer) {
    return (tags) -> {
      Context ctxt = timer.time();
      return () -> ctxt.close();
    };
  }

  /**
   * Create Cache Hit Monitor
   */
  public static synchronized CacheMonitor newCacheMonitor(String name, ResetType reset) {
    remove(name);
    HitRatio ratio;
    switch(reset) {
    case NEVER:
      ratio = new UniformHitRatio();
      break;
    case ON_SNAPSHOT:
      ratio = new ResetOnSnapshotHitRatio();
      break;
    case PERIODIC_15M:
    case PERIODIC_1D:
    case PERIODIC_7D:
      ratio = new ResetPeriodicallyHitRatio(reset.getDuration());
      break;
    case PERIODIC_DECAY:
      ratio = new SmoothlyDecayingRollingHitRatio(reset.getDuration(), DECAY_CHUNKS);
      break;
    default:
      throw new UnsupportedOperationException("Unknown type: " + reset);
    }

    return new CacheMonitor() {

      @Override
      public void miss(String... tags) {
        ratio.incrementMissCount();
      }

      @Override
      public void hit(String... tags) {
        ratio.incrementHitCount();
      }
    };
  }

  /**
   * Join metrics references by period.
   * @param fields
   * @return A period-based concatenated string
   */
  public static String join(String... fields) {
    return Joiner.on('.').join(fields);
  }

  /**
   * Tracks various metrics for Jetty server. This function provides an instance of Jetty {@link org.eclipse.jetty.server.HttpChannel.Listener}
   * injected with appropriate Metric Registry.
   * @param prefix The prefix for metrics captured by this Channel Listener.
   * @return A Channel Listener with Metric Registry injected.
   */
  public static InstrumentedHttpChannelListener newInstrumentedListener(String prefix) {
    return new InstrumentedHttpChannelListener(RegistryHolder.REGISTRY, prefix);
  }

  /**
   * Helper function to provide a QueuedThreadPool instance for Jetty Server injected with appropriate Metric Registry.
   * <br>
   * <b>NOTE:</b>The threadpool is instantiated with default setting for max and min threads, timeout etc. . If any
   * tuning is required, needs to be done after acquiring the instance.
   * @param prefix The prefix for metrics captured by this QueuedThreadPool.
   * @return a QueuedThreadPool instance injected with Metric Registry.
   */
  public static InstrumentedQueuedThreadPool newInstrumentedThreadPool(String prefix) {
    InstrumentedQueuedThreadPool instrumentedQTP = new InstrumentedQueuedThreadPool(RegistryHolder.REGISTRY);
    instrumentedQTP.setPrefix(prefix);
    return instrumentedQTP;
  }

  /**
   * Wrapper class to capture servlet response to modify later.
   */
  public static class CharResponseWrapper extends HttpServletResponseWrapper {
    private CharArrayWriter writer;

    public CharResponseWrapper(HttpServletResponse response) {
      super(response);
      writer = new CharArrayWriter();
    }

    public PrintWriter getWriter() {
      return new PrintWriter(writer);
    }

    public String toString() {
      return writer.toString();
    }

  }

  /**
   * A servlet filter to modify the prometheus export being generated from dropwizard
   * to add "_sum" counter for Summary and Histogram type metrics.
   */
  public static class HistogramSumGeneratorFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException { }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
      CharResponseWrapper wrapper = new CharResponseWrapper((HttpServletResponse) response);

      chain.doFilter(request, wrapper);

      PrintWriter responseWriter = response.getWriter();

      if (wrapper.getContentType().contains("text/plain")) {
        BufferedReader bufferedReader = new BufferedReader(new CharArrayReader(wrapper.writer.toCharArray()));
        String line;
        boolean needsSum = false;
        String metricName = null;
        while( (line=bufferedReader.readLine()) != null) {
          responseWriter.write(line);
          responseWriter.write('\n');
          if (line.contains("# TYPE ")) {
            needsSum = false;  //If type is encountered again, then we need to reset the boolean.
            if (line.endsWith("histogram") || line.endsWith("summary")) {
              needsSum = true;
              metricName = line.split(" ")[2];
              continue;
            }
          }
          if (needsSum && line.contains(metricName+"_count")) {
            line = line.replaceAll(metricName+"_count(.*?) .*$", metricName+"_sum$1 0.0");
            responseWriter.write(line);
            responseWriter.write('\n');
            needsSum = false;
          }
        }
        bufferedReader.close();
      }
    }

    @Override
    public void destroy() {}
  }
}
