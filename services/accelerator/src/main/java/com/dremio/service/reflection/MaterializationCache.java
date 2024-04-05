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
package com.dremio.service.reflection;

import static com.dremio.service.reflection.ExternalReflectionStatus.STATUS.OUT_OF_SYNC;
import static com.dremio.service.reflection.ReflectionMetrics.TAG_SOURCE_DOWN;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_INIT_TIMEOUT;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.planner.acceleration.CachedMaterializationDescriptor;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.ByteString;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;

/**
 * Cache for {@link MaterializationDescriptor} to avoid having to expand all the descriptor's plans
 * for every planned query.
 */
class MaterializationCache {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MaterializationCache.class);

  private static final Map<String, CachedMaterializationDescriptor> EMPTY_MAP = ImmutableMap.of();

  private final AtomicReference<Map<String, CachedMaterializationDescriptor>> cached =
      new AtomicReference<>(EMPTY_MAP);

  private final CountDownLatch latch;

  private final Meter.MeterProvider<Timer> syncHistogram;

  private final Meter.MeterProvider<Counter> errorCounter;

  /**
   * CacheHelper helps with keeping MaterializationCache up to date and also handles materialization
   * expansion. This interface is a bit confusing in that when materialization cache is disabled,
   * this interface still provides the materialization expansion implementation.
   */
  interface CacheHelper {
    Iterable<Materialization> getValidMaterializations();

    Iterable<ExternalReflection> getExternalReflections();

    MaterializationDescriptor getDescriptor(
        ExternalReflection externalReflection, EntityExplorer catalog) throws CacheException;

    /**
     * tries to expand the descriptor's plan
     *
     * @return expanded descriptor, or null if failed to deserialize the plan
     */
    DremioMaterialization expand(MaterializationDescriptor descriptor, Catalog catalog);

    /**
     * tries to expand the external materialization's plan.
     *
     * @return expanded descriptor, or null if failed to deserialize the plan
     */
    CachedMaterializationDescriptor expand(Materialization materialization, Catalog catalog)
        throws CacheException;
  }

  public interface CacheViewer {
    boolean isCached(MaterializationId id);

    boolean isInitialized();
  }

  private final CacheHelper provider;
  private final ReflectionStatusService reflectionStatusService;
  private final CatalogService catalogService;
  private final OptionManager optionManager;

  MaterializationCache(
      CacheHelper provider,
      ReflectionStatusService reflectionStatusService,
      CatalogService catalogService,
      OptionManager optionManager) {
    this.provider = Preconditions.checkNotNull(provider, "materialization provider required");
    this.reflectionStatusService =
        Preconditions.checkNotNull(reflectionStatusService, "reflection status service required");
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalog service required");
    this.optionManager = Preconditions.checkNotNull(optionManager, "option manager required");
    latch = new CountDownLatch(1);
    syncHistogram =
        Timer.builder(ReflectionMetrics.createName(ReflectionMetrics.MAT_CACHE_SYNC))
            .description("Histogram of reflection materialization cache sync times")
            .publishPercentileHistogram()
            .withRegistry(Metrics.globalRegistry);
    Gauge.builder(
            ReflectionMetrics.createName(ReflectionMetrics.MAT_CACHE_ENTRIES),
            () -> cached.get().size())
        .description("Number of materialization cache entries")
        .register(Metrics.globalRegistry);
    errorCounter =
        io.micrometer.core.instrument.Counter.builder(
                ReflectionMetrics.createName(ReflectionMetrics.MAT_CACHE_ERRORS))
            .description("Counter for materialization cache errors")
            .withRegistry(io.micrometer.core.instrument.Metrics.globalRegistry);
  }

  static final class CacheException extends Exception {
    CacheException(String message) {
      super(message);
    }
  }

  @WithSpan
  void refreshMaterializationCache() {
    if (latch.getCount() > 0) {
      syncHistogram
          .withTag(ReflectionMetrics.TAG_MAT_CACHE_INITIAL, "true")
          .record(() -> compareAndSetCache());
    } else {
      syncHistogram
          .withTag(ReflectionMetrics.TAG_MAT_CACHE_INITIAL, "false")
          .record(() -> compareAndSetCache());
    }
  }

  private void compareAndSetCache() {
    final Instant coldStart = Instant.now();
    try {
      boolean exchanged;
      do {
        Map<String, CachedMaterializationDescriptor> old = cached.get();
        Map<String, CachedMaterializationDescriptor> updated = updateMaterializationCache(old);
        exchanged = cached.compareAndSet(old, updated);
        if (!exchanged) {
          logger.warn(
              "Unable to compare and set cache.  Old count: {}.  Updated count: {}",
              old.size(),
              updated.size());
        }
      } while (!exchanged);
    } finally {
      if (latch.getCount() > 0) {
        logger.info(
            "Initial cold materialization cache update took {} ms: expanded={} version={}",
            Duration.between(coldStart, Instant.now()).toMillis(),
            cached.get().size(),
            DremioVersionInfo.getVersion());
      }
      latch.countDown();
    }
  }

  void resetCache() {
    boolean exchanged;
    do {
      Map<String, CachedMaterializationDescriptor> old = cached.get();
      exchanged = cached.compareAndSet(old, EMPTY_MAP);
    } while (!exchanged);
  }

  /**
   * Updates the cache map taking into account the existing cache.<br>
   * Will only "expand" descriptors that are new in the cache.<br>
   * Because, in debug mode, this can be called from multiple threads, it must be thread-safe
   *
   * @param old existing cache
   * @return updated cache
   */
  @WithSpan
  private Map<String, CachedMaterializationDescriptor> updateMaterializationCache(
      Map<String, CachedMaterializationDescriptor> old) {

    // new list of descriptors
    final Iterable<Materialization> provided = provider.getValidMaterializations();
    // this will hold the updated cache
    final Map<String, CachedMaterializationDescriptor> updated = Maps.newHashMap();

    int materializationExpandCount = 0;
    int materializationReuseCount = 0;
    int materializationErrorCount = 0;
    // cache is enabled so we want to reuse as much of the existing cache as possible. Make sure to:
    // remove all cached descriptors that no longer exist
    // reuse all descriptors that are already in the cache
    // add any descriptor that are not already cached
    final Catalog catalog = CatalogUtil.getSystemCatalogForMaterializationCache(catalogService);
    for (Materialization materialization : provided) {
      final CachedMaterializationDescriptor cachedDescriptor =
          old.get(materialization.getId().getId());
      if (cachedDescriptor == null
          || !materialization.getTag().equals(cachedDescriptor.getVersion())
          || schemaChanged(cachedDescriptor, materialization, catalog)) {
        if (updateMaterializationEntry(updated, materialization, catalog)) {
          materializationExpandCount++;
        } else {
          materializationErrorCount++;
        }
      } else {
        // descriptor already in the cache, we can just reuse it
        updated.put(materialization.getId().getId(), cachedDescriptor);
        materializationReuseCount++;
      }
    }

    int externalExpandCount = 0;
    int externalReuseCount = 0;
    int externalErrorCount = 0;
    for (ExternalReflection externalReflection : provider.getExternalReflections()) {
      final CachedMaterializationDescriptor cachedDescriptor = old.get(externalReflection.getId());
      if (cachedDescriptor == null
          || isExternalReflectionOutOfSync(externalReflection.getId())
          || isExternalReflectionMetadataUpdated(cachedDescriptor, catalog)) {
        if (updateExternalReflectionEntry(updated, externalReflection, catalog)) {
          externalExpandCount++;
        } else {
          externalErrorCount++;
        }
      } else {
        // descriptor already in the cache, we can just reuse it
        updated.put(externalReflection.getId(), cachedDescriptor);
        externalReuseCount++;
      }
    }
    logger.info(
        "Materialization cache updated. Materializations: "
            + "reused={} expanded={} errors={}. External: "
            + "reused={} expanded={} errors={}",
        materializationReuseCount,
        materializationExpandCount,
        materializationErrorCount,
        externalReuseCount,
        externalExpandCount,
        externalErrorCount);
    Span.current()
        .setAttribute(
            "dremio.materialization_cache.materializationReuseCount", materializationReuseCount);
    Span.current()
        .setAttribute(
            "dremio.materialization_cache.materializationExpandCount", materializationExpandCount);
    Span.current()
        .setAttribute(
            "dremio.materialization_cache.materializationErrorCount", materializationErrorCount);
    Span.current()
        .setAttribute("dremio.materialization_cache.externalReuseCount", externalReuseCount);
    Span.current()
        .setAttribute("dremio.materialization_cache.externalExpandCount", externalExpandCount);
    Span.current()
        .setAttribute("dremio.materialization_cache.externalErrorCount", externalErrorCount);
    CatalogUtil.clearAllDatasetCache(catalog);
    return updated;
  }

  private boolean isExternalReflectionMetadataUpdated(
      CachedMaterializationDescriptor descriptor, EntityExplorer catalog) {
    DremioMaterialization materialization = descriptor.getMaterialization();
    Pointer<Boolean> updated = new Pointer<>(false);
    materialization
        .getTableRel()
        .accept(
            new RelShuttleImpl() {
              @Override
              public RelNode visit(TableScan tableScan) {
                if (tableScan instanceof ScanCrel) {
                  String version = ((ScanCrel) tableScan).getTableMetadata().getVersion();
                  DatasetConfig datasetConfig =
                      CatalogUtil.getDatasetConfig(
                          catalog, new NamespaceKey(tableScan.getTable().getQualifiedName()));
                  if (datasetConfig == null) {
                    updated.value = true;
                  } else {
                    if (!datasetConfig.getTag().equals(version)) {
                      logger.debug(
                          "Dataset {} has new data. Invalidating cache for external reflection",
                          tableScan.getTable().getQualifiedName());
                      updated.value = true;
                    }
                  }
                } else {
                  updated.value = true;
                }
                return tableScan;
              }
            });
    return updated.value;
  }

  private boolean isExternalReflectionOutOfSync(String id) {
    return reflectionStatusService
            .getExternalReflectionStatus(new ReflectionId(id))
            .getConfigStatus()
        == OUT_OF_SYNC;
  }

  @WithSpan
  private boolean updateExternalReflectionEntry(
      Map<String, CachedMaterializationDescriptor> cache,
      ExternalReflection entry,
      Catalog catalog) {
    Span.current().setAttribute("dremio.materialization_cache.reflection_id", entry.getId());
    Span.current().setAttribute("dremio.materialization_cache.name", entry.getName());
    Span.current()
        .setAttribute("dremio.materialization_cache.query_dataset_id", entry.getQueryDatasetId());
    Span.current()
        .setAttribute("dremio.materialization_cache.target_dataset_id", entry.getTargetDatasetId());
    try {
      final MaterializationDescriptor descriptor = provider.getDescriptor(entry, catalog);
      if (descriptor != null) {
        final DremioMaterialization expanded = provider.expand(descriptor, catalog);
        if (expanded != null) {
          cache.put(
              entry.getId(),
              new CachedMaterializationDescriptor(descriptor, expanded, catalogService));
          return true;
        }
      }
    } catch (AssertionError e) {
      incrementErrorCount(e);
      // Calcite can throw assertion errors even when assertions are disabled :( that's why we need
      // to make sure we catch them here
      logger.debug("couldn't expand materialization {}", entry.getId(), e);
    } catch (Exception ignored) {
      incrementErrorCount(ignored);
      logger.debug("couldn't expand materialization {}", entry.getId(), ignored);
    }
    return false;
  }

  @WithSpan
  private boolean updateMaterializationEntry(
      Map<String, CachedMaterializationDescriptor> cache, Materialization entry, Catalog catalog) {
    try {
      Span.current()
          .setAttribute(
              "dremio.materialization_cache.reflection_id", entry.getReflectionId().getId());
      Span.current()
          .setAttribute("dremio.materialization_cache.materialization_id", entry.getId().getId());
      final CachedMaterializationDescriptor descriptor = provider.expand(entry, catalog);
      if (descriptor != null) {
        cache.put(entry.getId().getId(), descriptor);
        return true;
      }
    } catch (AssertionError e) {
      incrementErrorCount(e);
      // Calcite can throw assertion errors even when assertions are disabled :( that's why we need
      // to make sure we catch them here
      logger.debug("couldn't expand materialization {}", entry.getId(), e);
    } catch (Exception ignored) {
      incrementErrorCount(ignored);
      // Other exceptions are already logged through updateEntry function.
    }
    return false;
  }

  private void incrementErrorCount(Throwable t) {
    errorCounter
        .withTags(
            PlannerMetrics.TAG_REASON,
            t.getClass().getSimpleName(),
            TAG_SOURCE_DOWN,
            ReflectionUtils.isSourceDown(t) ? "true" : "false")
        .increment();
  }

  private boolean schemaChanged(
      MaterializationDescriptor old, Materialization materialization, EntityExplorer catalog) {
    // TODO is this enough ? shouldn't we use the dataset hash instead ??
    final NamespaceKey matKey =
        new NamespaceKey(ReflectionUtils.getMaterializationPath(materialization));

    DatasetConfig datasetConfig = CatalogUtil.getDatasetConfig(catalog, matKey);
    if (datasetConfig == null) {
      return true;
    }

    ByteString schemaString = datasetConfig.getRecordSchema();
    BatchSchema newSchema = BatchSchema.deserialize(schemaString);
    BatchSchema oldSchema =
        ((CachedMaterializationDescriptor) old).getMaterialization().getSchema();
    return !oldSchema.equals(newSchema);
  }

  /**
   * remove entry from the cache
   *
   * @param mId entry to be removed
   */
  void invalidate(MaterializationId mId) {
    boolean exchanged;
    do {
      Map<String, CachedMaterializationDescriptor> old = cached.get();
      if (!old.containsKey(mId.getId())) {
        break; // entry not present in the cache, nothing more to do
      }
      // copy over everything
      Map<String, CachedMaterializationDescriptor> updated = Maps.newHashMap(old);
      // remove the specific materialization.
      updated.remove(mId.getId());
      // update the cache.
      exchanged = cached.compareAndSet(old, updated);
    } while (!exchanged);
  }

  /**
   * Used by reflection manager to immediately update the materialization cache when RM is on the
   * same coordinator. Otherwise, on other coordinators, there could be a materialization cache sync
   * delay.
   */
  void update(Materialization m) throws CacheException, InterruptedException {
    // Let initial cold cache sync complete first so that we don't risk causing the compareAndSet to
    // fail there.
    latch.await(10, TimeUnit.MINUTES);

    // Do expansion (including deserialization) out of the do-while loop, so that in case it takes
    // long time
    // the update loop does not race with MaterializationCache.refresh() and falls into infinite
    // loop.
    final CachedMaterializationDescriptor descriptor =
        provider.expand(m, CatalogUtil.getSystemCatalogForMaterializationCache(catalogService));
    if (descriptor != null) {
      boolean exchanged;
      do {
        Map<String, CachedMaterializationDescriptor> old = cached.get();
        Map<String, CachedMaterializationDescriptor> updated =
            Maps.newHashMap(old); // copy over everything
        updated.put(m.getId().getId(), descriptor);
        exchanged = cached.compareAndSet(old, updated); // update the cache.
      } while (!exchanged);
    }
  }

  /**
   * Returns cached materialization descriptors for logical planning. Blocks on initialization of
   * the materialization cache.
   */
  Iterable<MaterializationDescriptor> getAll() {
    boolean success;
    try {
      success =
          latch.await(
              this.optionManager.getOption(MATERIALIZATION_CACHE_INIT_TIMEOUT), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      success = false;
    }
    if (!success) {
      throw new MaterializationCacheTimeoutException(
          "Timed out waiting for materialization cache to initialize.");
    }
    return Iterables.unmodifiableIterable(cached.get().values());
  }

  boolean isInitialized() {
    if (!optionManager.getOption(MATERIALIZATION_CACHE_ENABLED)) {
      return true;
    }
    return latch.getCount() == 0;
  }

  /** Returns descriptor for default raw reflection matching during convertToRel */
  MaterializationDescriptor get(MaterializationId mId) {
    return cached.get().get(mId.getId());
  }

  /**
   * Callers can check whether a particular reflection is "online" and available for the planner.
   */
  boolean contains(MaterializationId mId) {
    return cached.get().containsKey(mId.getId());
  }

  public static class MaterializationCacheTimeoutException extends RuntimeException {
    public MaterializationCacheTimeoutException(String message) {
      super(message);
    }
  }
}
