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

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.acceleration.CachedMaterializationDescriptor;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import io.protostuff.ByteString;

/**
 * Cache for {@link MaterializationDescriptor} to avoid having to expand all the descriptor's plans for every planned
 * query.
 */
class MaterializationCache {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaterializationCache.class);

  private static final Map<String, CachedMaterializationDescriptor> EMPTY_MAP = ImmutableMap.of();

  private final AtomicReference<Map<String, CachedMaterializationDescriptor>> cached = new AtomicReference<>(EMPTY_MAP);

  interface CacheHelper {
    Iterable<Materialization> getValidMaterializations();
    Iterable<ExternalReflection> getExternalReflections();
    MaterializationDescriptor getDescriptor(ExternalReflection externalReflection) throws CacheException;

    /**
     * tries to expand the descriptor's plan
     * @return expanded descriptor, or null if failed to deserialize the plan
     */
    DremioMaterialization expand(MaterializationDescriptor descriptor);

    /**
     * tries to expand the external materialization's plan.
     * if we fail to deserialize the plan, the reflection will be scheduled for update
     * @return expanded descriptor, or null if failed to deserialize the plan
     */
    CachedMaterializationDescriptor expand(Materialization materialization) throws CacheException;
  }

  public interface CacheViewer {
    boolean isCached(MaterializationId id);
  }

  private final CacheHelper provider;
  private final NamespaceService namespaceService;
  private final ReflectionStatusService reflectionStatusService;

  MaterializationCache(CacheHelper provider, NamespaceService namespaceService, ReflectionStatusService reflectionStatusService) {
    this.provider = Preconditions.checkNotNull(provider, "materialization provider required");
    this.namespaceService = Preconditions.checkNotNull(namespaceService, "namespace service required");
    this.reflectionStatusService = Preconditions.checkNotNull(reflectionStatusService, "reflection status service required");
  }

  static final class CacheException extends Exception {
    CacheException(String message) {
      super(message);
    }
  }

  void refresh() {
    compareAndSetCache();
  }

  private void compareAndSetCache() {
    boolean exchanged;
    do {
      Map<String, CachedMaterializationDescriptor> old = cached.get();
      Map<String, CachedMaterializationDescriptor> updated = updateCache(old);
      exchanged = cached.compareAndSet(old, updated);
    } while(!exchanged);
  }

  void resetCache() {
    boolean exchanged;
    do {
      Map<String, CachedMaterializationDescriptor> old = cached.get();
      exchanged = cached.compareAndSet(old, EMPTY_MAP);
    } while(!exchanged);
  }

  /**
   * Updates the cache map taking into account the existing cache.<br>
   * Will only "expand" descriptors that are new in the cache.<br>
   * Because, in debug mode, this can be called from multiple threads, it must be thread-safe
   *
   * @param old existing cache
   * @return updated cache
   */
  private Map<String, CachedMaterializationDescriptor> updateCache(Map<String, CachedMaterializationDescriptor> old) {
    // new list of descriptors
    final Iterable<Materialization> provided = provider.getValidMaterializations();
    // this will hold the updated cache
    final Map<String, CachedMaterializationDescriptor> updated = Maps.newHashMap();

    // cache is enabled so we want to reuse as much of the existing cache as possible. Make sure to:
    // remove all cached descriptors that no longer exist
    // reuse all descriptors that are already in the cache
    // add any descriptor that are not already cached
    for (Materialization materialization : provided) {
      final CachedMaterializationDescriptor cachedDescriptor = old.get(materialization.getId().getId());
      if (cachedDescriptor == null ||
          !materialization.getTag().equals(cachedDescriptor.getVersion()) ||
          schemaChanged(cachedDescriptor, materialization)) {
        safeUpdateEntry(updated, materialization);
      } else {
        // descriptor already in the cache, we can just reuse it
        updated.put(materialization.getId().getId(), cachedDescriptor);
      }
    }

    for (ExternalReflection externalReflection : provider.getExternalReflections()) {
      final CachedMaterializationDescriptor cachedDescriptor = old.get(externalReflection.getId());
      if (cachedDescriptor == null
          || isExternalReflectionOutOfSync(externalReflection.getId())
          || isExternalReflectionMetadataUpdated(cachedDescriptor)) {
        updateEntry(updated, externalReflection);
      } else {
        // descriptor already in the cache, we can just reuse it
        updated.put(externalReflection.getId(), cachedDescriptor);
      }
    }
    return updated;
  }

  private boolean isExternalReflectionMetadataUpdated(CachedMaterializationDescriptor descriptor) {
    DremioMaterialization materialization = descriptor.getMaterialization();
    Pointer<Boolean> updated = new Pointer<>(false);
    materialization.getTableRel().accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan tableScan) {
        if (tableScan instanceof ScanCrel) {
          String version = ((ScanCrel) tableScan).getTableMetadata().getVersion();
          try {
            DatasetConfig dataset = namespaceService.getDataset(new NamespaceKey(tableScan.getTable().getQualifiedName()));
            if (!dataset.getTag().equals(version)) {
              logger.debug("Dataset {} has new data. Invalidating cache for external reflection", tableScan.getTable().getQualifiedName());
              updated.value = true;
            }
          } catch (NamespaceException e) {
            updated.value = true;
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
    return reflectionStatusService.getExternalReflectionStatus(new ReflectionId(id)).getConfigStatus() == OUT_OF_SYNC;
  }

  private void updateEntry(Map<String, CachedMaterializationDescriptor> cache, ExternalReflection entry) {
    try {
      final MaterializationDescriptor descriptor = provider.getDescriptor(entry);
      if (descriptor != null) {
        final DremioMaterialization expanded = provider.expand(descriptor);
        if (expanded != null) {
          cache.put(entry.getId(), new CachedMaterializationDescriptor(descriptor, expanded));
        }
      }
    } catch (Exception e) {
      logger.warn("couldn't expand materialization {}", entry.getId(), e);
    }
  }

  private void safeUpdateEntry(Map<String, CachedMaterializationDescriptor> cache, Materialization entry) {
    try {
      updateEntry(cache, entry);
    } catch (Exception | AssertionError e) {
      // Calcite can throw assertion errors even when assertions are disabled :( that's why we need to make sure we catch them here
      logger.warn("couldn't expand materialization {}", entry.getId().getId(), e);
    }
  }

  private void updateEntry(Map<String, CachedMaterializationDescriptor> cache, Materialization entry) throws CacheException {
    final CachedMaterializationDescriptor descriptor = provider.expand(entry);
    if (descriptor != null) {
      cache.put(entry.getId().getId(), descriptor);
    }
  }

  private boolean schemaChanged(MaterializationDescriptor old, Materialization materialization) {
    if (namespaceService == null) {
      return false;
    }
    try {
      //TODO is this enough ? shouldn't we use the dataset hash instead ??
      final NamespaceKey matKey = new NamespaceKey(ReflectionUtils.getMaterializationPath(materialization));
      ByteString schemaString = namespaceService.getDataset(matKey).getRecordSchema();
      BatchSchema newSchema = BatchSchema.deserialize(schemaString);
      BatchSchema oldSchema = ((CachedMaterializationDescriptor) old).getMaterialization().getSchema();
      return !oldSchema.equals(newSchema);
    } catch (NamespaceException e) {
      return true;
    }
  }

  /**
   * remove entry from the cache
   * @param mId entry to be removed
   */
  void invalidate(MaterializationId mId) {
    boolean exchanged;
    do {
      Map<String, CachedMaterializationDescriptor> old = cached.get();
      if (!old.containsKey(mId.getId())) {
        break; // entry not present in the cache, nothing more to do
      }
      //copy over everything
      Map<String, CachedMaterializationDescriptor> updated =  Maps.newHashMap(old);
      //remove the specific materialization.
      updated.remove(mId.getId());
      //update the cache.
      exchanged = cached.compareAndSet(old, updated);
    } while(!exchanged);
  }

  void update(Materialization m) throws CacheException {
    boolean exchanged;
    do {
      Map<String, CachedMaterializationDescriptor> old = cached.get();
      Map<String, CachedMaterializationDescriptor> updated =  Maps.newHashMap(old); //copy over everything
      updateEntry(updated, m);
      exchanged = cached.compareAndSet(old, updated); //update the cache.
    } while(!exchanged);
  }

  Iterable<MaterializationDescriptor> getAll() {
    return Iterables.unmodifiableIterable(cached.get().values());
  }

  MaterializationDescriptor get(MaterializationId mId) {
    return cached.get().get(mId.getId());
  }

  boolean contains(MaterializationId mId) {
    return cached.get().containsKey(mId.getId());
  }
}
