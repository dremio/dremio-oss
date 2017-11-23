/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.accelerator;

import static com.dremio.exec.ExecConstants.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.exec.ExecConstants.MATERIALIZATION_CACHE_REFRESH_DURATION;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers.KryoDeserializationException;
import com.dremio.exec.planner.sql.CachedMaterializationDescriptor;
import com.dremio.exec.planner.sql.DremioRelOptMaterialization;
import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.work.AttemptId;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.users.SystemUser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.protostuff.ByteString;

/**
 * Cached implementation. Tries to reuse as much of the existing cache as possible when refreshing
 */
public class CachedMaterializationProvider implements MaterializationDescriptorProvider, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CachedMaterializationProvider.class);

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private AtomicReference<Map<String, MaterializationDescriptor>> cached = new AtomicReference<>();

  private final MaterializationDescriptorProvider provider;
  private final OptionManager options; // we need this to check if the cache is enabled or not
  /** dummy QueryContext used to create the SqlConverter, must be closed or we'll leak a ChildAllocator */
  private final QueryContext queryContext;
  private final AccelerationService accel;
  private final StoragePluginRegistry storageRegistry;
  private final NamespaceService namespaceService;

  /** when false, the loading cache will be bypassed and the refreshes don't expand the materializations */
  private volatile boolean enabled;
  /** true if the update thread successfully updated the cache.<br>
   *  if the last cache update failed or the cache was just enabled and the cache wasn't updated yet, this value will be false */
  private volatile boolean upToDate;

  // special mode where the cache update is done synchronously whenever get() is called
  // mainly needed for testing as it will exercise both cache updating and rel nodes copying
  private boolean debug;

  private final SettableFuture<Void> startedFuture = SettableFuture.create();

  private SqlConverter defaultConverter;

  private static final Map<String, MaterializationDescriptor> EMPTY_MAP = ImmutableMap.of();

  public CachedMaterializationProvider(MaterializationDescriptorProvider provider,
                                       Provider<SabotContext> contextProvider, AccelerationService accel) {
    this.provider = Preconditions.checkNotNull(provider, "provider is required");
    this.accel = Preconditions.checkNotNull(accel, "acceleration service is required");
    Preconditions.checkNotNull(contextProvider, "context is required");

    final SabotContext context = contextProvider.get();
    storageRegistry = context.getStorage();
    namespaceService = context.getNamespaceService(SystemUser.SYSTEM_USERNAME);

    this.options = context.getOptionManager();

    final UserSession session = systemSession(options);

    this.queryContext = new QueryContext(session, context, new AttemptId().toQueryId());

    final long refreshDelay = options.getOption(MATERIALIZATION_CACHE_REFRESH_DURATION);

    enabled = options.getOption(MATERIALIZATION_CACHE_ENABLED);
    cached.set(EMPTY_MAP);

    initUpdater(refreshDelay);
  }

  /**
   * @return ListenableFuture that gets set when the cache is updated for the very first time after startup
   */
  ListenableFuture<Void> getStartedFuture() {
    return startedFuture;
  }

  @VisibleForTesting
  void setDefaultConverter(SqlConverter converter) {
    this.defaultConverter = converter;
  }

  private UserSession systemSession(OptionManager options) {
    final UserBitShared.UserCredentials credentials = UserBitShared.UserCredentials.newBuilder()
      .setUserName(SYSTEM_USERNAME)
      .build();
    return UserSession.Builder.newBuilder()
      .withCredentials(credentials)
      .withOptionManager(options)
      .exposeInternalSources(true)
      .build();
  }

  /**
   * schedules a thread that will asynchronously update the cache. First update will be scheduled immediately,
   * and each subsequent update will start after the previous update finishes + refreshDelayInSeconds
   * @param refreshDelayInSeconds delay, in seconds, between the termination of a cache refresh and the start of the next one
   */
  private void initUpdater(long refreshDelayInSeconds) {
    scheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        refresh();
      }
    }, 0, refreshDelayInSeconds, TimeUnit.SECONDS);
  }

  private void refresh() {
    // no need to update the cache when it's disabled, get() will go straight to the provider anyway
    if (enabled) {
      try {
        compareAndSetCache();
        upToDate = true;

        if (!startedFuture.isDone()) {
          startedFuture.set(null);
        }
      } catch (Throwable e) {
        upToDate = false;
        cached.set(EMPTY_MAP);
        logger.warn("Materialization cache update failed, cache will be disabled until a future update is successful", e);
      }
    }
  }

  private void compareAndSetCache() {
    boolean exchanged;
    do {
      Map<String, MaterializationDescriptor> old = cached.get();
      Map<String, MaterializationDescriptor> updated = updateCache(old);
      exchanged = cached.compareAndSet(old, updated);
    } while(!exchanged);
  }

  private void compareAndSetCache(MaterializationDescriptor materializationDescriptor) {
    boolean exchanged;
    CachedMaterializationDescriptor cachedMaterializationDescriptor;
    DremioRelOptMaterialization materialization = expand(materializationDescriptor);
    if (materialization != null) {
      cachedMaterializationDescriptor =  new CachedMaterializationDescriptor(materializationDescriptor, materialization);
      final String layoutId = cachedMaterializationDescriptor.getLayoutId();
      do {
        Map<String, MaterializationDescriptor> old = cached.get();

        //copy over everything except the materialization belonging to the layout being updated
        Map<String, MaterializationDescriptor> updated =  Maps.newHashMap();
        for (MaterializationDescriptor descriptor: old.values()) {
          if (!descriptor.getLayoutId().equals(layoutId)) {
            updated.put(descriptor.getMaterializationId(), descriptor);
          }
        }
        //add the new materialization.
        updated.put(cachedMaterializationDescriptor.getMaterializationId(), cachedMaterializationDescriptor);

        //update the cache.
        exchanged = cached.compareAndSet(old, updated);
      } while(!exchanged);
    }
  }

  @Override
  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  /**
   * Updates the cache map taking into account the existing cache.<br>
   * Will only "expand" descriptors that are new in the cache.<br>
   * Because, in debug mode, this can be called from multiple threads, it must be thread-safe
   *
   * @param old existing cache
   * @return updated cache
   */
  private Map<String, MaterializationDescriptor> updateCache(Map<String, MaterializationDescriptor> old) {
    // new list of descriptors
    final List<MaterializationDescriptor> provided = provider.get(true);
    // this will hold the updated cache
    final Map<String, MaterializationDescriptor> updated = Maps.newHashMap();

    // cache is enabled so we want to reuse as much of the existing cache as possible. Make sure to:
    // remove all cached descriptors that no longer exist
    // reuse all descriptors that are already in the cache
    // add any descriptor that are not already cached
    for (MaterializationDescriptor descriptor : provided) {
      final MaterializationDescriptor cachedDescriptor = old.get(descriptor.getMaterializationId());
      if (cachedDescriptor == null || hasChanged(cachedDescriptor, descriptor) || schemaChanged(cachedDescriptor)) {
        DremioRelOptMaterialization materialization = expand(descriptor);
        if (materialization != null) {
          updated.put(descriptor.getMaterializationId(), new CachedMaterializationDescriptor(descriptor, materialization));
        }
      } else {
        // descriptor already in the cache, we can just reuse it
        updated.put(descriptor.getMaterializationId(), cachedDescriptor);
      }
    }
    return updated;
  }

  private boolean schemaChanged(MaterializationDescriptor old) {
    if (namespaceService == null) {
      return false;
    }
    String layoutId = old.getLayoutId();
    String materializationId = old.getMaterializationId();
    try {
      ByteString schemaString = namespaceService.getDataset(new NamespaceKey(ImmutableList.of("__accelerator", layoutId, materializationId))).getRecordSchema();
      BatchSchema newSchema = BatchSchema.deserialize(schemaString);
      BatchSchema oldSchema = ((CachedMaterializationDescriptor) old).getMaterialization().getSchema();
      return !oldSchema.equals(newSchema);
    } catch (NamespaceException e) {
      return true;
    }
  }

  private static boolean hasChanged(MaterializationDescriptor old, MaterializationDescriptor current) {
    return !(Objects.equals(old.getUpdateId(), current.getUpdateId()) &&
        old.isComplete() == current.isComplete() &&
        old.getExpirationTimestamp() == current.getExpirationTimestamp() &&
        Objects.deepEquals(old.getPlan(), current.getPlan()) &&
        Objects.deepEquals(old.getPath(), current.getPath()) &&
        Math.abs(old.getOriginalCost() - current.getOriginalCost()) <= 0.000001 &&
        Objects.equals(old.getIncrementalUpdateSettings(), current.getIncrementalUpdateSettings()));
  }

  private DremioRelOptMaterialization expand(MaterializationDescriptor descriptor) {
    DremioRelOptMaterialization materialization = null;
    // it's a new descriptor, expand and add it to the cache
    try {
      // get a new converter for each materialization. This ensures that we
      // always index flattens from zero. This is a partial fix for flatten
      // matching. We should really do a better job in matching.
      SqlConverter converter = defaultConverter;
      if (converter == null) {
         converter = SqlConverter.dummy(queryContext, storageRegistry);
      }
      materialization = descriptor.getMaterializationFor(converter);
    } catch (KryoDeserializationException e) {
      //DX-7858: we should only replan once in a single cache update
      logger.warn("Need to replan. MaterializationId = {}, LayoutId = {}",
          descriptor.getMaterializationId(), descriptor.getLayoutId(), e);
      accel.replan(new LayoutId(descriptor.getLayoutId()));
    } catch (Throwable e) {
      logger.warn("Unable to cache materialization {}", descriptor.getMaterializationId(), e);
    }
    return materialization;
  }

  @Override
  public List<MaterializationDescriptor> get() {
    return get(false);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(new AutoCloseable() {
      @Override
      public void close() throws Exception {
        scheduler.shutdown();
        scheduler.awaitTermination(30, TimeUnit.SECONDS);
      }
    }, queryContext);
  }

  @Override
  public Optional<MaterializationDescriptor> get(String materializationId) {
    MaterializationDescriptor descriptor = cached.get().get(materializationId);
    if (descriptor!= null && descriptor.isComplete()) {
      return Optional.of(descriptor);
    } else {
     return Optional.absent();
    }
  }

  @Override
  public void update(MaterializationDescriptor materializationDescriptor) {
    enabled = options.getOption(MATERIALIZATION_CACHE_ENABLED);
    if (enabled && upToDate) {
      compareAndSetCache(materializationDescriptor);
    }
  }

  private List<MaterializationDescriptor> getValid() {
    return FluentIterable
   .from(cached.get().values())
   .filter(new Predicate<MaterializationDescriptor>() {
     @Override
     public boolean apply(final MaterializationDescriptor materialization) {
       return materialization.isComplete();
     }
   }).toList();
 }

  private ImmutableList<MaterializationDescriptor> getAll() {
    return ImmutableList.copyOf(cached.get().values());
  }

  @Override
  public List<MaterializationDescriptor> get(boolean includeInComplete) {
    enabled = options.getOption(MATERIALIZATION_CACHE_ENABLED);

    if (debug) {
      // in debug mode we synchronously update the cache every time.
      compareAndSetCache();
      return (includeInComplete)?getAll():getValid();
    } else if (enabled) {
      if (upToDate) {
        // when the cache is up to date (up to the last update), we should use it
        return (includeInComplete) ? getAll() : getValid();
      } else {
        // otherwise don't return anything, queries won't be accelerated but sometimes it's less problematic than all
        // queries trying to expand all valid materialization plans during planing
        return ImmutableList.of();
      }
    } else {
      // cache value is not up to date, we go straight to the provider
      return provider.get(includeInComplete);
    }
  }

  @Override
  public void remove(String materializationId) {
    enabled = options.getOption(MATERIALIZATION_CACHE_ENABLED);
    if (enabled && upToDate) {
      boolean exchanged;
      do {
        Map<String, MaterializationDescriptor> old = cached.get();
        //check if materialization is present or not
        if (!old.containsKey(materializationId)) {
          break;
        }
        //copy over everything
        Map<String, MaterializationDescriptor> updated =  Maps.newHashMap(old);
        //remove the specific materialization.
        updated.remove(materializationId);
        //update the cache.
        exchanged = cached.compareAndSet(old, updated);
      } while(!exchanged);
    }
  }
}
