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

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers.KryoDeserializationException;
import com.dremio.exec.planner.sql.CachedMaterializationDescriptor;
import com.dremio.exec.planner.sql.DremioRelOptMaterialization;
import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.work.AttemptId;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.accelerator.proto.LayoutId;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Cached implementation. Tries to reuse as much of the existing cache as possible when refreshing
 */
public class CachedMaterializationProvider implements MaterializationDescriptorProvider, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CachedMaterializationProvider.class);

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private volatile Map<String, MaterializationDescriptor> cached = ImmutableMap.of();

  private final MaterializationDescriptorProvider provider;
  private final OptionManager options; // we need this to check if the cache is enabled or not
  /** dummy QueryContext used to create the SqlConverter, must be closed or we'll leak a ChildAllocator */
  private final QueryContext queryContext;
  private final AccelerationService accel;
  private final StoragePluginRegistry storageRegistry;

  /** when false, the loading cache will be bypassed and the refreshes don't expand the materializations */
  private volatile boolean enabled;
  /** true if the update thread successfully updated the cache.<br>
   *  if the last cache update failed or the cache was just enabled and the cache wasn't updated yet, this value will be false */
  private volatile boolean upToDate;

  // special mode where the cache update is done synchronously whenever get() is called
  // mainly needed for testing as it will exercise both cache updating and rel nodes copying
  private boolean debug;

  public CachedMaterializationProvider(MaterializationDescriptorProvider provider,
                                       Provider<SabotContext> contextProvider, AccelerationService accel) {
    this.provider = Preconditions.checkNotNull(provider, "provider is required");
    this.accel = Preconditions.checkNotNull(accel, "acceleration service is required");
    Preconditions.checkNotNull(contextProvider, "context is required");

    final SabotContext context = contextProvider.get();
    storageRegistry = context.getStorage();

    this.options = context.getOptionManager();

    final UserSession session = systemSession(options);

    this.queryContext = new QueryContext(session, context, new AttemptId().toQueryId());

    final long refreshDelay = options.getOption(MATERIALIZATION_CACHE_REFRESH_DURATION);

    enabled = options.getOption(MATERIALIZATION_CACHE_ENABLED);

    initUpdater(refreshDelay);
  }

  private UserSession systemSession(OptionManager options) {
    final UserBitShared.UserCredentials credentials = UserBitShared.UserCredentials.newBuilder()
      .setUserName(SYSTEM_USERNAME)
      .build();
    return UserSession.Builder.newBuilder()
      .withCredentials(credentials)
      .withOptionManager(options)
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
        // no need to update the cache when it's disabled, get() will go straight to the provider anyway
        if (enabled) {
          try {
            cached = updateCache(cached);
            upToDate = true;
          } catch (Throwable e) {
            upToDate = false;
            logger.warn("Materialization cache update failed, cache will be disabled until a future update is successful", e);
          }
        }
      }
    }, 0, refreshDelayInSeconds, TimeUnit.SECONDS);
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
    final List<MaterializationDescriptor> provided = provider.get();
    // this will hold the updated cache
    final Map<String, MaterializationDescriptor> updated = Maps.newHashMap();

    // cache is enabled so we want to reuse as much of the existing cache as possible. Make sure to:
    // remove all cached descriptors that no longer exist
    // reuse all descriptors that are already in the cache
    // add any descriptor that are not already cached
    for (MaterializationDescriptor descriptor : provided) {
      final MaterializationDescriptor cached = old.get(descriptor.getMaterializationId());
      if (cached == null || !Objects.equals(cached.getUpdateId(), descriptor.getUpdateId())) {
        // it's a new descriptor, expand and add it to the cache
        try {
          // get a new converter for each materialization. This ensures that we
          // always index flattens from zero. This is a partial fix for flatten
          // matching. We should really do a better job in matching.
          SqlConverter converter = SqlConverter.dummy(queryContext, storageRegistry);
          final DremioRelOptMaterialization materialization = descriptor.getMaterializationFor(converter);
          if (materialization != null) {
            updated.put(descriptor.getMaterializationId(), new CachedMaterializationDescriptor(descriptor, materialization));
          }
        } catch (KryoDeserializationException e) {
          //DX-7858: we should only replan once in a single cache update
          logger.warn("Need to replan. MaterializationId = {}, LayoutId = {}",
              descriptor.getMaterializationId(), descriptor.getLayoutId(), e);
          accel.replan(new LayoutId(descriptor.getLayoutId()));
        } catch (Throwable e) {
          logger.warn("Unable to cache materialization {}", descriptor.getMaterializationId(), e);
        }
      } else {
        // descriptor already in the cache, we can just reuse it
        updated.put(descriptor.getMaterializationId(), cached);
      }
    }

    return updated;
  }

  @Override
  public List<MaterializationDescriptor> get() {
    enabled = options.getOption(MATERIALIZATION_CACHE_ENABLED);

    if (debug) {
      // in debug mode we synchronously update the cache every time.
      cached = updateCache(cached);
      return ImmutableList.copyOf(cached.values());
    } else if (enabled && upToDate) {
      // when the cache is up to date (up to the last update), we should use it
      return ImmutableList.copyOf(cached.values());
    } else {
      // cache value is not up to date, we go straight to the provider
      return provider.get();
    }
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
    return Optional.fromNullable(cached.get(materializationId));
  }
}
