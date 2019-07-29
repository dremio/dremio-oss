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

import static com.dremio.service.reflection.ReflectionStatus.AVAILABILITY_STATUS.AVAILABLE;
import static com.dremio.service.reflection.ReflectionUtils.isTerminal;
import static com.dremio.service.reflection.proto.MaterializationState.DEPRECATED;
import static com.dremio.service.reflection.proto.ReflectionState.ACTIVE;
import static com.dremio.service.reflection.proto.ReflectionState.REFRESHING;

import java.util.Objects;
import java.util.concurrent.Future;

import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.service.reflection.MaterializationCache.CacheViewer;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;

/**
 * Monitors current status of reflections.
 */
public class ReflectionMonitor {

  private static final boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionMonitor.class);

  private final ReflectionService reflections;
  private final ReflectionStatusService statusService;
  private final MaterializationDescriptorProvider materializations;
  private final MaterializationStore materializationStore;
  private final long delay;
  private final long maxWait;

  public ReflectionMonitor(ReflectionService reflections, ReflectionStatusService statusService,
                           MaterializationDescriptorProvider materializations,
                           MaterializationStore materializationStore, long delay, long maxWait) {
    this.reflections = reflections;
    this.statusService = statusService;
    this.materializations = materializations;
    this.materializationStore = materializationStore;
    this.delay = delay;
    this.maxWait = maxWait;
  }

  public ReflectionMonitor withWait(long maxWait) {
    return new ReflectionMonitor(reflections, statusService, materializations, materializationStore, delay, maxWait);
  }

  public void waitUntilRefreshed(final ReflectionId reflectionId) {
    waitForState(reflectionId, REFRESHING);
    waitForState(reflectionId, ACTIVE);
  }

  public ReflectionEntry waitForState(final ReflectionId reflectionId, final ReflectionState state) {
    Optional<ReflectionEntry> reflection;
    Wait w = new Wait();
    while (w.loop()) {
      reflection = reflections.getEntry(reflectionId);
      if (reflection.isPresent()) {
        logger.debug("reflection {} is {}", reflection.get().getName(), reflection.get().getState());
        if(reflection.get().getState() == state) {
          return reflection.get();
        }

      } else {
        logger.debug("reflection not available");
      }
    }

    throw new IllegalStateException();
  }

  public Materialization waitUntilMaterialized(ReflectionId id) {
    return waitUntilMaterialized(id, (MaterializationId) null);
  }

  public Materialization waitUntilMaterialized(ReflectionId id, Materialization m) {
    return waitUntilMaterialized(id, m != null ? m.getId() : null);
  }

  public Materialization waitUntilMaterialized(final ReflectionId reflectionId, MaterializationId lastMaterializationId) {
    Wait w = new Wait();
    while (w.loop()) {
      final Materialization lastMaterialization = materializationStore.getLastMaterializationDone(reflectionId);
      if (lastMaterialization != null && !Objects.equals(lastMaterializationId, lastMaterialization.getId())) {
        return lastMaterialization;
      }
    }

    throw new IllegalStateException();
  }

  public void waitUntilCached(Materialization m) {
    waitUntilCached(m.getId());
  }

  public void waitUntilCached(MaterializationId id) {
    Wait w = new Wait();
    final CacheViewer cacheViewer = reflections.getCacheViewerProvider().get();
    while (w.loop()) {
      if (cacheViewer.isCached(id)) {
        return;
      }
    }

    throw new IllegalStateException();
  }


  public Materialization waitUntilMaterializationFails(final ReflectionId id) {
    return waitUntilMaterializationFails(id, (MaterializationId) null);
  }

  public Materialization waitUntilMaterializationFails(final ReflectionId id, Materialization m) {
    return waitUntilMaterializationFails(id, m.getId());
  }

  public Materialization waitUntilMaterializationFails(final ReflectionId id, MaterializationId lastMaterializationId) {
    Wait w = new Wait();
    while (w.loop()) {
      final Materialization lastMaterialization = materializationStore.getLastMaterialization(id);
      if (lastMaterialization != null &&
        !Objects.equals(lastMaterializationId, lastMaterialization.getId()) &&
        lastMaterialization.getState() == MaterializationState.FAILED) {
        return lastMaterialization;
      }
    }

    throw new IllegalStateException();
  }

  public Materialization waitUntilMaterializationFinished(final ReflectionId id, MaterializationId lastMaterializationId) {
    Wait w = new Wait();
    while (w.loop()) {
      final Materialization lastMaterialization = materializationStore.getLastMaterialization(id);
      if (lastMaterialization != null &&
          !Objects.equals(lastMaterializationId, lastMaterialization.getId()) &&
          isTerminal(lastMaterialization.getState())) {
        return lastMaterialization;
      }
    }

    throw new IllegalStateException();
  }

  public void waitUntilCanAccelerate(final ReflectionId reflectionId) {
    Wait w = new Wait();
    while(w.loop()) {
      if (statusService.getReflectionStatus(reflectionId).getAvailabilityStatus() == AVAILABLE) {
        return;
      }
    }

    throw new IllegalStateException();
  }

  public void waitUntilNoMaterializationsAvailable() {
    Wait w = new Wait();
    while (w.loop()) {
      if (materializations.get().isEmpty()) {
        return;
      }
    }

    throw new IllegalStateException();
  }

  public void waitUntilNoMoreRefreshing(long requestTime) {
    Wait w = new Wait();
    while (w.loop()) {
      Future<?> future = reflections.wakeupManager("start refresh");
      try {
        future.get();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
      if (materializations.get().stream().noneMatch(m -> {
        Optional<ReflectionEntry> e = reflections.getEntry(new ReflectionId(m.getLayoutId()));
        long lastSuccessful = e.get().getLastSuccessfulRefresh();
        return e.transform(r -> (r.getState() == REFRESHING || ((lastSuccessful != 0L) && (lastSuccessful < requestTime)))).or(false);
      })) {
        break;
      }
    }
  }

  private class Wait {
    private final long expire = System.currentTimeMillis() + maxWait;
    private int loop = 0;

    public boolean loop() {
      loop++;
      if(loop == 1) {
        return true;
      }

      if(System.currentTimeMillis() > expire && !IS_DEBUG) {
        throw new TimeoutException();
      }
      try {
        Thread.sleep(delay);
      }catch(InterruptedException ex) {
        throw Throwables.propagate(ex);
      }
      return true;
    }
  }

  public void waitUntilExternalReflectionsRemoved(String externalReflectionId) {
    Wait wait = new Wait();
    while (wait.loop()) {
      final Optional<ExternalReflection> entry = reflections.getExternalReflectionById(externalReflectionId);
      if (!entry.isPresent()) {
        return;
      }
    }
  }

  public void waitUntilRemoved(final ReflectionId reflectionId) {
    Wait wait = new Wait();
    while (wait.loop()) {
      final Optional<ReflectionEntry> entry = reflections.getEntry(reflectionId);
      if (!entry.isPresent()) {
        return;
      }
    }
  }

  public void waitUntilDeleted(final MaterializationId materializationId) {
    Wait wait = new Wait();
    while (wait.loop()) {
      final Materialization m = materializationStore.get(materializationId);
      if (m == null) {
        return;
      }
    }
  }

  public void waitUntilDeprecated(Materialization m) {
    waitUntilDeprecated(m.getId());
  }

  public void waitUntilDeprecated(MaterializationId id) {
    Wait w = new Wait();
    while (w.loop()) {
      if (materializationStore.get(id).getState() == DEPRECATED) {
        return;
      }
    }

    throw new IllegalStateException();
  }

  /**
   * Thrown when {@link Wait} times out
   */
  public static class TimeoutException extends RuntimeException {
    TimeoutException() {
      super("Maximum wait for event was exceeded.");
    }
  }
}
