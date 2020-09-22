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
import static com.dremio.service.reflection.proto.MaterializationState.FAILED;
import static com.dremio.service.reflection.proto.ReflectionState.ACTIVE;
import static com.dremio.service.reflection.proto.ReflectionState.REFRESHING;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.Objects;
import java.util.concurrent.Future;

import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsService;
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
import com.google.common.base.Preconditions;
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
  private final JobsService jobsService;
  private final MaterializationStore materializationStore;
  private final long delay;
  private final long maxWait;

  public ReflectionMonitor(ReflectionService reflections, ReflectionStatusService statusService,
                           MaterializationDescriptorProvider materializations, JobsService jobsService,
                           MaterializationStore materializationStore, long delay, long maxWait) {
    this.reflections = reflections;
    this.statusService = statusService;
    this.materializations = materializations;
    this.jobsService = jobsService;
    this.materializationStore = materializationStore;
    this.delay = delay;
    this.maxWait = maxWait;
  }

  public ReflectionMonitor withWait(long maxWait) {
    return new ReflectionMonitor(reflections, statusService, materializations, jobsService, materializationStore, delay, maxWait);
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
    return waitUntilMaterialized(id, null);
  }

  /**
   * Wait until any materialization is done after a specific materialization
   * @param reflectionId    reflection id
   * @param materialization   the specific materialization
   * @return    the materialization which is done after the specific materialization
   */
  public Materialization waitUntilMaterialized(final ReflectionId reflectionId, final Materialization materialization) {
    final MaterializationId materializationId = (materialization == null) ? null : materialization.getId();
    Wait w = new Wait();
    while (w.loop()) {
      // Get the last materialization done and return it if it's done after the specific materialization
      final Materialization lastMaterializationDone = materializationStore.getLastMaterializationDone(reflectionId);
      if (lastMaterializationDone != null && !Objects.equals(materializationId, lastMaterializationDone.getId())
        && (materialization == null || lastMaterializationDone.getInitRefreshSubmit() > materialization.getInitRefreshSubmit())) {
        return lastMaterializationDone;
      }
      // Throws materialization fail error if there is a failed materialization after the specific materialization
      final Materialization lastMaterializationFailed = materializationStore.getLastMaterializationFailed(reflectionId);
      if (lastMaterializationFailed != null && !Objects.equals(materializationId, lastMaterializationFailed.getId())
        && (materialization == null || lastMaterializationFailed.getInitRefreshSubmit() > materialization.getInitRefreshSubmit())) {
        throwMaterializationError(lastMaterializationFailed);
      }
    }

    throw new IllegalStateException();
  }

  /**
   * Throws a runtime exception for a failed materialization with its error message
   * @param failedMaterialization   failed materialization
   */
  private void throwMaterializationError(final Materialization failedMaterialization) {
    Preconditions.checkArgument(failedMaterialization.getState() == FAILED, "materialization did not fail");

    final QueryProfileRequest request = QueryProfileRequest.newBuilder()
      .setJobId(JobProtobuf.JobId.newBuilder()
        .setId(failedMaterialization.getInitRefreshJobId())
        .build())
      .setUserName(SYSTEM_USERNAME)
      .build();

    try {
      final QueryProfile queryProfile = jobsService.getProfile(request);
      if (queryProfile.getState() == QueryResult.QueryState.FAILED) {
        throw new RuntimeException("Materialization failed: " + queryProfile.getError());
      } else {
        throw new RuntimeException(String.format("Refresh job completed, but materialization failed with %s.", failedMaterialization.getFailure().getMessage()));
      }
    } catch (JobNotFoundException e) {
      throw new RuntimeException("Failed to get refresh job profile after materialization failed.");
    }
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

  /**
   * wait for the first materialization of a reflection to be refreshing
   * @param id    reflection id
   * @return    the running materialization
   */
  public Materialization waitUntilMaterializationRunning(final ReflectionId id) {
    return waitUntilMaterializationRunning(id, (MaterializationId) null);
  }

  /**
   * wait for the next materialization of a reflection to be refreshing
   * @param id    reflection id
   * @param m   previous materialization of the reflection
   * @return    the running materialization
   */
  public Materialization waitUntilMaterializationRunning(final ReflectionId id, Materialization m) {
    return waitUntilMaterializationRunning(id, m.getId());
  }

  /**
   * wait for the next materialization of a reflection to be refreshing
   *
   * **Note:** Given that RUNNING is an intermediate state, unit tests that use this need to be carefully written.
   * Otherwise we may see random failures if the materialization completes before the monitor got the chance to notice
   * it was in running state.
   *
   * @param id    reflection id
   * @param lastMaterializationId   previous materialization id of the reflection
   * @return    the running materialization
   */
  public Materialization waitUntilMaterializationRunning(final ReflectionId id, MaterializationId lastMaterializationId) {
    Wait w = new Wait();
    while (w.loop()) {
      final Materialization lastMaterialization = materializationStore.getLastMaterialization(id);
      if (lastMaterialization != null &&
        !Objects.equals(lastMaterializationId, lastMaterialization.getId()) &&
        lastMaterialization.getState() == MaterializationState.RUNNING) {
        return lastMaterialization;
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

  /**
   * wait for the first materialization of a reflection to be canceled
   * @param id    reflection id
   * @return    the canceled materialization
   */
  public Materialization waitUntilMaterializationCanceled(final ReflectionId id) {
    return waitUntilMaterializationCanceled(id, (MaterializationId) null);
  }

  /**
   * wait for the next materialization of a reflection to be canceled
   * @param id    reflection id
   * @param m   previous materialization of the reflection
   * @return    the canceled materialization
   */
  public Materialization waitUntilMaterializationCanceled(final ReflectionId id, Materialization m) {
    return waitUntilMaterializationCanceled(id, m.getId());
  }

  /**
   * wait for the next materialization of a reflection to be canceled
   * @param id    reflection id
   * @param lastMaterializationId   previous materialization id of the reflection
   * @return    the canceled materialization
   */
  public Materialization waitUntilMaterializationCanceled(final ReflectionId id, MaterializationId lastMaterializationId) {
    Wait w = new Wait();
    while (w.loop()) {
      final Materialization lastMaterialization = materializationStore.getLastMaterialization(id);
      if (lastMaterialization != null &&
        !Objects.equals(lastMaterializationId, lastMaterialization.getId()) &&
        lastMaterialization.getState() == MaterializationState.CANCELED) {
        return lastMaterialization;
      }
    }

    throw new IllegalStateException();
  }

  public Materialization waitUntilMaterializationFinished(final ReflectionId id, Materialization m) {
    return waitUntilMaterializationFinished(id, m != null ? m.getId() : null);
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
      if (materializations.get().stream()
        .filter(m -> m.getReflectionType() != ReflectionType.EXTERNAL)
        .noneMatch(m -> {
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
