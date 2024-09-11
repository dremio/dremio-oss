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

import static com.dremio.service.job.JobState.METADATA_RETRIEVAL;
import static com.dremio.service.job.JobState.PENDING;
import static com.dremio.service.job.JobState.PLANNING;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionStatus.AVAILABILITY_STATUS.AVAILABLE;
import static com.dremio.service.reflection.ReflectionUtils.isTerminal;
import static com.dremio.service.reflection.proto.MaterializationState.DEPRECATED;
import static com.dremio.service.reflection.proto.MaterializationState.DONE;
import static com.dremio.service.reflection.proto.MaterializationState.FAILED;
import static com.dremio.service.reflection.proto.ReflectionState.ACTIVE;
import static com.dremio.service.reflection.proto.ReflectionState.REFRESHING;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.common.VM;
import com.dremio.exec.planner.acceleration.descriptor.MaterializationDescriptor;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.exec.proto.UserBitShared.ReflectionType;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.options.OptionManager;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.reflection.MaterializationCache.CacheViewer;
import com.dremio.service.reflection.ReflectionStatus.AVAILABILITY_STATUS;
import com.dremio.service.reflection.ReflectionStatus.REFRESH_STATUS;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Monitors current status of reflections. */
public class ReflectionMonitor {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ReflectionMonitor.class);

  private final ReflectionService reflections;
  private final ReflectionStatusService statusService;
  private final MaterializationDescriptorProvider materializations;
  private final JobsService jobsService;
  private final MaterializationStore materializationStore;
  private final OptionManager optionManager;
  private final long delay;
  private final long maxWait;

  public ReflectionMonitor(
      ReflectionService reflections,
      ReflectionStatusService statusService,
      MaterializationDescriptorProvider materializations,
      JobsService jobsService,
      MaterializationStore materializationStore,
      OptionManager optionManager,
      long delay,
      long maxWait) {
    this.reflections = reflections;
    this.statusService = statusService;
    this.materializations = materializations;
    this.jobsService = jobsService;
    this.materializationStore = materializationStore;
    this.optionManager = optionManager;
    this.delay = delay;
    this.maxWait = maxWait;
  }

  public ReflectionMonitor withWait(long maxWait) {
    return new ReflectionMonitor(
        reflections,
        statusService,
        materializations,
        jobsService,
        materializationStore,
        optionManager,
        delay,
        maxWait);
  }

  public void waitUntilRefreshed(final ReflectionId reflectionId) {
    waitForState(reflectionId, REFRESHING);
    waitForState(reflectionId, ACTIVE);
  }

  public ReflectionEntry waitForState(
      final ReflectionId reflectionId, final ReflectionState state) {
    Optional<ReflectionEntry> reflection;
    Wait w = new Wait();
    String status = String.format("Waiting for %s state. Current state is UNKNOWN", state);
    while (w.loop(status)) {
      reflection = reflections.getEntry(reflectionId);
      if (reflection.isPresent()) {
        ReflectionState currentState = reflection.get().getState();
        status =
            String.format(
                "Reflection %s waiting for %s state. Current state is %s",
                reflection.get().getState(), state, currentState);
        logger.debug(status);
        if (currentState == state) {
          w.log("waitForState " + ReflectionUtils.getId(reflectionId) + "=" + state);
          return reflection.get();
        }
      } else {
        status = "reflection not available";
        logger.debug(status);
      }
    }

    throw new IllegalStateException();
  }

  public void waitForRefreshStatus(
      final ReflectionId reflectionId, final ReflectionStatus.REFRESH_STATUS status) {
    Wait w = new Wait();
    String progressMessage = String.format("Waiting for %s. Current status is UNKNOWN", status);
    while (w.loop(progressMessage)) {
      REFRESH_STATUS current = statusService.getReflectionStatus(reflectionId).getRefreshStatus();
      if (current == status) {
        w.log("waitForRefreshStatus " + ReflectionUtils.getId(reflectionId) + "=" + status);
        return;
      }
      progressMessage = String.format("Waiting for %s. Current status is %s", status, current);
    }

    throw new IllegalStateException();
  }

  public void waitUntilPlanned(JobId id) throws JobNotFoundException {
    Wait w = new Wait();
    String status =
        String.format("Waiting for job %s to finish planning. Current status is UNKNOWN", id);
    while (w.loop(status)) {
      JobSummary summary =
          jobsService.getJobSummary(
              JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(id)).build());
      // Logic based on LocalJobService.MapFilterToJobState
      JobState current = summary.getJobState();
      status = String.format("Waiting for job %s to complete. Current status is %s", id, current);
      if (!Arrays.asList(PENDING, METADATA_RETRIEVAL, PLANNING).contains(current)) {
        w.log(status);
        return;
      }
    }
  }

  public Materialization waitUntilMaterialized(ReflectionId id) {
    return waitUntilMaterialized(id, null);
  }

  /**
   * Wait until any materialization is done after a specific materialization
   *
   * @param reflectionId reflection id
   * @param materialization the specific materialization
   * @return the materialization which is done after the specific materialization
   */
  public Materialization waitUntilMaterialized(
      final ReflectionId reflectionId, final Materialization materialization) {
    final MaterializationId materializationId =
        (materialization == null) ? null : materialization.getId();
    Wait w = new Wait();
    String status =
        String.format(
            "Waiting for reflection %s (predecessor materialization %s) to materialize. Current state is UNKNOWN",
            reflectionId, materializationId);
    while (w.loop(status)) {
      // Get the last materialization done and return it if it's done after the specific
      // materialization.
      // Throw materialization fail error if there is a failed materialization after the specific
      // materialization.
      final Materialization lastMaterialization =
          materializationStore.getLastMaterialization(reflectionId);
      if (lastMaterialization != null
          && !Objects.equals(materializationId, lastMaterialization.getId())
          && (materialization == null
              || lastMaterialization.getInitRefreshSubmit()
                  > materialization.getInitRefreshSubmit())) {
        MaterializationState current = lastMaterialization.getState();
        if (current == DONE) {
          w.log(
              "waitUntilMaterialized "
                  + ReflectionUtils.getId(reflectionId)
                  + " and not "
                  + (materialization == null ? "any" : ReflectionUtils.getId(materialization)));

          return lastMaterialization;
        } else if (lastMaterialization.getState() == FAILED) {
          throwMaterializationError(lastMaterialization);
        }
        status =
            String.format(
                "Waiting for reflection %s (predecessor materialization %s) to materialize. Current state is %s",
                reflectionId, materializationId, current);
      }
    }

    throw new IllegalStateException();
  }

  /**
   * Wait until any materialization is done and cached after a specific materialization
   *
   * <p>Use it to avoid the case when reflection entry is active but materialization cache is still
   * pending (DX-88983)
   *
   * @param reflectionId reflection id
   * @param materialization the specific materialization
   * @return the materialization which is done after the specific materialization
   */
  public Materialization waitUntilMaterializedAndCached(
      final ReflectionId reflectionId, final Materialization materialization) {
    final MaterializationId materializationId =
        (materialization == null) ? null : materialization.getId();
    Wait w = new Wait();
    String status =
        String.format(
            "Waiting for materialization %s to materialize. Current state is UNKNOWN",
            materializationId);
    while (w.loop(status)) {
      // Get the last materialization done and return it if it's done after the specific
      // materialization.
      // Throw materialization fail error if there is a failed materialization after the specific
      // materialization.
      final Materialization lastMaterialization =
          materializationStore.getLastMaterialization(reflectionId);
      if (lastMaterialization != null
          && !Objects.equals(materializationId, lastMaterialization.getId())
          && (materialization == null
              || lastMaterialization.getInitRefreshSubmit()
                  > materialization.getInitRefreshSubmit())) {
        MaterializationState current = lastMaterialization.getState();
        if (current == DONE) {
          w.log(
              "waitUntilMaterialized "
                  + ReflectionUtils.getId(reflectionId)
                  + " and not "
                  + (materialization == null ? "any" : ReflectionUtils.getId(materialization)));

          // check if materialization cache is ready to avoid test flakiness caused by the case when
          // reflection entry is active but materialization cache is still pending (DX-88983)
          if (optionManager.getOption(MATERIALIZATION_CACHE_ENABLED)
              && lastMaterialization != null) {
            waitUntilCached(lastMaterialization);
          }
          return lastMaterialization;
        } else if (lastMaterialization.getState() == FAILED) {
          throwMaterializationError(lastMaterialization);
        }
        status =
            String.format(
                "Waiting for materialization %s to materialize. Current state is %s",
                materializationId, current);
      }
    }

    throw new IllegalStateException();
  }

  public void waitTillReflectionManagerHasCycled() {
    ReflectionManager reflectionManager = reflections.getReflectionManager().get();
    long last = reflectionManager.getLastWakeupTime();
    Wait w = new Wait();
    // We need to wait till 2 refresh cycles have completed to ensure we were not in the middle of
    // one
    boolean cycled = false;
    String statusMessage;
    do {
      long latest = reflectionManager.getLastWakeupTime();
      statusMessage =
          String.format(
              "Waiting for reflection manager to have cycled. Previous wakeup time was %d. "
                  + "Most recent wakeup time is %d",
              last, latest);
      if (last < latest) {
        if (cycled) {
          w.log("waitTillReflectionManagerHasCycled");
          return;
        } else {
          cycled = true;
        }
      } else {
        reflections.wakeupManager("Testing");
      }
    } while (w.loop(statusMessage));
    throw new IllegalStateException();
  }

  /**
   * Throws a runtime exception for a failed materialization with its error message
   *
   * @param failedMaterialization failed materialization
   */
  private void throwMaterializationError(final Materialization failedMaterialization) {
    Preconditions.checkArgument(
        failedMaterialization.getState() == FAILED, "materialization did not fail");

    final QueryProfileRequest request =
        QueryProfileRequest.newBuilder()
            .setJobId(
                JobProtobuf.JobId.newBuilder()
                    .setId(failedMaterialization.getInitRefreshJobId())
                    .build())
            .setUserName(SYSTEM_USERNAME)
            .build();

    try {
      final QueryProfile queryProfile = jobsService.getProfile(request);
      if (queryProfile.getState() == QueryResult.QueryState.FAILED) {
        throw new RuntimeException("Materialization failed: " + queryProfile.getError());
      } else {
        throw new RuntimeException(
            String.format(
                "Refresh job completed, but materialization failed with %s.",
                failedMaterialization.getFailure().getMessage()));
      }
    } catch (JobNotFoundException e) {
      throw new RuntimeException("Failed to get refresh job profile after materialization failed.");
    }
  }

  public void waitUntilCached(Materialization m) {
    Wait w = new Wait();
    final CacheViewer cacheViewer = reflections.getCacheViewerProvider().get();
    String status = String.format("waitUntilCached for %s", ReflectionUtils.getId(m));
    while (w.loop(status)) {
      w.log(
          extendWithReflectionsAndMaterializations(
              String.format("waitUntilCached loading for %s", ReflectionUtils.getId(m))));
      if (cacheViewer.isCached(m.getId())) {
        status = String.format("waitUntilCached done for %s", ReflectionUtils.getId(m));
        w.log(extendWithReflectionsAndMaterializations(status));
        return;
      }
    }
    throw new IllegalStateException();
  }

  public void waitUntilMaterializationDescriptorStale(Materialization m) {
    Wait w = new Wait();
    String status =
        String.format("waitUntilMaterializationDescriptorStale for %s", ReflectionUtils.getId(m));
    while (w.loop(status)) {
      w.log(
          extendWithReflectionsAndMaterializations(
              String.format(
                  "waitUntilMaterializationDescriptorStale loading for %s",
                  ReflectionUtils.getId(m))));
      Optional<MaterializationDescriptor> descriptor =
          materializations.get().stream()
              .filter(input -> input.getLayoutId().equals(m.getReflectionId().getId()))
              .findFirst();
      if (descriptor.isPresent() && descriptor.get().isStale()) {
        status =
            String.format(
                "waitUntilMaterializationDescriptorStale done for %s", ReflectionUtils.getId(m));
        w.log(extendWithReflectionsAndMaterializations(status));
        return;
      }
    }
    throw new IllegalStateException();
  }

  /**
   * wait for the first materialization of a reflection to be refreshing
   *
   * @param id reflection id
   * @return the running materialization
   */
  public Materialization waitUntilMaterializationRunning(final ReflectionId id) {
    return waitUntilMaterializationRunning(id, (MaterializationId) null);
  }

  /**
   * wait for the next materialization of a reflection to be refreshing
   *
   * @param id reflection id
   * @param m previous materialization of the reflection
   * @return the running materialization
   */
  public Materialization waitUntilMaterializationRunning(final ReflectionId id, Materialization m) {
    return waitUntilMaterializationRunning(id, m.getId());
  }

  /**
   * wait for the next materialization of a reflection to be refreshing
   *
   * <p>**Note:** Given that RUNNING is an intermediate state, unit tests that use this need to be
   * carefully written. Otherwise we may see random failures if the materialization completes before
   * the monitor got the chance to notice it was in running state.
   *
   * @param id reflection id
   * @param lastMaterializationId previous materialization id of the reflection
   * @return the running materialization
   */
  public Materialization waitUntilMaterializationRunning(
      final ReflectionId id, MaterializationId lastMaterializationId) {
    Wait w = new Wait();
    String status = "Waiting until last Materialization is running";
    while (w.loop(status)) {
      final Materialization lastMaterialization = materializationStore.getLastMaterialization(id);
      if (lastMaterialization != null) {
        if (!Objects.equals(lastMaterializationId, lastMaterialization.getId())
            && lastMaterialization.getState() == MaterializationState.RUNNING) {
          w.log(
              "waitUntilMaterializationRunning "
                  + ReflectionUtils.getId(id)
                  + " and not "
                  + ReflectionUtils.getId(lastMaterialization));
          return lastMaterialization;
        } else {
          status =
              String.format(
                  "State of the last materialization %s is %s",
                  ReflectionUtils.getId(lastMaterialization), lastMaterialization.getState());
        }
      }
    }

    throw new IllegalStateException();
  }

  public Materialization waitUntilMaterializationFails(final ReflectionId id) {
    return waitUntilMaterializationFails(id, (MaterializationId) null, null);
  }

  public Materialization waitUntilMaterializationFails(final ReflectionId id, Materialization m) {
    return waitUntilMaterializationFails(id, m.getId());
  }

  public Materialization waitUntilMaterializationFails(
      final ReflectionId id, MaterializationId lastMaterializationId) {
    return waitUntilMaterializationFails(id, lastMaterializationId, null);
  }

  public Materialization waitUntilMaterializationFails(
      final ReflectionId id,
      MaterializationId lastMaterializationId,
      final Long maxWaitUntilFailed) {
    Wait w = new Wait(maxWaitUntilFailed);
    String status = "Waiting until last Materialization is running";
    while (w.loop(status)) {
      final Materialization lastMaterialization = materializationStore.getLastMaterialization(id);
      if (lastMaterialization != null) {
        if (!Objects.equals(lastMaterializationId, lastMaterialization.getId())
            && lastMaterialization.getState() == MaterializationState.FAILED) {
          w.log(
              "waitUntilMaterializationFails "
                  + ReflectionUtils.getId(id)
                  + " and not "
                  + ReflectionUtils.getId(lastMaterialization));
          return lastMaterialization;
        } else {
          status =
              String.format(
                  "Latest state of the last materialization %s is %s",
                  ReflectionUtils.getId(lastMaterialization), lastMaterialization.getState());
        }
      }
    }

    throw new IllegalStateException();
  }

  /**
   * wait for the first materialization of a reflection to be canceled
   *
   * @param id reflection id
   * @return the canceled materialization
   */
  public Materialization waitUntilMaterializationCanceled(final ReflectionId id) {
    return waitUntilMaterializationCanceled(id, (MaterializationId) null);
  }

  /**
   * wait for the next materialization of a reflection to be canceled
   *
   * @param id reflection id
   * @param m previous materialization of the reflection
   * @return the canceled materialization
   */
  public Materialization waitUntilMaterializationCanceled(
      final ReflectionId id, Materialization m) {
    return waitUntilMaterializationCanceled(id, m.getId());
  }

  /**
   * wait for the next materialization of a reflection to be canceled
   *
   * @param id reflection id
   * @param lastMaterializationId previous materialization id of the reflection
   * @return the canceled materialization
   */
  public Materialization waitUntilMaterializationCanceled(
      final ReflectionId id, MaterializationId lastMaterializationId) {
    Wait w = new Wait();
    String status = "Waiting until last Materialization is running";
    while (w.loop(status)) {
      final Materialization lastMaterialization = materializationStore.getLastMaterialization(id);
      if (lastMaterialization != null) {
        if (!Objects.equals(lastMaterializationId, lastMaterialization.getId())
            && lastMaterialization.getState() == MaterializationState.CANCELED) {
          w.log(
              "waitUntilMaterializationCanceled "
                  + ReflectionUtils.getId(id)
                  + " and not "
                  + ReflectionUtils.getId(lastMaterialization));
          return lastMaterialization;
        } else {
          status =
              String.format(
                  "Latest state of the last materialization %s is %s",
                  ReflectionUtils.getId(lastMaterialization), lastMaterialization.getState());
        }
      }
    }

    throw new IllegalStateException();
  }

  public Materialization waitUntilMaterializationFinished(
      final ReflectionId id, Materialization m) {
    return waitUntilMaterializationFinished(id, m != null ? m.getId() : null);
  }

  public Materialization waitUntilMaterializationFinished(
      final ReflectionId id, MaterializationId lastMaterializationId) {
    Wait w = new Wait();
    String status = "Waiting until last Materialization is finished";
    while (w.loop(status)) {
      final Materialization lastMaterialization = materializationStore.getLastMaterialization(id);
      if (lastMaterialization != null) {
        if (!Objects.equals(lastMaterializationId, lastMaterialization.getId())
            && isTerminal(lastMaterialization.getState())) {
          w.log(
              "waitUntilMaterializationFinished "
                  + ReflectionUtils.getId(id)
                  + " and not "
                  + ReflectionUtils.getId(lastMaterialization));
          return lastMaterialization;
        } else {
          status =
              String.format(
                  "Latest state of the last materialization %s is %s",
                  ReflectionUtils.getId(lastMaterialization), lastMaterialization.getState());
        }
      }
    }
    throw new IllegalStateException();
  }

  /**
   * Wait for the reflection giving up after all the materialization reties fail or/and cancel
   *
   * <p>Note that the threshold is specified by support key LAYOUT_REFRESH_MAX_ATTEMPTS
   *
   * @param id reflection id
   * @param m predecessor materialization
   * @param maxWaitUntilFailed maximum waiting time until one materialization fails
   * @param maxWaitUntilGivenUp maximum waiting time until getting the given up state
   * @return
   */
  public void waitUntilReflectionGivesUp(
      final ReflectionId id,
      final Materialization m,
      final Long maxWaitUntilFailed,
      final Long maxWaitUntilGivenUp) {
    Wait w = new Wait(maxWaitUntilGivenUp);
    String status = "Waiting until reflection gives up";
    int iterations = 0;
    Materialization pre = m;

    while (w.loop(status)) {
      try {
        pre =
            waitUntilMaterializationFails(id, pre != null ? pre.getId() : null, maxWaitUntilFailed);
        iterations++;
        w.log(
            "waitUntilMaterializationFails for reflection "
                + ReflectionUtils.getId(id)
                + " and the id of the failed materialization is "
                + ReflectionUtils.getId(pre));
      } catch (ReflectionMonitor.TimeoutException e) {
        // stop retrying
        // when the number of failures is equal to the value of LAYOUT_REFRESH_MAX_ATTEMPTS
        w.log("waitUntilReflectionGivesUp times out after " + iterations + " attempts");
        break;
      } catch (Exception other) {
        w.log("Unexpected error in waitUntilReflectionGivesUp: " + other);
        throwMaterializationError(pre);
      }
    }
  }

  public void waitUntilCanAccelerate(final ReflectionId reflectionId) {
    Wait w = new Wait();
    String message =
        String.format("Waiting until reflection %s can accelerate", reflectionId.getId());
    while (w.loop(message)) {
      AVAILABILITY_STATUS status =
          statusService.getReflectionStatus(reflectionId).getAvailabilityStatus();
      if (status == AVAILABLE) {
        w.log("waitUntilCanAccelerate " + ReflectionUtils.getId(reflectionId));
        return;
      } else {
        message =
            String.format("Latest status of reflection %s was %s", reflectionId.getId(), status);
      }
    }

    throw new IllegalStateException();
  }

  public void waitUntilNoMaterializationsAvailable() {
    Wait w = new Wait();
    String status = "Waiting for no materializations to be available";
    while (w.loop(status)) {
      if (materializations.get().isEmpty()) {
        w.log("waitUntilNoMaterializationsAvailable");
        return;
      } else {
        status =
            String.format(
                "Materializations %s are still available",
                materializations.get().stream()
                    .map(MaterializationDescriptor::getMaterializationId)
                    .reduce((x, y) -> x + "," + y));
      }
    }

    throw new IllegalStateException();
  }

  public void waitUntilNoMoreRefreshing(long requestTime, long numMaterializations) {
    Wait w = new Wait();
    String status = "Waiting until no reflections are refreshing";
    while (w.loop(status)) {
      Future<?> future = reflections.wakeupManager("start refresh");
      try {
        future.get();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
      List<MaterializationDescriptor> materializationDescriptorList = materializations.get();
      if ((materializationDescriptorList.size() == numMaterializations)
          && materializationDescriptorList.stream()
              .filter(m -> m.getReflectionType() != ReflectionType.EXTERNAL)
              .noneMatch(
                  m -> {
                    Optional<ReflectionEntry> e =
                        reflections.getEntry(new ReflectionId(m.getLayoutId()));
                    long lastSuccessful = e.get().getLastSuccessfulRefresh();
                    return e.map(
                            r ->
                                (r.getState() == REFRESHING
                                    || ((lastSuccessful != 0L) && (lastSuccessful < requestTime))))
                        .orElse(false);
                  })) {
        w.log("waitUntilNoMoreRefreshing numMaterializations=" + numMaterializations);
        break;
      }
    }
  }

  private final class Wait {
    private final long expire;
    private int loop = 0;
    private long start = System.currentTimeMillis();

    public Wait() {
      this(null);
    }

    public Wait(final Long newMaxWait) {
      if (newMaxWait == null) {
        this.expire = System.currentTimeMillis() + maxWait;
      } else {
        this.expire = System.currentTimeMillis() + newMaxWait;
      }
    }

    public boolean loop(String message) {
      loop++;
      if (loop == 1) {
        return true;
      }

      if (System.currentTimeMillis() > expire && !VM.isDebugEnabled()) {
        throw new TimeoutException(extendWithReflectionsAndMaterializations(message));
      }
      try {
        Thread.sleep(delay);
      } catch (InterruptedException ex) {
        throw new RuntimeException(extendWithReflectionsAndMaterializations(message), ex);
      }
      return true;
    }

    public void log(String waitReason) {
      logger.debug(
          "Waited for {} took {} s",
          waitReason,
          TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
    }
  }

  public void waitUntilExternalReflectionsRemoved(String externalReflectionId) {
    Wait wait = new Wait();
    String status =
        String.format("Waiting until external reflection %s to be removed", externalReflectionId);
    while (wait.loop(status)) {
      final Optional<ExternalReflection> entry =
          reflections.getExternalReflectionById(externalReflectionId);
      if (!entry.isPresent()) {
        wait.log(
            "waitUntilExternalReflectionsRemoved externalReflectionId=" + externalReflectionId);
        return;
      }
    }
  }

  public void waitUntilRemoved(final ReflectionId reflectionId) {
    Wait wait = new Wait();
    String id = ReflectionUtils.getId(reflectionId);
    String status = String.format("Waiting for reflection ID %s to get deleted", id);
    while (wait.loop(status)) {
      final Optional<ReflectionEntry> entry = reflections.getEntry(reflectionId);
      if (!entry.isPresent()) {
        wait.log(id);
        return;
      }
    }
  }

  public void waitUntilDeleted(final Materialization deleteMe) {
    Wait wait = new Wait();
    String status =
        String.format(
            "Waiting for the reflection %s to get deleted", ReflectionUtils.getId(deleteMe));
    while (wait.loop(status)) {
      final Materialization m = materializationStore.get(deleteMe.getId());
      if (m == null) {
        wait.log("waitUntilDeleted " + ReflectionUtils.getId(deleteMe));
        return;
      }
    }
  }

  public void waitUntilDeprecated(Materialization m) {
    Wait w = new Wait();
    String status =
        String.format(
            "Waiting for materialization %s to get deprecated. Latest state is %s",
            ReflectionUtils.getId(m), materializationStore.get(m.getId()).getState());
    while (w.loop(status)) {
      status =
          String.format(
              "Waiting for materialization %s to get deprecated. Latest state is %s",
              ReflectionUtils.getId(m), materializationStore.get(m.getId()).getState());
      if (materializationStore.get(m.getId()).getState() == DEPRECATED) {
        w.log("waitUntilDeprecated " + ReflectionUtils.getId(m));
        return;
      }
    }
    throw new IllegalStateException();
  }

  private String extendWithReflectionsAndMaterializations(String original) {
    StringBuilder finalMessage = new StringBuilder(original + "\n\n");

    finalMessage.append("The existing reflections are: \n");
    for (ReflectionGoal goal : reflections.getAllReflections()) {
      finalMessage.append(reflections.getEntry(goal.getId()).orElse(null) + "\n");
    }

    finalMessage.append("The existing materializations are: \n");
    for (Materialization m : materializationStore.getAllMaterializations()) {
      finalMessage.append(m + "\n");
    }

    finalMessage.append("The existing materialization descriptors are: \n");
    for (MaterializationDescriptor d : materializations.get()) {
      finalMessage.append(d + "\n");
    }

    return finalMessage.toString();
  }

  /** Thrown when {@link Wait} times out */
  public static class TimeoutException extends RuntimeException {
    TimeoutException(String message) {
      super(message);
    }
  }
}
