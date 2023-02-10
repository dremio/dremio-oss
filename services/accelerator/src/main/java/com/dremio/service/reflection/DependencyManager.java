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

import static com.dremio.service.reflection.DependencyUtils.filterDatasetDependencies;
import static com.dremio.service.reflection.DependencyUtils.filterReflectionDependencies;
import static com.dremio.service.reflection.DependencyUtils.filterTableFunctionDependencies;
import static com.dremio.service.reflection.ReflectionUtils.getId;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.notNull;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.dremio.exec.store.sys.accel.AccelerationManager.ExcludedReflectionsProvider;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.reflection.DependencyEntry.DatasetDependency;
import com.dremio.service.reflection.DependencyEntry.ReflectionDependency;
import com.dremio.service.reflection.DependencyEntry.TableFunctionDependency;
import com.dremio.service.reflection.DependencyGraph.DependencyException;
import com.dremio.service.reflection.proto.DependencyType;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.RefreshRequest;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
/**
 * Reflection dependencies manager.  This is a singleton.  Only one per coordinator.
 */
public class DependencyManager {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DependencyManager.class);

  private final Function<ReflectionDependency, Materialization> getLastMaterializationDoneFunc =
    new Function<ReflectionDependency, Materialization>() {
      @Nullable
      @Override
      public Materialization apply(ReflectionDependency entry) {
        return materializationStore.getLastMaterializationDone(entry.getReflectionId());
      }
    };

  private final Predicate<DependencyEntry> isDeletedReflection = new Predicate<DependencyEntry>() {
    @Override
    public boolean apply(DependencyEntry dependency) {
      if (dependency.getType() != DependencyType.REFLECTION) {
        return false;
      }
      final ReflectionEntry entry = entriesStore.get(new ReflectionId(dependency.getId()));
      return entry == null || entry.getState() == ReflectionState.FAILED || entry.getState() == ReflectionState.DEPRECATE;
    }
  };

  private final MaterializationStore materializationStore;
  private final ReflectionEntriesStore entriesStore;

  private final DependencyGraph graph;
  DependencyManager(MaterializationStore materializationStore, ReflectionEntriesStore entriesStore,
                    DependenciesStore dependenciesStore) {
    this.materializationStore = Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.entriesStore = Preconditions.checkNotNull(entriesStore, "reflection entry store required");
    this.graph = new DependencyGraph(dependenciesStore);
  }

  void start() {
    graph.loadFromStore();
  }

  ExcludedReflectionsProvider getExcludedReflectionsProvider() {
    return new ExcludedReflectionsProvider() {
      @Override
      public List<String> getExcludedReflections(String rId) {
        Preconditions.checkNotNull(rId, "Reflection id required.");
        return DependencyManager.this.getExclusions(new ReflectionId(rId));
      }};
  }

  /**
   * dontGiveUp is used to identify a PDS or VDS reflection whose PDS dependencies all have a "never refresh"
   * policy.  These reflections will never enter the FAILED state and be removed from the dependency graph.
   * This is so that users can always manually refresh this reflection from a PDS.  When manual reflections
   * fail to refresh, they are never automatically retried by the reflection manager by design.
   *
   * dontGiveUp should be kept up-to-date on a reflection entry because the UI depends on this flag to let the user
   * know whether his reflection is scheduled to refresh or configured to never refresh.
   *
   * A reflection may not have any dependencies such as when the initial refresh fails before planning or the
   * reflection selects from values.  In this case, we can't figure out the refresh policy on the failed reflection
   * and keep retrying until it permanently fails.
   *
   * @param entry
   * @param dependencyResolutionContext
   */
  public void updateDontGiveUp(final ReflectionEntry entry, final DependencyResolutionContext dependencyResolutionContext) {
    boolean dontGiveUp = dontGiveUpHelper(entry.getId(), dependencyResolutionContext);
    if (entry.getDontGiveUp() != dontGiveUp) {
      entry.setDontGiveUp(dontGiveUp);
      entriesStore.save(entry);
      logger.debug("Updated dontGiveUp to {} for {}", dontGiveUp, getId(entry));
    }
  }

  private boolean dontGiveUpHelper(final ReflectionId reflectionId, final DependencyResolutionContext dependencyResolutionContext) {
    final Iterable<DependencyEntry> dependencies = graph.getPredecessors(reflectionId);
    if (Iterables.isEmpty(dependencies)) {
      return false;
    }
    return filterReflectionDependencies(dependencies).allMatch(dependency -> {
      return dontGiveUpHelper(dependency.getReflectionId(), dependencyResolutionContext);
    }) && filterDatasetDependencies(dependencies).allMatch(dependency -> {
      final AccelerationSettings settings = dependencyResolutionContext.getReflectionSettings(dependency.getNamespaceKey());
      return Boolean.TRUE.equals(settings.getNeverRefresh());
    }) && filterTableFunctionDependencies(dependencies).allMatch(dependency -> {
      final AccelerationSettings settings = dependencyResolutionContext.getReflectionSettings(new NamespaceKey(dependency.getSourceName()));
      return Boolean.TRUE.equals(settings.getNeverRefresh());
    });
  }

  /**
   * Returns reflections that should be excluded for substitution when planning a refresh
   * for the input reflection.
   *
   * This method is not called from ReflectinManager.sync so it should not use the
   * HandleEntriesSyncCache cache.
   *
   * @return FAILED and cyclic dependencies
   */
  private List<String> getExclusions(ReflectionId rId) {
    return filterReflectionDependencies(graph.getPredecessors(rId))
      .filter(dependency -> {
        final ReflectionEntry entry = entriesStore.get(dependency.getReflectionId());
        return entry != null && entry.getState() == ReflectionState.FAILED;
      })
      .transform(dependency -> dependency.getReflectionId())
      .append(graph.getSubGraph(rId))
      .transform(id -> id.getId()).toList();
  }

  boolean reflectionHasKnownDependencies(ReflectionId id) {
    return !graph.getPredecessors(id).isEmpty();
  }

  boolean shouldRefresh(final ReflectionEntry entry, final long noDependencyRefreshPeriodMs,
                        final DependencyResolutionContext dependencyResolutionContext) {
    final long currentTime = System.currentTimeMillis();
    final ReflectionId id = entry.getId();
    final long lastSubmitted = Preconditions.checkNotNull(entry.getLastSubmittedRefresh(),
      "trying to check if reflection %s should be refreshed but it has not last_submitted_refresh field", id.getId());

    final List<DependencyEntry> dependencies = graph.getPredecessors(id);
    if (dependencies.isEmpty()) {
      return lastSubmitted + noDependencyRefreshPeriodMs < System.currentTimeMillis();
    }

    // first go through all reflection dependencies, computing last successful refresh time
    final List<Long> lastSuccessfulRefreshes = filterReflectionDependencies(dependencies)
      .transform(new Function<ReflectionDependency, Long>() {
        @Nullable
        @Override
        public Long apply(ReflectionDependency dependency) {
          final Optional<Long> lastSuccessfulRefresh = Preconditions.checkNotNull(dependencyResolutionContext.getLastSuccessfulRefresh(dependency.getReflectionId()),
            "Reflection %s depends on a non-existing reflection %s", id.getId(), dependency.getReflectionId().getId());
          return lastSuccessfulRefresh.orElse(null);
        }
      }).filter(notNull())
      .toList(); // we need to apply the transform so that refreshNow gets computed

    // check if reflection is due for refresh because of one of its parent reflections
    final boolean dependsOnReflections = !lastSuccessfulRefreshes.isEmpty();
    if (dependsOnReflections) {
      final Long refreshStart = Preconditions.checkNotNull(Ordering.natural().max(lastSuccessfulRefreshes),
        "refreshStart cannot be null");
      if (lastSubmitted < refreshStart) {
        return true;
      }
    }

    // go through dataset dependencies and table function depedencies to compute last start refresh
    // from refresh period and refresh requests
    final Iterable<Long> refreshStarts = filterDatasetDependencies(dependencies)
      .transform(new Function<DatasetDependency, Long>() {
        @Override
        public Long apply(DatasetDependency dependency) {
          // first account for the dataset's refresh period
          final AccelerationSettings settings = dependencyResolutionContext.getReflectionSettings(dependency.getNamespaceKey());
          final long refreshStart = Boolean.TRUE.equals(settings.getNeverRefresh()) || settings.getRefreshPeriod() == 0 ? 0 : currentTime - settings.getRefreshPeriod();

          // then account for any refresh request against the dataset
          final RefreshRequest request = dependencyResolutionContext.getRefreshRequest(dependency.getId());
          if (request != null) {
            return Math.max(refreshStart, request.getRequestedAt());
          }

          return refreshStart;
        }
      })
      .append(filterTableFunctionDependencies(dependencies)
        .transform(new Function<TableFunctionDependency, Long>() {
          @Nullable
          @Override
          public Long apply(TableFunctionDependency entry) {
            final AccelerationSettings settings = dependencyResolutionContext.getReflectionSettings(new NamespaceKey(entry.getSourceName()));
            final long refreshStart = Boolean.TRUE.equals(settings.getNeverRefresh()) || settings.getRefreshPeriod() == 0 ? 0 : currentTime -  settings.getRefreshPeriod();
            return refreshStart;
          }
        }))
      .toList();

    final boolean dependsOnDatasets = !Iterables.isEmpty(refreshStarts);
    if (dependsOnDatasets) {
      final long refreshStart = Preconditions.checkNotNull(Ordering.natural().max(refreshStarts),
        "refreshStart cannot be null");
      return lastSubmitted < refreshStart;
    }

    if (!dependsOnReflections) {
      logger.warn("couldn't compute a refresh time for reflection {}, will be scheduled immediately", id.getId());
      return true; // possible case: all dependencies have been deleted, we should try to refresh right away
    }

    return false;
  }

  /**
   * Computes a reflection's oldest dependent materialization from the reflections it depends upon.<br>
   * If the reflection only depends on physical datasets, returns Optional.empty()
   */
  public Optional<Long> getOldestDependentMaterialization(ReflectionId reflectionId) {
    // retrieve all the reflection entries reflectionId depends on
    final Iterable<Long> dependencies = filterReflectionDependencies(graph.getPredecessors(reflectionId))
      .transform(getLastMaterializationDoneFunc)
      .transform(new Function<Materialization, Long>() {
        @Nullable
        @Override
        public Long apply(@Nullable Materialization m) {
          return m != null ? m.getLastRefreshFromPds() : null;
        }
      })
      .filter(notNull());

    if (Iterables.isEmpty(dependencies)) {
      return Optional.empty();
    }

    return Optional.of(Ordering.natural().min(dependencies));
  }

  /**
   * Find all the scan against physical datasets, and return the minimum ttl value
   * If no physical datasets
   * @return the minimum ttl value
   */
  public Optional<Long> getGracePeriod(final ReflectionId reflectionId, final DependencyResolutionContext dependencyResolutionContext) {
    // extract the gracePeriod from all dataset entries
    final Iterable<Long> gracePeriods = filterDatasetDependencies(graph.getPredecessors(reflectionId))
      .transform(new Function<DatasetDependency, Long>() {
        @Nullable
        @Override
        public Long apply(DatasetDependency entry) {
          final AccelerationSettings settings = dependencyResolutionContext.getReflectionSettings(entry.getNamespaceKey());
          // for reflections that never expire, use a grace period of 1000 years from now
          return Boolean.TRUE.equals(settings.getNeverExpire()) ? (TimeUnit.DAYS.toMillis(365)*1000) : settings.getGracePeriod();
        }
      })
      .append(filterTableFunctionDependencies(graph.getPredecessors(reflectionId))
      .transform(new Function<TableFunctionDependency, Long>() {
        @Nullable
        @Override
        public Long apply(TableFunctionDependency entry) {
          final AccelerationSettings settings = dependencyResolutionContext.getReflectionSettings(new NamespaceKey(entry.getSourceName()));
          return Boolean.TRUE.equals(settings.getNeverExpire()) ? (TimeUnit.DAYS.toMillis(365)*1000) : settings.getGracePeriod();
        }
      }))
      .filter(notNull()
      );


    if (Iterables.isEmpty(gracePeriods)) {
      return Optional.empty();
    }

    return Optional.of(Ordering.natural().min(gracePeriods));
  }

  /**
   * The new materialization is only as fresh as the most stale input materialization. This method finds which of the
   * input materializations has the earliest expiration. The new materialization's expiration must be equal to or sooner
   * than this.
   * @return the earliest expiration. if no accelerations, Long.MAX_VALUE is returned
   */
  public Optional<Long> getEarliestExpiration(final ReflectionId reflectionId) {
    // extract expiration time of all reflection entries
    final Iterable<Long> expirationTimes = filterReflectionDependencies(graph.getPredecessors(reflectionId))
      .transform(getLastMaterializationDoneFunc)
      .transform(new Function<Materialization, Long>() {
        @Nullable
        @Override
        public Long apply(@Nullable Materialization m) {
          return m != null ? m.getExpiration() : null;
        }
      })
      .filter(notNull());

    if (Iterables.isEmpty(expirationTimes)) {
      return Optional.empty();
    }

    return Optional.of(Ordering.natural().min(expirationTimes));
  }

  List<DependencyEntry> getDependencies(final ReflectionId reflectionId) {
    return graph.getPredecessors(reflectionId);
  }

  public void setDependencies(final ReflectionId reflectionId, ExtractedDependencies extracted) throws DependencyException {
    Preconditions.checkState(!extracted.isEmpty(), "expected non empty dependencies");

    // Plan dependencies can include reflections.... decision dependencies cannot
    Set<DependencyEntry> dependencies = extracted.getPlanDependencies();

    if (Iterables.isEmpty(dependencies)) {
      // no plan dependencies found, use decision dependencies instead
      graph.setDependencies(reflectionId, extracted.getDecisionDependencies());
      return;
    }

    // When updating the dependencies for R2,
    // if P > R1 > R2 and R1 is either FAILED or DEPRECATED we should exclude it from the dependencies and include
    // all datasets instead. We do this to avoid never refreshing R2 again if R1 is never refreshed.
    // Note that even though delete(R1) will also update R2 dependencies, it may not be enough if R2 was refreshing
    // while delete(R1) happens as setDependencies(R2, ...) would overwrite the delete(R1).
    // That is why we need to exclude R1 below and include all the physical dataset dependencies.
    if (Iterables.any(dependencies, isDeletedReflection)) {
      // filter out deleted reflections, and include all dataset dependencies
      dependencies = FluentIterable.from(dependencies)
        .filter(not(isDeletedReflection))
        .append(extracted.getDecisionDependencies())
        .toSet();
    }

    graph.setDependencies(reflectionId, dependencies);
  }

  synchronized void delete(ReflectionId id) {
    graph.delete(id);
  }

}
