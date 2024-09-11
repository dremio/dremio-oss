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
import static java.util.concurrent.TimeUnit.MINUTES;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.store.sys.accel.AccelerationManager.ExcludedReflectionsProvider;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.proto.RefreshPolicyType;
import com.dremio.service.reflection.DependencyEntry.DatasetDependency;
import com.dremio.service.reflection.DependencyEntry.ReflectionDependency;
import com.dremio.service.reflection.DependencyEntry.TableFunctionDependency;
import com.dremio.service.reflection.DependencyGraph.DependencyException;
import com.dremio.service.reflection.proto.DependencyType;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.RefreshRequest;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/** Reflection dependencies manager. This is a singleton. Only one per coordinator. */
public class DependencyManager {
  protected static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DependencyManager.class);

  private final Function<ReflectionDependency, Materialization> getLastMaterializationDoneFunc =
      new Function<ReflectionDependency, Materialization>() {
        @Nullable
        @Override
        public Materialization apply(ReflectionDependency entry) {
          return materializationStore.getLastMaterializationDone(entry.getReflectionId());
        }
      };

  private final Predicate<DependencyEntry> isDeletedReflection =
      new Predicate<DependencyEntry>() {
        @Override
        public boolean apply(DependencyEntry dependency) {
          if (dependency.getType() != DependencyType.REFLECTION) {
            return false;
          }
          final ReflectionEntry entry = entriesStore.get(new ReflectionId(dependency.getId()));
          return entry == null
              || entry.getState() == ReflectionState.FAILED
              || entry.getState() == ReflectionState.DEPRECATE;
        }
      };

  private final MaterializationStore materializationStore;
  private final ReflectionEntriesStore entriesStore;

  private final DependencyGraph graph;
  private final OptionManager optionManager;

  /*
   * MaterializationInfo cache. Caches ReflectionEntry and Materialization info that ReflectionManager.sync requires
   * when resolving information of a reflection on its dependencies.
   *
   * Note that a very important design assumption is that the dependency manager is always in-sync with the KV store.
   * Reflection manager needs to guarantee this by notifying the dependency manager of all changes to reflection entries.
   */
  private Map<ReflectionId, MaterializationInfo> materializationInfoCache;

  DependencyManager(
      MaterializationStore materializationStore,
      ReflectionEntriesStore entriesStore,
      OptionManager optionManager,
      DependencyGraph dependencyGraph) {
    this.materializationStore =
        Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.entriesStore = Preconditions.checkNotNull(entriesStore, "reflection entry store required");
    this.graph = dependencyGraph;
    this.materializationInfoCache = new HashMap<>();
    this.optionManager = optionManager;
  }

  void start() {
    graph.loadFromStore();
    entriesStore.find().forEach(this::createMaterializationInfo);
  }

  ExcludedReflectionsProvider getExcludedReflectionsProvider() {
    return new ExcludedReflectionsProvider() {
      @Override
      public List<String> getExcludedReflections(String rId) {
        Preconditions.checkNotNull(rId, "Reflection id required.");
        return DependencyManager.this.getExclusions(new ReflectionId(rId));
      }
    };
  }

  /**
   * RefreshPolicyType such as RefreshPolicyType.NEVER is used to identify a PDS or VDS reflection
   * whose PDS dependencies all have a "never refresh" policy. These reflections will never enter
   * the FAILED state and be removed from the dependency graph. This is so that users can always
   * manually refresh this reflection from a PDS. When manual reflections fail to refresh, they are
   * never automatically retried by the reflection manager by design.
   *
   * <p>refreshPolicyType should be kept up-to-date on a reflection entry because the UI depends on
   * this flag to let the user know whether his reflection is scheduled to refresh or configured to
   * never refresh.
   *
   * <p>A reflection may not have any dependencies such as when the initial refresh fails before
   * planning or the reflection selects from values. In this case, we can't figure out the refresh
   * policy on the failed reflection and keep retrying until it permanently fails.
   *
   * @param entry
   * @param dependencyResolutionContext
   */
  public void updateRefreshPolicyType(
      final ReflectionEntry entry, final DependencyResolutionContext dependencyResolutionContext) {
    Set<RefreshPolicyType> refreshPolicyTypes =
        updateRefreshPolicyHelper(
            entry.getId(),
            dependency -> {
              final AccelerationSettings settings =
                  getSettingsForDependency(dependencyResolutionContext, dependency);
              return settings.getNeverRefresh()
                  ? RefreshPolicyType.NEVER
                  : settings.getRefreshPolicyType();
            });
    List<RefreshPolicyType> refreshPolicyTypesList =
        refreshPolicyTypes.stream().sorted().collect(Collectors.toList());
    if (!refreshPolicyTypesList.equals(entry.getRefreshPolicyTypeList())) {
      entry.setRefreshPolicyTypeList(refreshPolicyTypesList);
      entriesStore.save(entry);
      updateMaterializationInfo(entry);
      logger.debug("Updated refreshPolicyType to {} for {}", refreshPolicyTypes, getId(entry));
    }
  }

  private Set<RefreshPolicyType> updateRefreshPolicyHelper(
      final ReflectionId reflectionId,
      Function<DependencyEntry, RefreshPolicyType> checkDependencyFunc) {
    final Iterable<DependencyEntry> dependencies = graph.getPredecessors(reflectionId);
    if (Iterables.isEmpty(dependencies)) {
      return Collections.emptySet();
    }
    return Stream.concat(
            filterReflectionDependencies(dependencies).stream()
                .flatMap(
                    dependency -> {
                      return updateRefreshPolicyHelper(
                          dependency.getReflectionId(), checkDependencyFunc)
                          .stream();
                    }),
            Stream.concat(
                filterDatasetDependencies(dependencies).stream().map(checkDependencyFunc),
                filterTableFunctionDependencies(dependencies).stream().map(checkDependencyFunc)))
        .collect(Collectors.toSet());
  }

  /** Updates the staleness info of given materialization. */
  public void updateStaleMaterialization(Materialization materialization) {
    try {
      final boolean isStale = isReflectionStale(materialization.getReflectionId());
      if (materialization.getIsStale() != isStale) {
        materializationStore.save((materialization.setIsStale(isStale)));
        logger.debug("Updated isStale to {} for {}", isStale, getId(materialization));
      }
    } catch (RuntimeException e) {
      logger.warn("Couldn't updateStaleMaterialization for {}", getId(materialization));
    }
  }

  /** Updates the staleness info of last DONE materialization of given reflection. */
  public void updateStaleMaterialization(ReflectionId reflectionId) {
    final Materialization lastDone = materializationStore.getLastMaterializationDone(reflectionId);
    if (lastDone != null) {
      updateStaleMaterialization(lastDone);
    }
  }

  boolean isReflectionStale(final ReflectionId reflectionId) {
    return false;
  }

  /**
   * Returns reflections that should be excluded for substitution when planning a refresh for the
   * input reflection.
   *
   * <p>This method is not called from ReflectinManager.sync so it should not use the
   * HandleEntriesSyncCache cache.
   *
   * @return FAILED and cyclic dependencies
   */
  private List<String> getExclusions(ReflectionId rId) {
    return filterReflectionDependencies(graph.getPredecessors(rId))
        .filter(
            dependency -> {
              final ReflectionEntry entry = entriesStore.get(dependency.getReflectionId());
              return entry != null && entry.getState() == ReflectionState.FAILED;
            })
        .transform(dependency -> dependency.getReflectionId())
        .append(graph.getSubGraph(rId))
        .transform(id -> id.getId())
        .toList();
  }

  boolean reflectionHasKnownDependencies(ReflectionId id) {
    return !graph.getPredecessors(id).isEmpty();
  }

  /**
   * Recursively checks if a reflection has any direct or indirect dependencies due for refresh or
   * refreshing.
   */
  boolean hasDependencyRefreshing(
      final Supplier<Long> currentTimeSupplier,
      final ReflectionId reflectionId,
      final long noDependencyRefreshPeriodMs,
      final DependencyResolutionContext dependencyResolutionContext) {
    final List<DependencyEntry> dependencies = graph.getPredecessors(reflectionId);
    final MaterializationInfo materializationInfo = getMaterializationInfo(reflectionId);
    final String reflectionName = materializationInfo.getReflectionName();
    return filterReflectionDependencies(dependencies)
        .anyMatch(
            dependency -> {
              final MaterializationInfo dependencyMaterializationInfo =
                  getMaterializationInfo(dependency.getReflectionId());
              final String dependencyReflectionName =
                  dependencyMaterializationInfo.getReflectionName();
              final ReflectionState dependencyReflectionState =
                  dependencyMaterializationInfo.getReflectionState();
              logger.trace(
                  "Reflection {}[{}], dependency {}[{}], state {}",
                  reflectionId.getId(),
                  reflectionName,
                  dependency.getReflectionId().getId(),
                  dependencyReflectionName,
                  dependencyReflectionState.name());
              return (dependencyMaterializationInfo.getReflectionState() != ReflectionState.ACTIVE)
                  || shouldRefresh(
                      currentTimeSupplier,
                      dependency.getReflectionId(),
                      noDependencyRefreshPeriodMs,
                      dependencyResolutionContext)
                  || hasDependencyRefreshing(
                      currentTimeSupplier,
                      dependency.getReflectionId(),
                      noDependencyRefreshPeriodMs,
                      dependencyResolutionContext);
            });
  }

  /**
   * Utility method to describe all possible paths from current reflection to upstream leaf nodes
   * base on dependency graph.
   *
   * <p>For example, for dependency graph like: pds1 > level1_raw1 > level2_raw1 pds2 > level1_raw2
   * > level3_raw1 > level2_raw2 pds3 > level1_raw3
   *
   * <p>Refresh dependency paths for reflection 2797cf9f-2516-47af-a0f4-82b041b4a082[level3_raw1]:
   * dataset pds1 -> 22386126-6c2f-469e-b073-53eb472862d3[level1_raw1] ->
   * 48a1ce92-2260-41e1-b735-3c95f8f75e93[level2_raw1] dataset pds2 ->
   * 4f81cc3f-7a4e-465a-9410-79e220564c5f[level1_raw2] ->
   * 48a1ce92-2260-41e1-b735-3c95f8f75e93[level2_raw1] dataset pds2 ->
   * 4f81cc3f-7a4e-465a-9410-79e220564c5f[level1_raw2] ->
   * 3b390c83-b31f-48c1-b7a9-3bd983b5739a[level2_raw2] dataset pds3 ->
   * cd20e951-e4b6-45b5-a56c-d0d8921bac3c[level1_raw3] ->
   * 3b390c83-b31f-48c1-b7a9-3bd983b5739a[level2_raw2]
   */
  String describeRefreshPaths(ReflectionId reflectionId) {
    List<List<String>> allPaths = new ArrayList<>();
    LinkedList<String> curPath = new LinkedList<>();

    describeRefreshPathsHelper(reflectionId, allPaths, curPath);
    return String.join(
        "\n",
        allPaths.stream()
            .map(
                s ->
                    s.size() == 0
                        ? s
                        : s.subList(0, s.size() - 1)) // subList to exclude current reflection
            .map(s -> String.join(" -> ", s))
            .collect(Collectors.toList()));
  }

  void describeRefreshPathsHelper(
      ReflectionId reflectionId, List<List<String>> allPaths, LinkedList<String> curPath) {
    MaterializationInfo materializationInfo = getMaterializationInfo(reflectionId);
    curPath.addFirst(
        String.format("%s[%s]", reflectionId.getId(), materializationInfo.getReflectionName()));

    final List<DependencyEntry> dependencies = graph.getPredecessors(reflectionId);
    if (dependencies.isEmpty()) {
      allPaths.add(ImmutableList.copyOf(curPath));
      curPath.removeFirst();
      return;
    }

    for (DependencyEntry dependency : dependencies) {
      if (dependency.getType() == DependencyType.REFLECTION) {
        describeRefreshPathsHelper(
            ((ReflectionDependency) dependency).getReflectionId(), allPaths, curPath);
      } else if (dependency.getType() == DependencyType.DATASET) {
        final List<String> path = dependency.getPath();
        curPath.addFirst(String.format("dataset %s", PathUtils.constructFullPath(path)));
        allPaths.add(ImmutableList.copyOf(curPath));
        curPath.removeFirst();
      } else {
        final String sourceName = ((TableFunctionDependency) dependency).getSourceName();
        curPath.addFirst(String.format("table function %s", sourceName));
        allPaths.add(ImmutableList.copyOf(curPath));
        curPath.removeFirst();
      }
    }

    curPath.removeFirst();
  }

  /**
   * Computes all reflections that will be refreshed if a refresh request is placed for given
   * reflectionId. Also calculates the batch number for each output reflection, representing the
   * depth from reflection's upstream base tables in the Dependency Graph.
   *
   * @param reflectionId
   * @return Map of reflectionId and its batch number
   */
  public ImmutableMap<ReflectionId, Integer> computeReflectionLineage(ReflectionId reflectionId) {
    return graph.computeReflectionLineage(reflectionId);
  }

  boolean shouldRefresh(
      final ReflectionEntry entry,
      final long noDependencyRefreshPeriodMs,
      final DependencyResolutionContext dependencyResolutionContext) {
    return shouldRefresh(
        System::currentTimeMillis,
        entry.getId(),
        noDependencyRefreshPeriodMs,
        dependencyResolutionContext);
  }

  boolean shouldRefresh(
      final Supplier<Long> currentTimeSupplier,
      final ReflectionId reflectionId,
      final long noDependencyRefreshPeriodMs,
      final DependencyResolutionContext dependencyResolutionContext) {
    final long currentTime = currentTimeSupplier.get();
    final MaterializationInfo materializationInfo = getMaterializationInfo(reflectionId);
    final int numFailures = Optional.ofNullable(materializationInfo.getNumFailures()).orElse(0);
    final long lastSubmitted = materializationInfo.getLastSubmittedRefresh();
    final String reflectionName = materializationInfo.getReflectionName();

    final StringBuilder traceMsg = new StringBuilder();

    // all the entries with failures will enter this block and terminate here
    if (optionManager.getOption(ReflectionOptions.BACKOFF_RETRY_POLICY) && numFailures > 0) {
      return shouldRetryAfterFailure(
          reflectionId, numFailures, lastSubmitted, currentTime, dependencyResolutionContext);
    }

    final List<DependencyEntry> dependencies = graph.getPredecessors(reflectionId);
    if (dependencies.isEmpty()) {
      return lastSubmitted + noDependencyRefreshPeriodMs < System.currentTimeMillis();
    }

    // first go through all reflection dependencies, computing last successful refresh time
    final List<Long> lastSuccessfulRefreshes =
        filterReflectionDependencies(dependencies)
            .transform(
                new Function<ReflectionDependency, Long>() {
                  @Nullable
                  @Override
                  public Long apply(ReflectionDependency dependency) {
                    final MaterializationInfo dependencyMaterializationInfo =
                        getMaterializationInfo(dependency.getReflectionId());
                    final Long lastSuccessfulRefresh =
                        dependencyMaterializationInfo.getLastSuccessfulRefresh();
                    final String dependencyReflectionName =
                        dependencyMaterializationInfo.getReflectionName();
                    traceMsg.append(
                        String.format(
                            "\nreflection %s[%s], dependency %s[%s], lastSuccessfulRefresh %d",
                            reflectionId.getId(),
                            reflectionName,
                            dependency.getReflectionId().getId(),
                            dependencyReflectionName,
                            lastSuccessfulRefresh));
                    return lastSuccessfulRefresh;
                  }
                })
            .filter(notNull())
            .toList(); // we need to apply the transform so that refreshNow gets computed

    // check if reflection is due for refresh because of one of its parent reflections
    final boolean dependsOnReflections = !lastSuccessfulRefreshes.isEmpty();
    if (dependsOnReflections) {
      final Long refreshStart =
          Preconditions.checkNotNull(
              Ordering.natural().max(lastSuccessfulRefreshes), "refreshStart cannot be null");
      if (lastSubmitted < refreshStart) {
        traceMsg.append(
            String.format(
                "\nreflection %s[%s], lastSubmitted %d < refreshStart %d",
                reflectionId.getId(), reflectionName, lastSubmitted, refreshStart));
        logger.trace(traceMsg.toString());
        return true;
      }
    }

    // Go through dataset and table function dependencies with schedule policies
    // returning true if any of the schedules dictate there should be a refresh
    if (optionManager.getOption(ReflectionOptions.REFLECTION_SCHEDULE_POLICY_ENABLED)) {
      boolean shouldRefreshFromSchedule =
          dependencies.stream()
              .filter(
                  (Predicate<DependencyEntry>)
                      entry ->
                          ((entry.getType() == DependencyType.DATASET)
                              || entry.getType() == DependencyType.TABLEFUNCTION))
              .anyMatch(
                  dependency -> {
                    AccelerationSettings settings =
                        getSettingsForDependency(dependencyResolutionContext, dependency);
                    return (settings.getRefreshPolicyType().equals(RefreshPolicyType.SCHEDULE)
                        && materializationInfo.getLastSuccessfulRefresh() != null
                        && isRefreshFromScheduleDue(
                            settings.getRefreshSchedule(),
                            materializationInfo.getLastSuccessfulRefresh(),
                            currentTime));
                  });
      if (shouldRefreshFromSchedule) {
        return true;
      }
    }

    // go through dataset dependencies and table function depedencies to compute last start refresh
    // from refresh period and refresh requests
    final Iterable<Long> refreshStarts =
        filterDatasetDependencies(dependencies)
            .transform(
                new Function<DatasetDependency, Long>() {
                  @Override
                  public Long apply(DatasetDependency dependency) {
                    // first account for the dataset's refresh period
                    final AccelerationSettings settings =
                        getSettingsForDependency(dependencyResolutionContext, dependency);
                    final long refreshStart =
                        Boolean.TRUE.equals(settings.getNeverRefresh())
                                || !settings.getRefreshPolicyType().equals(RefreshPolicyType.PERIOD)
                                || settings.getRefreshPeriod() == 0
                            ? 0
                            : currentTime - settings.getRefreshPeriod();

                    // then account for any refresh request against the dataset
                    final RefreshRequest request =
                        dependencyResolutionContext.getRefreshRequest(dependency.getId());
                    if (request != null) {
                      return Math.max(refreshStart, request.getRequestedAt());
                    }

                    return refreshStart;
                  }
                })
            .append(
                filterTableFunctionDependencies(dependencies)
                    .transform(
                        new Function<TableFunctionDependency, Long>() {
                          @Nullable
                          @Override
                          public Long apply(TableFunctionDependency entry) {
                            final AccelerationSettings settings =
                                getSettingsForDependency(dependencyResolutionContext, entry);
                            final long refreshStart =
                                Boolean.TRUE.equals(settings.getNeverRefresh())
                                        || !settings
                                            .getRefreshPolicyType()
                                            .equals(RefreshPolicyType.PERIOD)
                                        || settings.getRefreshPeriod() == 0
                                    ? 0
                                    : currentTime - settings.getRefreshPeriod();
                            return refreshStart;
                          }
                        }))
            .toList();

    final boolean dependsOnDatasets = !Iterables.isEmpty(refreshStarts);
    if (dependsOnDatasets) {
      final long refreshStart =
          Preconditions.checkNotNull(
              Ordering.natural().max(refreshStarts), "refreshStart cannot be null");
      return lastSubmitted < refreshStart;
    }

    if (!dependsOnReflections) {
      logger.warn(
          "couldn't compute a refresh time for reflection {}, will be scheduled immediately",
          reflectionId.getId());
      return true; // possible case: all dependencies have been deleted, we should try to refresh
      // right away
    }

    return false;
  }

  /**
   * Computes a reflection's oldest dependent materialization from the reflections it depends upon.
   * <br>
   * If the reflection only depends on physical datasets, returns Optional.empty()
   */
  public Optional<Long> getOldestDependentMaterialization(ReflectionId reflectionId) {
    // retrieve all the reflection entries reflectionId depends on
    final Iterable<Long> dependencies =
        filterReflectionDependencies(graph.getPredecessors(reflectionId))
            .transform(getLastMaterializationDoneFunc)
            .transform(
                new Function<Materialization, Long>() {
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
   * Find all the scan against physical datasets, and return the minimum ttl value If no physical
   * datasets
   *
   * @return the minimum ttl value
   */
  public Optional<Long> getGracePeriod(
      final ReflectionId reflectionId,
      final DependencyResolutionContext dependencyResolutionContext) {
    // extract the gracePeriod from all dataset entries
    final Iterable<Long> gracePeriods =
        filterDatasetDependencies(graph.getPredecessors(reflectionId))
            .transform(
                new Function<DatasetDependency, Long>() {
                  @Nullable
                  @Override
                  public Long apply(DatasetDependency entry) {
                    final AccelerationSettings settings =
                        getSettingsForDependency(dependencyResolutionContext, entry);
                    // for reflections that never expire, use a grace period of 1000 years from now
                    return Boolean.TRUE.equals(settings.getNeverExpire())
                        ? (TimeUnit.DAYS.toMillis(365) * 1000)
                        : settings.getGracePeriod();
                  }
                })
            .append(
                filterTableFunctionDependencies(graph.getPredecessors(reflectionId))
                    .transform(
                        new Function<TableFunctionDependency, Long>() {
                          @Nullable
                          @Override
                          public Long apply(TableFunctionDependency entry) {
                            final AccelerationSettings settings =
                                getSettingsForDependency(dependencyResolutionContext, entry);
                            return Boolean.TRUE.equals(settings.getNeverExpire())
                                ? (TimeUnit.DAYS.toMillis(365) * 1000)
                                : settings.getGracePeriod();
                          }
                        }))
            .filter(notNull());

    if (Iterables.isEmpty(gracePeriods)) {
      return Optional.empty();
    }

    return Optional.of(Ordering.natural().min(gracePeriods));
  }

  /**
   * Creates a CatalogEntityKey from a reflection dependency that can be used to retrieve the
   * entity's reflection settings.
   *
   * <p>For a dataset dependency, the dataset may be versioned in which case we extract the version
   * context from the datasetId. For an external table dependency, only the root/source name will be
   * passed in and datasetId can be ignored.
   */
  CatalogEntityKey createCatalogEntityKey(List<String> path, String datasetId) {
    final CatalogEntityKey.Builder builder = CatalogEntityKey.newBuilder().keyComponents(path);
    VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(datasetId);
    if (versionedDatasetId != null) {
      assert (path.equals(versionedDatasetId.getTableKey()));
      builder.tableVersionContext(versionedDatasetId.getVersionContext());
    }
    return builder.build();
  }

  /**
   * Retreives the AccelerationSettings for a given dependency.
   *
   * @param dependency
   * @return accelerationSettings
   */
  AccelerationSettings getSettingsForDependency(
      DependencyResolutionContext context, DependencyEntry dependency) {
    switch (dependency.getType()) {
      case DATASET:
        return context.getReflectionSettings(
            createCatalogEntityKey(dependency.getPath(), dependency.getId()));
      case TABLEFUNCTION:
        return context.getReflectionSettings(
            createCatalogEntityKey(
                ImmutableList.of(((TableFunctionDependency) dependency).getSourceName()),
                dependency.getId()));
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot get acceleration settings for dependency type %s", dependency.getType()));
    }
  }

  /**
   * The new materialization is only as fresh as the most stale input materialization. This method
   * finds which of the input materializations has the earliest expiration. The new
   * materialization's expiration must be equal to or sooner than this.
   *
   * @return the earliest expiration. if no accelerations, Long.MAX_VALUE is returned
   */
  public Optional<Long> getEarliestExpiration(final ReflectionId reflectionId) {
    // extract expiration time of all reflection entries
    final Iterable<Long> expirationTimes =
        filterReflectionDependencies(graph.getPredecessors(reflectionId))
            .transform(getLastMaterializationDoneFunc)
            .transform(
                new Function<Materialization, Long>() {
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

  public void setDependencies(final ReflectionId reflectionId, ExtractedDependencies extracted)
      throws DependencyException {
    Preconditions.checkState(!extracted.isEmpty(), "expected non empty dependencies");

    // Plan dependencies can include reflections.... decision dependencies cannot
    Set<DependencyEntry> dependencies = extracted.getPlanDependencies();

    if (Iterables.isEmpty(dependencies)) {
      // no plan dependencies found, use decision dependencies instead
      graph.setDependencies(reflectionId, extracted.getDecisionDependencies());
      return;
    }

    // When updating the dependencies for R2,
    // if P > R1 > R2 and R1 is either FAILED or DEPRECATED we should exclude it from the
    // dependencies and include
    // all datasets instead. We do this to avoid never refreshing R2 again if R1 is never refreshed.
    // Note that even though delete(R1) will also update R2 dependencies, it may not be enough if R2
    // was refreshing
    // while delete(R1) happens as setDependencies(R2, ...) would overwrite the delete(R1).
    // That is why we need to exclude R1 below and include all the physical dataset dependencies.
    if (Iterables.any(dependencies, isDeletedReflection)) {
      // filter out deleted reflections, and include all dataset dependencies
      dependencies =
          FluentIterable.from(dependencies)
              .filter(not(isDeletedReflection))
              .append(extracted.getDecisionDependencies())
              .toSet();
    }

    graph.setDependencies(reflectionId, dependencies);
  }

  synchronized void delete(ReflectionId id) {
    graph.delete(id);
  }

  /**
   * Checks reflections successors to see if they all depend on entry's latest snapshot.
   *
   * @return true if all successors depend on the latest snapshot, otherwise false.
   */
  boolean areAllSuccessorsUsingLatestSnapshot(ReflectionEntry entry) {
    long snapshotId = getMaterializationInfo(entry.getId()).getSnapshotId();
    List<ReflectionId> successors = graph.getSuccessors(entry.getId());
    // Go through the successors
    for (ReflectionId successor : successors) {
      // Find all predecessors for the successor that use the entry.
      // If any of these use a snapshot that is not the current one, return false
      if (graph.getPredecessors(successor).stream()
          .anyMatch(
              d ->
                  d.getId().equals(entry.getId().getId())
                      && ((ReflectionDependency) d).getSnapshotId() != snapshotId)) {
        return false;
      }
    }
    return true;
  }

  /** Does preliminary checks to determine if a reflection is ready to be vacuumed. */
  boolean shouldVacuum(ReflectionEntry entry) {
    if (Optional.of(getMaterializationInfo(entry.getId()))
        .map(MaterializationInfo::isVacuumed)
        .orElse(true)) {
      // if there is no isVacuumed info or the materialization was already vacuumed, don't vacuum
      return false;
    }
    if (!areAllSuccessorsUsingLatestSnapshot(entry)) {
      // if any successors are not dependent on the current snapshot, wait until they are
      logger.debug(
          "Waiting for dependants to be refreshed before vacuuming {}", entry.getId().getId());
      return false;
    }
    return true;
  }

  /** Creates MaterializationInfo cache entry with the info from given ReflectionEntry. */
  void createMaterializationInfo(ReflectionEntry reflectionEntry) {
    ReflectionId reflectionId = reflectionEntry.getId();
    ImmutableMaterializationInfo.Builder builder = MaterializationInfo.builder();
    builder
        .setReflectionName(reflectionEntry.getName())
        .setReflectionState(reflectionEntry.getState())
        .setLastSubmittedRefresh(reflectionEntry.getLastSubmittedRefresh())
        .setLastSuccessfulRefresh(reflectionEntry.getLastSuccessfulRefresh())
        .setNumFailures(reflectionEntry.getNumFailures());
    materializationInfoCache.put(reflectionId, builder.build());
  }

  /** Updates MaterializationInfo cache entry with the new info from given ReflectionEntry. */
  void updateMaterializationInfo(ReflectionEntry reflectionEntry) {
    ReflectionId reflectionId = reflectionEntry.getId();
    ImmutableMaterializationInfo.Builder builder = MaterializationInfo.builder();
    MaterializationInfo previous = getMaterializationInfo(reflectionId);
    builder.from(previous);
    builder
        .setReflectionName(reflectionEntry.getName())
        .setReflectionState(reflectionEntry.getState())
        .setLastSubmittedRefresh(reflectionEntry.getLastSubmittedRefresh())
        .setLastSuccessfulRefresh(reflectionEntry.getLastSuccessfulRefresh())
        .setNumFailures(reflectionEntry.getNumFailures());
    materializationInfoCache.put(reflectionId, builder.build());
  }

  /**
   * Updates MaterializationInfo cache entry with a new MaterializationId, snapshotId and isVacuumed
   * for the given ReflectionId.
   */
  void updateMaterializationInfo(
      ReflectionId reflectionId,
      MaterializationId materializationId,
      long snapshotId,
      boolean isVacuumed) {
    ImmutableMaterializationInfo.Builder builder = MaterializationInfo.builder();
    MaterializationInfo previous = getMaterializationInfo(reflectionId);
    builder.from(previous);
    builder
        .setMaterializationId(materializationId)
        .setSnapshotId(snapshotId)
        .setIsVacuumed(isVacuumed);
    materializationInfoCache.put(reflectionId, builder.build());
  }

  /** Deletes MaterializationInfo cache entry for given reflectionId. */
  void deleteMaterializationInfo(ReflectionId reflectionId) {
    materializationInfoCache.remove(reflectionId);
  }

  /**
   * Gets MaterializationInfo from MaterializationInfo cache for a given ReflectionId. Throws a
   * RuntimeException if ReflectionId is not found in MaterializationInfo cache.
   *
   * @return MaterializationInfo.
   */
  MaterializationInfo getMaterializationInfo(ReflectionId reflectionId) {
    MaterializationInfo materializationInfo = getMaterializationInfoUnchecked(reflectionId);
    Preconditions.checkNotNull(
        materializationInfo,
        String.format(
            "Getting MaterializationInfo: Reflection id %s not found in MaterializationInfo cache.",
            reflectionId));
    return materializationInfo;
  }

  /**
   * Gets MaterializationInfo from MaterializationInfo cache for a given ReflectionId. Returns null
   * if ReflectionId is not found in MaterializationInfo cache.
   *
   * @return MaterializationInfo.
   */
  MaterializationInfo getMaterializationInfoUnchecked(ReflectionId reflectionId) {
    return materializationInfoCache.get(reflectionId);
  }

  /**
   * Utility method to determine if a refresh is due according to a cron expression from a refresh
   * schedule. Returns true if the reflection should refresh according to the schedule, else false.
   */
  public static boolean isRefreshFromScheduleDue(
      String cronExpression, long lastSuccessfulRefresh, long currentTime) {
    Calendar lastSuccessfulRefreshCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    lastSuccessfulRefreshCalendar.setTimeInMillis(lastSuccessfulRefresh);
    Calendar currentTimeCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    currentTimeCalendar.setTimeInMillis(currentTime);

    String[] cronFields = cronExpression.split(" ");
    int cronMinute = Integer.parseInt(cronFields[1]);
    int cronHour = Integer.parseInt(cronFields[2]);
    String cronDaysOfWeekField = cronFields[5];

    List<Integer> scheduleDaysOfWeek = convertDaysOfWeek(cronDaysOfWeekField);

    // we have to potentially check up to a full week
    // start by applying the refresh time to lastSuccessfulRefresh and working forward
    // if we find a time when the current day we're checking is in the schedule, and it's
    // <= currentTime, then return true
    Calendar currentRefreshTime = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    currentRefreshTime.setTimeInMillis(lastSuccessfulRefresh);
    currentRefreshTime.set(Calendar.SECOND, 0);
    currentRefreshTime.set(Calendar.MINUTE, cronMinute);
    currentRefreshTime.set(Calendar.HOUR_OF_DAY, cronHour);
    for (int i = 0; i < 8; i++) {
      currentRefreshTime.add(Calendar.DATE, i);
      // the current day needs to be in the schedule to be considered
      if (!scheduleDaysOfWeek.contains(currentRefreshTime.get(Calendar.DAY_OF_WEEK))
          || (i == 0
              && (lastSuccessfulRefreshCalendar.after(currentRefreshTime)
                  || lastSuccessfulRefreshCalendar.equals(currentRefreshTime)))) {
        continue;
      }
      if (currentTimeCalendar.after(currentRefreshTime)
          || currentRefreshTime.equals(currentTimeCalendar)) {
        return true;
      }
    }
    return false;
  }

  public static List<Integer> convertDaysOfWeek(String cronExpression) {
    if ("*".equals(cronExpression) || "?".equals(cronExpression)) {
      // Wildcard (*) or (?) represents all days of the week
      return Arrays.asList(1, 2, 3, 4, 5, 6, 7);
    }

    String[] daysAbbreviations = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};
    Map<String, Integer> dayAbbreviationsMap = new HashMap<>();

    // Populate map for day abbreviations
    for (int i = 1; i < 8; i++) {
      dayAbbreviationsMap.put(daysAbbreviations[i - 1], i);
    }

    List<Integer> daysList = new ArrayList<>();

    // Split by comma to handle multiple values
    String[] parts = cronExpression.split(",");
    for (String part : parts) {
      if (part.contains("-")) {
        // Handle range
        String[] range = part.split("-");
        int start = getDayToIntValue(range[0], dayAbbreviationsMap);
        int end = getDayToIntValue(range[range.length - 1], dayAbbreviationsMap);
        for (int i = start; i <= end; i++) {
          daysList.add(i);
        }
      } else {
        daysList.add(getDayToIntValue(part, dayAbbreviationsMap));
      }
    }

    // Ensure the final list is sorted
    daysList.sort(null);

    return daysList;
  }

  public static int getDayToIntValue(String day, Map<String, Integer> dayAbbreviationsMap) {
    if (dayAbbreviationsMap.containsKey(day.toUpperCase())) {
      return dayAbbreviationsMap.get(day.toUpperCase());
    }
    try {
      int dayAsInt = Integer.parseInt(day);
      if (dayAsInt >= 1 && dayAsInt <= 7) {
        return dayAsInt;
      }
    } catch (NumberFormatException ignored) {
      // Ignore the exception so we can throw the one below
    }
    throw new IllegalArgumentException(
        String.format("Invalid value specified in days of week for refreshSchedule: %s", day));
  }

  DependencyGraph getGraph() {
    return graph;
  }

  OptionManager getOptionManager() {
    return optionManager;
  }

  /** Get backoff time interval in minute for refresh retry */
  private long getBackoffMinutes(final Integer numFailures) {
    long[] backoffs = {1, 2, 5, 15, 30, 60, 120, 240};
    if (numFailures < backoffs.length) {
      return backoffs[numFailures - 1];
    } else {
      return backoffs[backoffs.length - 1];
    }
  }

  /**
   * Decide whether to retry refresh or not for a reflection entry 1. check if the backoff interval
   * is met 2. check if "Refresh Now" is requested
   */
  boolean shouldRetryAfterFailure(
      final ReflectionId reflectionId,
      final int numFailures,
      final long lastSubmitted,
      final long currentTime,
      final DependencyResolutionContext dependencyResolutionContext) {
    assert numFailures > 0;
    final long backoffMinutes = getBackoffMinutes(numFailures);
    // Either immediate backoff is enabled or the backoff time interval is met, we return true
    if (!optionManager.getOption(ReflectionOptions.ENABLE_EXPONENTIAL_BACKOFF_FOR_RETRY_POLICY)
        || lastSubmitted + MINUTES.toMillis(backoffMinutes) <= currentTime) {
      logger.info(
          "Retry refresh due for {}, which has failed {} times. Current time is {},"
              + " last submitted refresh is {}, backoff is {} mins",
          ReflectionUtils.getId(reflectionId),
          numFailures,
          Instant.ofEpochMilli(currentTime),
          Instant.ofEpochMilli(lastSubmitted),
          backoffMinutes);
      return true;
    }

    return dependencyGraphDeepSearchHelper(
        reflectionId,
        dependency -> {
          final RefreshRequest request =
              dependencyResolutionContext.getRefreshRequest(dependency.getId());
          if (request != null && lastSubmitted < request.getRequestedAt()) {
            logger.info(
                "Retry refresh for {} because of Refresh Now,"
                    + " which has failed {} times. Current time is {},"
                    + " last submitted refresh is {}, refresh is requested at {}",
                ReflectionUtils.getId(reflectionId),
                numFailures,
                Instant.ofEpochMilli(currentTime),
                Instant.ofEpochMilli(lastSubmitted),
                Instant.ofEpochMilli(request.getRequestedAt()));
            return true;
          }
          return false;
        });
  }

  /**
   * A helper function to do dfs on dependency graph. At each level we call the helper function
   * itself again for each reflection dependency predecessor until there is no dependencies or at
   * least one dataset dependency (leaf node) is found with true returned by checkDependencyFunc.
   *
   * @param reflectionId Reflection ID that specifies the starting point of dfs
   * @param checkDependencyFunc A function defined by caller to return boolean result, which only
   *     applies to dataset dependencies.
   * @return return true if any of the checkDependencyFunc results is true. Otherwise, return false.
   */
  boolean dependencyGraphDeepSearchHelper(
      final ReflectionId reflectionId, Function<DependencyEntry, Boolean> checkDependencyFunc) {
    final Iterable<DependencyEntry> dependencies = getGraph().getPredecessors(reflectionId);
    if (Iterables.isEmpty(dependencies)) {
      return false;
    }
    return filterReflectionDependencies(dependencies).stream()
            .anyMatch(
                dependency ->
                    dependencyGraphDeepSearchHelper(
                        dependency.getReflectionId(), checkDependencyFunc))
        || filterDatasetDependencies(dependencies).stream().anyMatch(checkDependencyFunc::apply);
  }
}
