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

import com.dremio.service.reflection.DependencyEntry.DatasetDependency;
import com.dremio.service.reflection.DependencyEntry.ReflectionDependency;
import com.dremio.service.reflection.proto.DependencyType;
import com.dremio.service.reflection.proto.ReflectionDependencies;
import com.dremio.service.reflection.proto.ReflectionDependencyEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.store.DependenciesStore;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents dependencies between reflections and datasets.
 *
 * <p>It's a directed graph where edge A -> B means B depends on A. B is always a reflection, A can
 * be either a reflection or a pds
 */
public class DependencyGraph {
  protected static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DependencyGraph.class);

  private final DependenciesStore dependenciesStore;

  private final SetMultimap<ReflectionId, DependencyEntry> predecessors =
      MultimapBuilder.hashKeys().hashSetValues().build();
  // note that we don't store successors of datasets in successors map
  private final SetMultimap<ReflectionId, ReflectionId> successors =
      MultimapBuilder.hashKeys().hashSetValues().build();
  // store successors of datasets in datasetSuccessors map
  private final SetMultimap<String, ReflectionId> datasetSuccessors =
      MultimapBuilder.hashKeys().hashSetValues().build();

  DependencyGraph(DependenciesStore dependenciesStore) {
    this.dependenciesStore =
        Preconditions.checkNotNull(dependenciesStore, "dependencies store required");
  }

  public synchronized void loadFromStore() {
    int total = 0;
    int noDependencies = 0;
    int errors = 0;
    for (Map.Entry<ReflectionId, ReflectionDependencies> entry : dependenciesStore.getAll()) {
      total++;
      final List<ReflectionDependencyEntry> dependencies = entry.getValue().getEntryList();
      if (dependencies == null || dependencies.isEmpty()) {
        noDependencies++;
        continue;
      }

      try {
        setPredecessors(
            entry.getKey(),
            FluentIterable.from(dependencies)
                .transform(
                    new Function<ReflectionDependencyEntry, DependencyEntry>() {
                      @Override
                      public DependencyEntry apply(ReflectionDependencyEntry entry) {
                        return DependencyEntry.of(entry);
                      }
                    })
                .toSet());
      } catch (DependencyException e) {
        // this should never happen as we don't allow saving cyclic dependencies in the in-memory
        // graph
        logger.warn(
            "Found a cyclic dependency while loading dependencies for {}, skipping",
            entry.getKey().getId(),
            e);
        errors++;
      }
    }
    logger.info(
        "Loaded reflection dependency graph: totalReflections={},noDependencyReflections={},dependencyExceptions={}",
        total,
        noDependencies,
        errors);
  }

  synchronized List<DependencyEntry> getPredecessors(final ReflectionId reflectionId) {
    return ImmutableList.copyOf(predecessors.get(reflectionId));
  }

  synchronized Set<DatasetDependency> getAllDatasetDependencies() {
    return ImmutableSet.copyOf(
        this.predecessors.values().stream()
            .filter(entry -> entry.getType() == DependencyType.DATASET)
            .map(entry -> ((DatasetDependency) entry))
            .collect(Collectors.toSet()));
  }

  synchronized List<ReflectionId> getSuccessors(final ReflectionId reflectionId) {
    return ImmutableList.copyOf(successors.get(reflectionId));
  }

  synchronized List<ReflectionId> getDatasetSuccessors(final String datasetId) {
    return ImmutableList.copyOf(datasetSuccessors.get(datasetId));
  }

  synchronized Set<ReflectionId> getSubGraph(final ReflectionId reflectionId) {
    Set<ReflectionId> subGraph = Sets.newHashSet();
    final Queue<ReflectionId> queue = new ArrayDeque<>();
    queue.add(reflectionId);

    while (!queue.isEmpty()) {
      final ReflectionId current = queue.remove();
      if (subGraph.add(current)) {
        queue.addAll(successors.get(current));
      }
    }

    return subGraph;
  }

  synchronized void setPredecessors(
      final ReflectionId reflectionId, Set<DependencyEntry> dependencies)
      throws DependencyException {
    // make sure we are not causing any cyclic dependency.
    // if reflectionId depends on reflectionId' and reflectionId' is in reflectionId sub-graph
    final Set<ReflectionId> subgraph = getSubGraph(reflectionId);
    for (DependencyEntry entry : dependencies) {
      if (entry.getType() == DependencyType.REFLECTION
          && subgraph.contains(new ReflectionId(entry.getId()))) {
        throw new DependencyException(
            String.format(
                "Cyclic dependency detected between %s and %s",
                reflectionId.getId(), entry.getId()));
      }
    }

    Set<DependencyEntry> previousPredecessors =
        this.predecessors.replaceValues(reflectionId, dependencies);

    final Set<DependencyEntry> removed = Sets.difference(previousPredecessors, dependencies);
    for (DependencyEntry entry : removed) {
      if (entry.getType() == DependencyType.REFLECTION) {
        successors.remove(((ReflectionDependency) entry).getReflectionId(), reflectionId);
      } else if (entry.getType() == DependencyType.DATASET) {
        datasetSuccessors.remove(entry.getId(), reflectionId);
      }
    }
    final Set<DependencyEntry> added = Sets.difference(dependencies, previousPredecessors);
    for (DependencyEntry entry : added) {
      if (entry.getType() == DependencyType.REFLECTION) {
        successors.put(((ReflectionDependency) entry).getReflectionId(), reflectionId);
      } else if (entry.getType() == DependencyType.DATASET) {
        datasetSuccessors.put(entry.getId(), reflectionId);
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          DependencyUtils.describeDependencies(reflectionId, predecessors.get(reflectionId)));
    }
  }

  public synchronized void setDependencies(
      final ReflectionId reflectionId, Set<DependencyEntry> dependencies)
      throws DependencyException {
    setPredecessors(reflectionId, dependencies);
    dependenciesStore.save(reflectionId, dependencies);
  }

  public synchronized void delete(ReflectionId id) {
    // for each pair (R, R') such as R depends on id and id depends on R', R now depends on R'
    final Set<DependencyEntry> ps = predecessors.get(id);
    for (ReflectionId successor : successors.get(id)) {
      // Remove the specified reflection from the successor's set of predecessors.
      // NB: This logic relies on the fact that we do not allow reflections to depend on the same
      // reflection multiple
      //     times with different snapshots, otherwise the same reflection ID could appear multiple
      // times in the set.
      Optional<DependencyEntry> predecessor =
          predecessors.get(successor).stream().filter(p -> id.getId().equals(p.getId())).findAny();
      predecessor.ifPresent(p -> predecessors.remove(successor, p));

      // Update successor to depend on all of id's predecessors.
      predecessors.putAll(successor, ps);
      // can't use "ps", as it may not be enough
      dependenciesStore.save(successor, predecessors.get(successor));
    }

    // kvstore and predecessors are updated correctly, need to update successors in-memory
    final Set<ReflectionId> sc = successors.get(id);
    for (DependencyEntry predecessor : predecessors.get(id)) {
      if (predecessor.getType() == DependencyType.REFLECTION) {
        // successor no longer depends on id
        successors.remove(((ReflectionDependency) predecessor).getReflectionId(), id);
        // successor depends on all id predecessors
        successors.putAll(((ReflectionDependency) predecessor).getReflectionId(), sc);
      } else if (predecessor.getType() == DependencyType.DATASET) {
        datasetSuccessors.remove(predecessor.getId(), id);
        datasetSuccessors.putAll(predecessor.getId(), sc);
      }
    }

    successors.removeAll(id);
    predecessors.removeAll(id);
    dependenciesStore.delete(id);
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
    Set<ReflectionId> bottomReflections = Sets.newHashSet();
    Map<ReflectionId, Integer> batchNumbers = Maps.newHashMap();
    Queue<ReflectionId> queue = new ArrayDeque<>();

    // Get the successors of all base tables (reflections that directly depend on base tables).
    queue.add(reflectionId);
    while (!queue.isEmpty()) {
      final ReflectionId current = queue.remove();
      List<DependencyEntry> dependencies = getPredecessors(current);
      for (DependencyEntry dependency : dependencies) {
        if (dependency.getType() == DependencyType.REFLECTION) {
          queue.add(((ReflectionDependency) dependency).getReflectionId());
        } else if (dependency.getType() == DependencyType.DATASET) {
          bottomReflections.addAll(getDatasetSuccessors(dependency.getId()));
        }
      }
    }

    // Compute the batch number as the topology sort order for each reflection:
    // - Initially, reflections that directly depend on base tables have batch number of 0
    // - Reflection's batch number = max(reflection's predecessors' batch number) + 1
    bottomReflections.forEach(rId -> batchNumbers.put(rId, 0));
    queue.addAll(bottomReflections);
    while (!queue.isEmpty()) {
      final ReflectionId current = queue.remove();
      List<ReflectionId> successors = getSuccessors(current);
      for (ReflectionId successor : successors) {
        batchNumbers.compute(
            successor,
            (k, v) -> {
              int maxParent =
                  getPredecessors(k).stream()
                      .filter(
                          (Predicate<DependencyEntry>)
                              entry -> entry.getType() == DependencyType.REFLECTION)
                      .map(entry -> ((ReflectionDependency) entry).getReflectionId())
                      .map(reflection -> batchNumbers.getOrDefault(reflection, 0))
                      .mapToInt(Integer::intValue)
                      .max()
                      .orElse(0);
              return maxParent + 1;
            });
        queue.add(successor);
      }
    }

    return ImmutableMap.copyOf(batchNumbers);
  }

  /** Something went wrong while dealing with dependencies */
  public static class DependencyException extends Exception {
    DependencyException(String msg) {
      super(msg);
    }
  }
}
