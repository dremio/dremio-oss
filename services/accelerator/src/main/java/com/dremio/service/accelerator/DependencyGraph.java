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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.DirectedGraph.EdgeFactory;
import org.apache.calcite.util.graph.TopologicalOrderIterator;
import org.apache.commons.lang3.tuple.Pair;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.accelerator.MaterializationTask.MaterializationContext;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * DependencyGraph
 */
public class DependencyGraph implements Iterable<DependencyNode>, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DependencyGraph.class);

  private final SchedulerService schedulerService;
  private final AccelerationServiceImpl accelerationService;
  private final MaterializationContext materializationContext;

  private volatile List<ChainExecutor> chainExecutors;

  private EdgeFactory<DependencyNode, DefaultEdge> factory = new DirectedGraph.EdgeFactory<DependencyNode, DefaultEdge>() {
    @Override
    public DefaultEdge createEdge(final DependencyNode v0, final DependencyNode v1) {
      return new DefaultEdge(v0, v1) {
        @Override
        public String toString() {
          return String.format("Vertex: %s, Target: %s", v0, v1);
        }
      };
    }
  };
  private final DefaultDirectedGraph<DependencyNode,DefaultEdge> graph = new DefaultDirectedGraph<>(factory);


  public DependencyGraph(AccelerationServiceImpl accelerationService,
                         JobsService jobsService,
                         NamespaceService namespaceService,
                         CatalogService catalogService,
                         SchedulerService schedulerServicer,
                         final String acceleratorStorageName,
                         final MaterializationStore materializationStore,
                         final ExecutorService executorService,
                         FileSystemPlugin acceleratorStoragePlugin) {
    this.schedulerService = schedulerServicer;
    this.accelerationService = accelerationService;
    this.materializationContext = new MaterializationContext(acceleratorStorageName, materializationStore, jobsService,
      namespaceService, catalogService, executorService, accelerationService, acceleratorStoragePlugin);
  }

  public List<Long> buildAndSchedule() {
    this.chainExecutors = buildChainExecutors();
    return FluentIterable.from(chainExecutors)
      .transform(new Function<ChainExecutor, Long>() {
        @Override
        public Long apply(ChainExecutor chainExecutor) {
          return chainExecutor.schedule();
        }
      }).toList();
  }

  @VisibleForTesting
  public List<ChainExecutor> getChainExecutors() {
    return chainExecutors;
  }

  private List<ChainExecutor> buildChainExecutors() {

    final Map<DependencyNode, Pair<Long,Long>> periodMap = new HashMap<>(); // in millis

    for (DependencyNode node : graph.vertexSet()) {
      // check if its a physical dataset.

      // if the refresh period is equal to or more than INFINITE_REFRESH_PERIOD, do not refresh
      if (node.isPhysicalDataset()
          && node.getRefreshPeriod() < NamespaceService.INFINITE_REFRESH_PERIOD) {
        for (DefaultEdge edge : graph.getOutwardEdges(node)) {
          DependencyNode target = (DependencyNode) edge.target;
          Pair<Long, Long> periodPair = periodMap.get(target);
          if (periodPair == null) {
            periodPair = Pair.of(node.getRefreshPeriod(), node.getGracePeriod());
          } else {
            periodPair = Pair.of(Math.min(periodPair.getLeft(), node.getRefreshPeriod()),
                Math.min(periodPair.getRight(), node.getGracePeriod())) ;
          }
          periodMap.put(target, periodPair);
        }
      }
    }
    ImmutableList.Builder<ChainExecutor> builder = ImmutableList.builder();
    for (Entry<DependencyNode, Pair<Long,Long>> entry : periodMap.entrySet()) {
      final DirectedGraph<DependencyNode, DefaultEdge> subGraph = DefaultDirectedGraph.create(factory);

      reachable(subGraph, entry.getKey());

      final Layout head = entry.getKey().getLayout();
      final Optional<Materialization> materialization = accelerationService.getEffectiveMaterialization(head);
      final long startTimeOfLastChain = materialization.isPresent() ? materialization.get().getJob().getJobStart() : 0;
      builder.add(createChainExecutor(graph, subGraph, startTimeOfLastChain, entry.getValue().getLeft(), entry.getValue().getRight()));
    }
    return builder.build();
  }

  private void reachable(DirectedGraph<DependencyNode, DefaultEdge> newGraph, DependencyNode start) {
    buildReachableGraph(newGraph, new HashSet<DependencyNode>(), start);
  }

  private void buildReachableGraph(DirectedGraph<DependencyNode,DefaultEdge> newGraph,
                                  Set<DependencyNode> activeVertices, DependencyNode start) {
    if (!activeVertices.add(start)) {
      return;
    }
    newGraph.addVertex(start);
    List<DefaultEdge> edges = graph.getOutwardEdges(start);
    for (DefaultEdge edge : edges) {
      //noinspection unchecked
      buildReachableGraph(newGraph, activeVertices, (DependencyNode) edge.target);
      //noinspection unchecked
      newGraph.addEdge((DependencyNode) edge.source, (DependencyNode) edge.target);
    }
    activeVertices.remove(start);
  }

  public void addVertex(DependencyNode vertex) {
    graph.addVertex(vertex);
  }

  public void addEdge(DependencyNode vertex, DependencyNode target) {
    graph.addEdge(vertex, target);
  }

  @Override
  public void close() {
    try {
      AutoCloseables.close(chainExecutors);
    } catch (Exception ignored) {
      logger.warn("Failed while closing chain executors", ignored);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(System.lineSeparator()).append("DependencyGraph").append(System.lineSeparator());
    for (DefaultEdge e : graph.edgeSet()) {
      sb.append(e.toString()).append(System.lineSeparator());
    }
    //TODO add proper logging as part of DX-9062
    return sb.toString();
  }

  @Override
  public Iterator<DependencyNode> iterator() {
    return TopologicalOrderIterator.of(graph).iterator();
  }

  /**
   * Creates a chain executor for the given sub graph of layouts, that last started at the given time, at the given
   * refresh period.
   *
   * @param graph dependency graph
   * @param subGraph sub graph
   * @param startTimeOfLastChain last start time of the chain
   * @param refreshPeriod refresh period
   * @return chain executor
   */
  private ChainExecutor createChainExecutor(DirectedGraph<DependencyNode, DefaultEdge> graph,
                                            DirectedGraph<DependencyNode, DefaultEdge> subGraph,
                                            long startTimeOfLastChain, long refreshPeriod, long gracePeriod) {
    final List<Layout> layouts = FluentIterable.from(TopologicalOrderIterator.of(subGraph))
      .transform(new Function<DependencyNode, Layout>() {
        @Override
        public Layout apply(DependencyNode node) {
          return node.getLayout();
        }
      }).toList();

    String uuid = UUID.randomUUID().toString();
    String rootId = layouts.get(0).getId().getId();
    logger.info("Creating chain executor for root node {} with id {}.", rootId, uuid);
    if (logger.isDebugEnabled()) {
      logger.debug("Sub graph for chain executor {}:{} is: {}.", rootId, uuid, layouts.toString());
    }

    return new ChainExecutor(graph, layouts, startTimeOfLastChain, refreshPeriod, gracePeriod, schedulerService,
      materializationContext, accelerationService.getSettings().getLayoutRefreshMaxAttempts(), rootId + ":" + uuid);
  }
}
