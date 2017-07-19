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

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.DirectedGraph.EdgeFactory;
import org.apache.calcite.util.graph.TopologicalOrderIterator;
import org.threeten.bp.Instant;
import org.threeten.bp.temporal.TemporalAmount;

import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.accelerator.DependencyGraph.DependencyNode;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.store.AccelerationIndexKeys;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * DependencyGraph
 */
public class DependencyGraph implements Iterable<DependencyNode> {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(DependencyGraph.class);

  private final JobsService jobsService;
  private final NamespaceService namespaceService;
  private final SchedulerService schedulerService;
  private final AccelerationServiceImpl accelerationService;
  private final String acceleratorStorageName;
  private final MaterializationStore materializationStore;
  private final ExecutorService executorService;
  private final AccelerationStore accelerationStore;
  private final CatalogService catalogService;

  private List<DependencySubGraph> subGraphs;

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

  private final FileSystemPlugin acceleratorStoragePlugin;


  public DependencyGraph(AccelerationServiceImpl accelerationService,
                         JobsService jobsService,
                         NamespaceService namespaceService,
                         CatalogService catalogService,
                         SchedulerService schedulerServicer,
                         final String acceleratorStorageName,
                         final MaterializationStore materializationStore,
                         final ExecutorService executorService,
                         final AccelerationStore accelerationStore,
                         FileSystemPlugin acceleratorStoragePlugin) {
    this.jobsService = jobsService;
    this.namespaceService = namespaceService;
    this.schedulerService = schedulerServicer;
    this.accelerationService = accelerationService;
    this.acceleratorStorageName = acceleratorStorageName;
    this.materializationStore = materializationStore;
    this.executorService = executorService;
    this.accelerationStore = accelerationStore;
    this.acceleratorStoragePlugin = acceleratorStoragePlugin;
    this.catalogService = catalogService;
  }

  public List<Long> buildAndSchedule() {
    this.subGraphs = buildDependencySubgraphs();
    List<Long> startTimes = FluentIterable.from(subGraphs)
      .transform(new Function<DependencySubGraph, Long>() {
        @Nullable
        @Override
        public Long apply(@Nullable DependencySubGraph subGraph) {
          return subGraph.schedule();
        }
      }).toList();
    return startTimes;
  }

  public List<DependencySubGraph> getDependencySubgraphs() {
    return subGraphs;
  }

  private List<DependencySubGraph> buildDependencySubgraphs() {
    Map<DependencyNode, Long> ttlMap = new HashMap<>();

    for (DependencyNode node : graph.vertexSet()) {
      if (node.isPhysicalDataset()) {
        for (DefaultEdge edge : graph.getOutwardEdges(node)) {
          DependencyNode target = (DependencyNode) edge.target;
          Long ttl = ttlMap.get(target);
          if (ttl == null) {
              ttl = node.ttl;
          } else {
            ttl = Math.min(ttl, node.ttl);
          }
          ttlMap.put(target, ttl);
        }
      }
    }
    ImmutableList.Builder<DependencySubGraph> builder = ImmutableList.builder();
    for (Entry<DependencyNode, Long> entry : ttlMap.entrySet()) {
      DirectedGraph<DependencyNode, DefaultEdge> newGraph = DefaultDirectedGraph.create(factory);
      reachable(newGraph, entry.getKey());
      Layout head = entry.getKey().layout;
      Optional<Materialization> materialization = accelerationService.getEffectiveMaterialization(head);
      if (materialization.isPresent()) {
        long startTime = materialization.get().getJob().getJobStart();
        long ttl = entry.getValue();
        long runningTime = 0;
        for (DependencyNode node : newGraph.vertexSet()) {
          Layout layout = node.layout;
          Optional<Materialization> m = accelerationService.getEffectiveMaterialization(layout);
          if (m.isPresent()) {
            runningTime += (m.get().getJob().getJobEnd() - m.get().getJob().getJobStart());
          }
        }
        builder.add(new DependencySubGraph(newGraph, ttl, startTime, runningTime));
      }
    }
    return builder.build();
  }

  public void reachable(DirectedGraph<DependencyNode, DefaultEdge> newGraph, DependencyNode start) {
    buildReachableGraph(newGraph, new HashSet<DependencyNode>(), start);
  }

  public void buildReachableGraph(DirectedGraph<DependencyNode,DefaultEdge> newGraph,
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

  public void close() {
    for (DependencySubGraph subGraph : subGraphs) {
      subGraph.close();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (DefaultEdge e : graph.edgeSet()) {
      sb.append(e.toString());
      sb.append("\n");
    }
    return sb.toString();
  }

  @Override
  public Iterator<DependencyNode> iterator() {
    return TopologicalOrderIterator.of(graph).iterator();
  }

  /**
   * Dependency node
   */
  public static class DependencyNode {
    private final Layout layout;
    private final List<String> tableSchemaPath;
    private final long ttl;

    public DependencyNode(Layout layout) {
      this.layout = Preconditions.checkNotNull(layout);
      this.tableSchemaPath = null;
      this.ttl = -1;
    }

    public DependencyNode(List<String> tableSchemaPath, long ttl) {
      this.tableSchemaPath = Preconditions.checkNotNull(tableSchemaPath);
      this.ttl = ttl;
      this.layout = null;
    }

    public Layout getLayout() {
      return layout;
    }

    public List<String> getTableSchemaPath() {
      return tableSchemaPath;
    }

    public boolean isAcceleration() {
      return layout != null;
    }

    public boolean isPhysicalDataset() {
      return tableSchemaPath != null;
    }

    @Override
    public String toString() {
      if (layout != null) {
        return layout.getId().getId();
      } else {
        return tableSchemaPath.toString();
      }
    }

    @Override
    public int hashCode() {
      if (isAcceleration()) {
        return layout.getId().hashCode();
      } else {
        return tableSchemaPath.hashCode();
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DependencyNode)) {
        return false;
      }
      DependencyNode that = (DependencyNode) obj;

      return Objects.equals(this.layout, that.layout) &&
        Objects.equals(this.tableSchemaPath, that.tableSchemaPath);
    }
  }

  /**
   * Dependency sub graph
   */
  public class DependencySubGraph implements Closeable, Iterable<DependencyNode> {
    private DirectedGraph<DependencyNode, DefaultEdge> graph;
    private final long ttl;
    private AtomicLong timestamp;
    private AtomicLong runTime;
    private Cancellable cancellable;
    private volatile boolean cancelled = false;

    public DependencySubGraph(DirectedGraph<DependencyNode, DefaultEdge> graph, long ttl, long timestamp, long runTime) {
      this.graph = graph;
      this.ttl = ttl;
      this.timestamp = new AtomicLong(timestamp);
      this.runTime = new AtomicLong(runTime);
    }

    public long schedule() {
      if (cancelled) {
        return -1;
      }
      final long startTime = timestamp.get() + ttl - runTime.get();
      if (startTime < System.currentTimeMillis()) {
        LOGGER.warn("Unable to start materialization task in time");
      }
      Schedule schedule = new Schedule() {
        @Override
        public TemporalAmount getPeriod() {
          return null;
        }

        @Override
        public Iterator<Instant> iterator() {
          return Collections.singletonList(Instant.ofEpochMilli(startTime)).iterator();
        }
      };
      cancellable = schedulerService.schedule(schedule, getTasks());
      LOGGER.debug("Scheduled chain: {}", Instant.ofEpochMilli(startTime));
      return startTime;
    }

    private Runnable getTasks() {
      List<DependencyNode> orderedNodes = Lists.newArrayList(TopologicalOrderIterator.of(graph));

      final MaterializationTask firstTask = materializationTask(orderedNodes.get(0).layout);
      MaterializationTask previous = firstTask;

      for (int i = 1; i < orderedNodes.size() - 1; i++) {
        MaterializationTask next = materializationTask(orderedNodes.get(i).layout);
        previous.setNext(next);
        previous = next;
      }

      final MaterializationTask lastTask = materializationTask(orderedNodes.get(orderedNodes.size() - 1).layout);
      previous.setNext(new AsyncTask() {
        @Override
        protected void doRun() {
          lastTask.doRun();
          runTime.set(System.currentTimeMillis() - timestamp.get());
          LOGGER.debug("Finished materialization chain");
          schedule();
        }
      });

      return new Runnable() {
        @Override
        public void run() {
          timestamp.set(System.currentTimeMillis());
          LOGGER.debug("Beginning materialization chain");
          firstTask.run();
        }
      };
    }

    private MaterializationTask materializationTask(Layout layout) {
      Acceleration acceleration = accelerationStore.getByIndex(AccelerationIndexKeys.LAYOUT_ID, layout.getId().getId()).get();
      return MaterializationTask.create(acceleratorStorageName, materializationStore, jobsService, namespaceService, catalogService,
        layout, executorService, acceleration, acceleratorStoragePlugin);
    }

    @Override
    public void close() {
      cancelled = true;
      if (cancellable != null) {
        cancellable.cancel();
      }
    }

    @Override
    public Iterator<DependencyNode> iterator() {
      return TopologicalOrderIterator.of(graph).iterator();
    }
  }
}
