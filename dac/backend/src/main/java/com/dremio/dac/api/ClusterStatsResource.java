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
package com.dremio.dac.api;

import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_SOURCES;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.reflection.ReflectionStatusUI;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.edition.EditionProvider;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.job.JobStats;
import com.dremio.service.job.JobStatsRequest;
import com.dremio.service.jobs.JobTypeStats;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsServiceUtil;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.reflection.ReflectionStatus.AVAILABILITY_STATUS;
import com.dremio.service.reflection.ReflectionStatus.REFRESH_STATUS;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.Timestamps;

/**
 * ClusterStatsResource represents the resource for all sources information.
 */
@APIResource
@Secured
@Path("/cluster/stats")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class ClusterStatsResource {
  private static final Logger logger = LoggerFactory.getLogger(ClusterStatsResource.class);

  private final Provider<SabotContext> context;
  private final SourceService sourceService;
  private final NamespaceService namespaceService;
  private final JobsService jobsService;
  private final ReflectionServiceHelper reflectionServiceHelper;
  private final EditionProvider editionProvider;

  @Inject
  public ClusterStatsResource(
    Provider<SabotContext> context,
    SourceService sourceService,
    NamespaceService namespaceService,
    JobsService jobsService,
    ReflectionServiceHelper reflectionServiceHelper,
    EditionProvider editionProvider
  ) {
    this.context = context;
    this.sourceService = sourceService;
    this.namespaceService = namespaceService;
    this.jobsService = jobsService;
    this.reflectionServiceHelper = reflectionServiceHelper;
    this.editionProvider = editionProvider;
  }

  /**
   * Defines the GET HTTP operation which retrieve the stats for the cluster.
   *
   * @param showCompactStats a flag which indicates if the stats information should be shown
   *                         in a compacted visualization or not.
   * @return                 the cluster stats
   */
  @GET
  @RolesAllowed({"admin", "user"})
  public ClusterStats getStats(@DefaultValue("false") @QueryParam("showCompactStats") final boolean showCompactStats) {
    return createStats(showCompactStats);
  }

  /**
   * Creates and process the cluster stats information.
   *
   * @param showCompactStats a flag which indicates if the stats information should be shown
   *                         in a compacted visualization or not.
   * @return                 the cluster stats
   */
  ClusterStats createStats(boolean showCompactStats) {
    final ClusterStats result = new ClusterStats();
    final SabotContext sabotContext = this.context.get();

    if (showCompactStats) {
      ClusterNodes nodes = new ClusterNodes();
      nodes.setCoordinator(getNodeStats(sabotContext.getCoordinators()));
      nodes.setExecutor(getNodeStats(sabotContext.getExecutors()));
      result.setClusterNodes(nodes);
    } else {
      result.setExecutors(processEndPoints(sabotContext.getExecutors()));
      result.setCoordinators(processEndPoints(sabotContext.getCoordinators()));
    }

    final Stats resource = getSources(this.sourceService.getSources(), sabotContext);


    // source stats
    final List<SourceStats> sources = resource.getAllSources();

    // optimize vds count queries by only going one to the index with a list of queries
    final List<SearchTypes.SearchQuery> vdsQueries = resource.getVdsQueries();

    try {
      List<Integer> counts = namespaceService.getCounts(vdsQueries.toArray(new SearchTypes.SearchQuery[vdsQueries.size()]));
      for (int i = 0; i < counts.size(); i++) {
        sources.get(i).setVdsCount(counts.get(i));
      }
    } catch (NamespaceException e) {
      logger.warn("Failed to get vds counts", e);
    }

    result.setSources(sources);

    final long end = System.currentTimeMillis();
    final long start = end - TimeUnit.DAYS.toMillis(7);
    final JobStatsRequest request = JobStatsRequest.newBuilder()
      .setStartDate(Timestamps.fromMillis(start))
      .setEndDate(Timestamps.fromMillis(end))
      .build();
    // job stats
    final JobStats jobStats = jobsService.getJobStats(request);
    final List<JobTypeStats> jobTypeStats = jobStats.getCountsList().stream()
      .map(jobCountWithType -> new JobTypeStats(JobsServiceUtil.toType(jobCountWithType.getType()),
        jobCountWithType.getCount()))
      .collect(Collectors.toList());
    result.setJobStats(jobTypeStats);

    // acceleration stats
    Iterable<ReflectionGoal> reflections = reflectionServiceHelper.getAllReflections();

    int activeReflections = 0;
    int errorReflections = 0;
    Long latestReflectionsSizeBytes = 0L;
    long totalReflectionSizeBytes = 0L;
    int incrementalReflectionCount = 0;

    for (ReflectionGoal reflection : reflections) {
      String id = reflection.getId().getId();

      latestReflectionsSizeBytes += reflectionServiceHelper.getCurrentSize(id);
      totalReflectionSizeBytes += reflectionServiceHelper.getTotalSize(id);

      ReflectionStatusUI status = reflectionServiceHelper.getStatusForReflection(id);

      AVAILABILITY_STATUS availability = status.getAvailability();
      if (availability == AVAILABILITY_STATUS.AVAILABLE) {
        activeReflections++;
      } else if (availability == AVAILABILITY_STATUS.INCOMPLETE || status.getRefresh() == REFRESH_STATUS.GIVEN_UP) {
        errorReflections++;
      }

      if (reflectionServiceHelper.isReflectionIncremental(id)) {
        incrementalReflectionCount++;
      }
    }

    ReflectionStats reflectionStats = new ReflectionStats(activeReflections, errorReflections, totalReflectionSizeBytes, latestReflectionsSizeBytes, incrementalReflectionCount);
    result.setReflectionStats(reflectionStats);

    result.setEdition(editionProvider.getEdition());


    return result;
  }

  /**
   * Creates and process the general sources stats and vds queries information.
   *
   * @param allSources a list of sources configuration
   * @param context    the SabotContext object instance
   * @return           the general sources stats
   */
  @VisibleForTesting
  public static Stats getSources(List<SourceConfig> allSources, SabotContext context) {

    final Stats resource = new Stats();

    for (SourceConfig sourceConfig : allSources) {
      int pdsCount = -1;

      String type = sourceConfig.getType();

      if (type == null && sourceConfig.getLegacySourceTypeEnum() != null) {
        type = sourceConfig.getLegacySourceTypeEnum().name();
      }

      if ("S3".equals(type) && sourceConfig.getName().startsWith("Samples")) {
        type = "SamplesS3";

      }

      SourceStats source = new SourceStats(sourceConfig.getId(), type, pdsCount);
      resource.addVdsQuery(SearchQueryUtils.newTermQuery(DATASET_SOURCES, sourceConfig.getName()));
      resource.addSource(source);
    }

    return resource;
  }

  /**
   * Stats represents the general sources and vds queries statistics for the service.
   */
  static class Stats {
    private List<SourceStats> sources;
    private List<SearchTypes.SearchQuery> vdsQueries;

    public Stats() {
      sources = new ArrayList<>();
      vdsQueries = new ArrayList<>
              ();
    }

    /**
     * Adds a new source at the source statistics.
     *
     * @param source the SourceStats instance
     */
    public void addSource(SourceStats source) {
      sources.add(source);
    }

    /**
     * Adds a new SearchQuery instance at the source statistics.
     *
     * @param query the SearchQuery instance
     */
    public void addVdsQuery(SearchTypes.SearchQuery query) {
      vdsQueries.add(query);
    }

    /**
     * Gets all the SourceStats instances that existing.
     *
     * @return the SourceStats instances that existing
     */
    public List<SourceStats> getAllSources() {
      return sources;
    }

    /**
     * Gets all the virtual datasets query that existing.
     *
     * @return the virtual datasets query that existing
     */
    public List<SearchTypes.SearchQuery> getVdsQueries() {
      return vdsQueries;
    }

  }


  /**
   * Gets the general node stats such as average memory and available cores for each cluster node.
   *
   * @param endpoints the list of node endpoints
   * @return          the general node stats
   */
  private NodeStats getNodeStats(Collection<CoordinationProtos.NodeEndpoint> endpoints) {
    final int count = endpoints.size();
    long mem = 0;
    int cores = 0;
    for (final CoordinationProtos.NodeEndpoint endpoint : endpoints) {
      mem += endpoint.getMaxDirectMemory();
      cores += endpoint.getAvailableCores();
    }

    return new NodeStats(count, count == 0 ? 0 : (mem / count), count == 0 ? 0 : (cores / count));
  }

  /**
   * SourceStats represents the sources statistics.
   */
  public static class SourceStats {
    private final EntityId id;
    private final String type;
    private final int pdsCount;
    private int vdsCount;

    @JsonCreator
    public SourceStats(
      @JsonProperty("id") EntityId id,
      @JsonProperty("type") String type,
      @JsonProperty("pdsCount") int pdsCount,
      @JsonProperty("vdsCount") int vdsCount) {
      this.id = id;
      this.type = type;
      this.pdsCount = pdsCount;
      this.vdsCount = vdsCount;
    }

    public SourceStats(EntityId id, String type, int pdsCount) {
      this.id = id;
      this.type = type;
      this.pdsCount = pdsCount;
      this.vdsCount = -1;
    }

    /**
     * Gets the source stats id.
     *
     * @return the source stats id
     */
    public String getId() {
      return id.getId();
    }

    /**
     * Gets the source stats type.
     *
     * @return the source stats type
     */
    public String getType() {
      return type;
    }

    /**
     * Gets the pds count in the source stats.
     *
     * @return the pds count in the source stats
     */
    public int getPdsCount() {
      return pdsCount;
    }

    /**
     * Gets the vds count in the source stats.
     *
     * @return the vds count in the source stats
     */
    public int getVdsCount() {
      return vdsCount;
    }

    /**
     * Sets the vds count in the source stats.
     *
     */
    public void setVdsCount(int vdsCount) {
      this.vdsCount = vdsCount;
    }
  }

  /**
   * ReflectionStats represents the data reflection statistics.
   */
  public static class ReflectionStats {
    private final int activeReflections;
    private final int errorReflections;
    private final long totalReflectionSizeBytes;
    private final long latestReflectionsSizeBytes;
    private final int incrementalReflectionCount;

    @JsonCreator
    public ReflectionStats(
      @JsonProperty("activeReflections") int activeReflections,
      @JsonProperty("errorReflections") int errorReflections,
      @JsonProperty("totalReflectionSizeBytes") long totalReflectionSizeBytes,
      @JsonProperty("latestReflectionsSizeBytes") long latestReflectionsSizeBytes,
      @JsonProperty("incrementalReflectionCount") int incrementalReflectionCount) {
      this.activeReflections = activeReflections;
      this.errorReflections = errorReflections;
      this.totalReflectionSizeBytes = totalReflectionSizeBytes;
      this.latestReflectionsSizeBytes = latestReflectionsSizeBytes;
      this.incrementalReflectionCount = incrementalReflectionCount;
    }

    public int getActiveReflections() {
      return activeReflections;
    }

    public int getErrorReflections() {
      return errorReflections;
    }

    public long getTotalReflectionSizeBytes() {
      return totalReflectionSizeBytes;
    }

    public long getLatestReflectionsSizeBytes() {
      return latestReflectionsSizeBytes;
    }

    public int getIncrementalReflectionCount() {
      return incrementalReflectionCount;
    }
  }

  /**
   * ClusterStats represents the cluster statistics.
   */
  public static class ClusterStats {
    private List<EndpointStats> coordinators;
    private List<EndpointStats> executors;
    private ClusterNodes nodes;
    private List<SourceStats> sources;
    private List<JobTypeStats> jobStats;
    private ReflectionStats reflectionStats;
    private String edition;

    public ClusterStats() {
    }

    @JsonCreator
    public ClusterStats(
      @JsonProperty("coordinators") List<EndpointStats> coordinators,
      @JsonProperty("executors") List<EndpointStats> executors,
      @JsonProperty("nodes") ClusterNodes nodes,
      @JsonProperty("sources") List<SourceStats> sources,
      @JsonProperty("jobStats") List<JobTypeStats> jobStats,
      @JsonProperty("reflectionStats") ReflectionStats reflectionStats,
      @JsonProperty("edition") String edition
    ) {
      this.coordinators = coordinators;
      this.executors = executors;
      this.nodes = nodes;
      this.sources = sources;
      this.jobStats = jobStats;
      this.reflectionStats = reflectionStats;
      this.edition = edition;
    }

    public ClusterNodes getClusterNodes() {
      return nodes;
    }

    public void setClusterNodes(ClusterNodes nodes) {
      this.nodes = nodes;
    }

    public List<SourceStats> getSources() {
      return sources;
    }

    public void setSources(List<SourceStats> sources) {
      this.sources = sources;
    }

    public List<JobTypeStats> getJobStats() {
      return jobStats;
    }

    public void setJobStats(List<JobTypeStats> jobStats) {
      this.jobStats = jobStats;
    }

    public ReflectionStats getReflectionStats() {
      return reflectionStats;
    }

    public void setReflectionStats(ReflectionStats reflectionStats) {
      this.reflectionStats = reflectionStats;
    }

    public String getEdition() {
      return edition;
    }

    public void setEdition(String edition) {
      this.edition = edition;
    }

    public List<EndpointStats> getCoordinators() {
      return coordinators;
    }

    public void setCoordinators(List<EndpointStats> coordinators) {
      this.coordinators = coordinators;
    }

    public List<EndpointStats> getExecutors() {
      return executors;
    }

    public void setExecutors(List<EndpointStats> executors) {
      this.executors = executors;
    }
  }

  /**
   * Creates and process the general node endpoint stats.
   *
   * @param endpoints the list of node endpoints
   * @return          the list of node endpoint stats
   */
  private List<EndpointStats> processEndPoints(Collection<CoordinationProtos.NodeEndpoint> endpoints) {
    final List<EndpointStats> result = endpoints.stream()
      .map(endpoint -> {
        return new EndpointStats(endpoint.getAddress(), endpoint.getAvailableCores(), endpoint.getMaxDirectMemory(),
          endpoint.getStartTime());
      })
      .collect(Collectors.toList());

    return result;
  }

  /**
   * EndpointStats represents the node endpoint statistics.
   */
  public static final class EndpointStats {
    private final String address;
    private final int availableCores;
    private final long maxDirectMemoryBytes;
    private final long startedAt;

    @JsonCreator
    public EndpointStats(
      @JsonProperty("address") String address,
      @JsonProperty("availableCores") int availableCores,
      @JsonProperty("maxDirectMemoryBytes") long maxDirectMemoryBytes,
      @JsonISODateTime
      @JsonProperty("startedAt") long startedAt) {
      this.address = address;
      this.availableCores = availableCores;
      this.maxDirectMemoryBytes = maxDirectMemoryBytes;
      this.startedAt = startedAt;
    }

    public String getAddress() {
      return address;
    }

    public int getAvailableCores() {
      return availableCores;
    }

    public long getMaxDirectMemoryBytes() {
      return maxDirectMemoryBytes;
    }

    public long getStartedAt() {
      return startedAt;
    }

    @Override
    public String toString() {
      return "EndPoint{" +
        "address='" + address + '\'' +
        ", availableCores=" + availableCores +
        ", maxDirectMemoryBytes=" + maxDirectMemoryBytes +
        ", startedAt=" + startedAt +
        '}';
    }
  }

  /**
   * ClusterNodes represents the general coordinator & executor statistics.
   */
  public static class ClusterNodes {
    private NodeStats coordinator;
    private NodeStats executor;

    public ClusterNodes() {
    }

    @JsonCreator
    public ClusterNodes(
      @JsonProperty("coordinator") NodeStats coordinator,
      @JsonProperty("executor") NodeStats executor) {
      this.coordinator = coordinator;
      this.executor = executor;
    }


    /**
     * Gets the coordinator nodes in the cluster.
     *
     * @return the coordinator nodes in the cluster
     */
    public NodeStats getCoordinator() {
      return coordinator;
    }


    /** Sets the coordinator nodes in the cluster.
     *
     * @param coordinator the coordinator to be set
     */
    public void setCoordinator(NodeStats coordinator) {
      this.coordinator = coordinator;
    }


    /** Gets the executor nodes in the cluster.
     *
     * @return the executor nodes in the cluster
     */
    public NodeStats getExecutor() {
      return executor;
    }

    /** Sets the executor nodes in the cluster.
     *
     * @param executor the executor to be set
     */
    public void setExecutor(NodeStats executor) {
      this.executor = executor;
    }
  }

  /**
   * NodeStats represents the general node statistics.
   */
  public static class NodeStats {
    private int count;
    private long mem;
    private int cpu;

    @JsonCreator
    public NodeStats(
      @JsonProperty("count") int count,
      @JsonProperty("mem") long mem,
      @JsonProperty("cpu") int cpu) {
      this.count = count;
      this.mem = mem;
      this.cpu = cpu;
    }

    /**
     * Gets the total number of NodeEndpoints in the cluster.
     *
     * @return the total number of NodeEndpoints in the cluster
     */
    public int getCount() {
      return count;
    }

    /**
     * Gets the average of maximum memory direct for each NodeEndpoint in the cluster.
     *
     * @return the average of maximum memory direct for each NodeEndpoint in the cluster
     */
    public long getMem() {
      return mem;
    }

    /**
     * Gets the average of available cpu cores for each NodeEndpoint.
     *
     * @return the average of available cpu cores for each NodeEndpoint
     */
    public int getCpu() {
      return cpu;
    }
  } //nodestats
}
