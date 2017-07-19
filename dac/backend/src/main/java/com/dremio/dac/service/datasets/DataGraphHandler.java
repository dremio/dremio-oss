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
package com.dremio.dac.service.datasets;

import static com.dremio.dac.util.DatasetsUtil.isCreatedFromParent;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_ALLPARENTS;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_PARENTS;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.graph.DataGraph;
import com.dremio.dac.model.graph.DataGraph.SourceGraphUI;
import com.dremio.dac.model.graph.DatasetGraphNode;
import com.dremio.dac.model.spaces.TempSpace;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.service.datasets.DatasetVersionMutator.VersionDatasetKey;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceType;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Generate datagraph.
 */
public class DataGraphHandler {
  private static final Logger logger = LoggerFactory.getLogger(DatasetVersionMutator.class);
  private static final String MATCH_ALL = "*";

  private final NamespaceService namespaceService;
  private final JobsService jobsService;
  private final KVStore<VersionDatasetKey, VirtualDatasetVersion> datasetVersions;

  public DataGraphHandler(
      NamespaceService namespaceService,
      JobsService jobsService,
      KVStore<VersionDatasetKey, VirtualDatasetVersion> datasetVersions) {
    this.namespaceService = namespaceService;
    this.jobsService = jobsService;
    this.datasetVersions = datasetVersions;
  }

  private List<Integer> getDescendantCounts(List<NamespaceKey> datasetPaths) throws NamespaceException {
    try (TimedBlock b = Timer.time("getDescendantCounts")) {
      final List<SearchQuery> descendantsQueries = Lists.newArrayList();
      for (NamespaceKey dataset : datasetPaths) {
        descendantsQueries.add(SearchQueryUtils.newTermQuery(DATASET_ALLPARENTS, dataset.toString()));
      }
      return namespaceService.getCounts(descendantsQueries.toArray(new SearchQuery[descendantsQueries.size()]));
    }
  }

  private List<SourceGraphUI> getSources(DatasetConfig datasetConfig) throws NamespaceException {
    final List<SourceGraphUI> sources = Lists.newArrayList();
    final List<NamespaceKey> sourceKeys = Lists.newArrayList();
    final List<String> sourceNames = QueryMetadata.getSources(datasetConfig);
    for (String sourceName : sourceNames) {
      sourceKeys.add(new NamespaceKey(sourceName));
    }
    int i = 0;
    for (NameSpaceContainer container : namespaceService.getEntities(sourceKeys)) {
      if (container != null && container.getType() == Type.SOURCE && container.getSource() != null) {
          sources.add(new SourceGraphUI(container.getSource().getName(), container.getSource().getType()));
      } else {
        // missing source
        sources.add(new SourceGraphUI(sourceNames.get(i), null, true));
      }
      ++i;
    }
    return sources;
  }

  private List<DatasetConfig> getParents(List<NamespaceKey> parentPaths) throws NamespaceException {
    final List<DatasetConfig> datasets = Lists.newArrayList();
    int i = 0;
    for (NameSpaceContainer container : namespaceService.getEntities(parentPaths)) {
      if (container != null && container.getType() == Type.DATASET && container.getDataset() != null) {
        datasets.add(container.getDataset());
      } else {
        // missing parent, don't set type
        datasets.add(new DatasetConfig().setFullPathList(parentPaths.get(i).getPathComponents()).setName(parentPaths.get(i).getName()));
      }
      ++i;
    }
    return datasets;
  }

  private List<DatasetConfig> findChildren(NamespaceKey dataset) {
    final FindByCondition findByCondition = new FindByCondition();
    findByCondition.setCondition(SearchQueryUtils.newTermQuery(DATASET_PARENTS, dataset.toString()));
    findByCondition.setPageSize(Integer.MAX_VALUE);
    findByCondition.setLimit(Integer.MAX_VALUE);
    final List<DatasetConfig> children = Lists.newArrayList();
    for (Entry<NamespaceKey, NameSpaceContainer> entry : namespaceService.find(findByCondition)) {
      children.add(entry.getValue().getDataset());
    }
    return children;
  }

  // Do a local search for filtering parents and children
  private boolean datasetMatches(DatasetConfig parent, String filter) {
    if (filter != null && !filter.isEmpty() && !MATCH_ALL.equals(filter)) {
      return StringUtils.containsIgnoreCase(PathUtils.constructFullPath(parent.getFullPathList()), filter);
    }
    return true;
  }

  /**
   * Get data graph at currentPath
   * @param origin dataset which is at the center of datagraph
   * @param originVersion version of dataset
   * @param currentDatasetConfig dataset properties of root of the graph
   * @param currentPath path of root of the graph.
   * @return
   * @throws NamespaceException
   */
  private DataGraph getDataGraph(DatasetPath origin, DatasetVersion originVersion,
                                 DatasetConfig currentDatasetConfig, NamespaceKey currentPath,
                                 String parentFilter, String childrenFilter) throws NamespaceException {
    if (currentDatasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
      final List<NamespaceKey> parentPaths = Lists.newArrayList();
      final VirtualDataset virtualDataset = currentDatasetConfig.getVirtualDataset();
      for (ParentDataset parentDataset:  virtualDataset.getParentsList()) {
        parentPaths.add(new NamespaceKey(parentDataset.getDatasetPathList()));
      }

      // rpc 2. get parent datasets in single call
      final List<DatasetConfig> parents = getParents(parentPaths);

      // rpc 3. get children
      final List<DatasetConfig> children  = findChildren(currentPath);

      // combine list of all datasets we want to get descedants and job count for.
      // Parents + children + origin
      for (DatasetConfig child : children) {
        parentPaths.add(new NamespaceKey(child.getFullPathList()));
      }
      parentPaths.add(currentPath);

      // rpc 4. get counts for descendants.
      final List<Integer> descendantsCount = getDescendantCounts(parentPaths);

      // rpc 5. get counts for jobs.
      final List<Integer> jobsCount = jobsService.getJobsCount(parentPaths);

      // build datagraph
      final int currentOffset = parentPaths.size() - 1;

      final DataGraph dataGraph = new DataGraph(origin, originVersion,
        currentDatasetConfig, jobsCount.get(currentOffset), descendantsCount.get(currentOffset));

      // rpc 6. add sources
      dataGraph.addSources(getSources(currentDatasetConfig));

      int index = 0;
      for (DatasetConfig parent : parents) {
        if (datasetMatches(parent, parentFilter)) {
          dataGraph.addParent(parent, jobsCount.get(index), descendantsCount.get(index));
        }
        ++index;
      }
      for (DatasetConfig child : children) {
        if (datasetMatches(child, childrenFilter)) {
          dataGraph.addChild(child, jobsCount.get(index), descendantsCount.get(index));
        }
        ++index;
      }
      return dataGraph;
    } else { // physical dataset
      final List<NamespaceKey> datasetPaths = Lists.newArrayList();
      // rpc 2. get children
      final List<DatasetConfig> children  = findChildren(currentPath);

      // combine list of datasets to query for getting counts
      for (DatasetConfig child : children) {
        datasetPaths.add(new NamespaceKey(child.getFullPathList()));
      }
      datasetPaths.add(currentPath);

      // rpc 3. get counts for descendants
      final List<Integer> descendantsCount = getDescendantCounts(datasetPaths);

      // rpc 4. get job counts
      final List<Integer> jobsCount = jobsService.getJobsCount(datasetPaths);

      // build datagraph
      final int lastDataset = datasetPaths.size() - 1;
      final DataGraph dataGraph = new DataGraph(origin, originVersion,
        currentDatasetConfig, jobsCount.get(lastDataset), descendantsCount.get(lastDataset));

      int index = 0;
      for (DatasetConfig child :  children) {
        if (datasetMatches(child, childrenFilter)) {
          dataGraph.addChild(child, jobsCount.get(index), descendantsCount.get(index));
        }
        ++index;
      }
      if (currentDatasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE ||
        currentDatasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER) {
        dataGraph.addSources(Collections.singletonList(new SourceGraphUI(currentDatasetConfig.getFullPathList().get(0), SourceType.HOME)));
      } else {
        // rpc 5. add source
        final NamespaceKey sourcePath = new NamespaceKey(currentDatasetConfig.getFullPathList().get(0));
        dataGraph.addSources(Collections.singletonList(new SourceGraphUI(sourcePath.getName(),
          namespaceService.getSource(sourcePath).getType())));
      }
      return dataGraph;
    }
  }

  public static DatasetPath getParentDatasetPath(VirtualDatasetVersion datasetVersion) {
    return new DatasetPath(datasetVersion.getLastTransform().getTransformCreateFromParent().getCreateFrom().getTable().getDatasetPath());
  }
  /**
   * Generate datagrap for unsaved datasets.
   * Children in datagraph are children of saved dataset
   * Parents in datagraph are based on whats inside versioned dataset.
   * Create a link from parents to origin dataset when current view is one of the parent.
   * @param origin path of dataset
   * @param version version of dataset.
   * @param currentView current view in datagraph
   * @param parentFilter show parents matching this term
   * @param childrenFilter show children matching this term
   * @return datagraph
   * @throws NamespaceException
   */
  public DataGraph getDataGraph(DatasetPath origin, DatasetVersion version, DatasetPath currentView,
                                String parentFilter, String childrenFilter) throws NamespaceException {
    Preconditions.checkNotNull(version);

    try (TimedBlock b = Timer.time("getDataGraphVersion")) {
      b.addID(new VersionDatasetKey(origin, version).toString());
      // rpc 1. get dataset from versions table
      final VirtualDatasetVersion originDataset = datasetVersions.get(new VersionDatasetKey(origin, version));
      final DatasetConfig originDatasetConfig = originDataset.getDataset();
      Preconditions.checkNotNull(originDatasetConfig.getVirtualDataset());
      final boolean isTmpUntitled = TempSpace.isTempSpace(origin.getRoot().getName()) && isCreatedFromParent(originDataset.getLastTransform());

      if (currentView == null) {
        // Update the origin dataset if this is a tmp dataset and uses select * from parent query.
        // In this case we want to center datagraph around parent.
        if (isTmpUntitled) {
          currentView = getParentDatasetPath(originDataset);
          final DatasetConfig parentDatasetConfig = namespaceService.getDataset(currentView.toNamespaceKey());
          return getDataGraph(origin, version, parentDatasetConfig, currentView.toNamespaceKey(), parentFilter, childrenFilter);
        }
        return getDataGraph(origin, version, originDatasetConfig, origin.toNamespaceKey(), parentFilter, childrenFilter);
      } else {
        if (isTmpUntitled && currentView.equals(getParentDatasetPath(originDataset))) {
          final DatasetConfig parentDatasetConfig = namespaceService.getDataset(currentView.toNamespaceKey());
          return getDataGraph(origin, version, parentDatasetConfig, currentView.toNamespaceKey(), parentFilter, childrenFilter);
        }
        // rpc 2. get current view dataset
        final DataGraph dataGraph = getDataGraph(origin, version,
          namespaceService.getDataset(currentView.toNamespaceKey()), currentView.toNamespaceKey(),
          parentFilter, childrenFilter);
        final Set<NamespaceKey> parents = Sets.newHashSet();
        for (ParentDataset parentDataset : originDatasetConfig.getVirtualDataset().getParentsList()) {
          parents.add(new NamespaceKey(parentDataset.getDatasetPathList()));
        }
        if (parents.contains(currentView.toNamespaceKey())) { // current view is at one of the parents of origin
          // Check if datagraph of parent already has origin as a child.
          final Set<NamespaceKey> children = Sets.newHashSet();
          for (DatasetGraphNode child : dataGraph.getChildren()) {
            children.add(new NamespaceKey(child.getFullPath()));
          }
          if (!children.contains(origin.toNamespaceKey())) {
            // 2 rpcs to get job count and descendants
            dataGraph.addChild(originDatasetConfig, jobsService.getJobsCountForDataset(origin.toNamespaceKey(), version),
              getDescendantsCount(origin.toNamespaceKey()));
          }
        }
        return dataGraph;
      }
    }
  }

  /**
   * Geenrate datagraph for a saved dataset
   * @param origin path of saved dataset
   * @param currentView current view in datagraph
   * @param parentFilter show parents matching this term
   * @param childrenFilter show children matching this term
   * @return datagraph
   * @throws NamespaceException
   */
  public DataGraph getDataGraph(DatasetPath origin, DatasetPath currentView, String parentFilter, String childrenFilter) throws NamespaceException {
    try (TimedBlock b = Timer.time("getDataGraph")) {
      b.addID(origin.toString());
      if (currentView == null) {
        // rpc 1. get dataset
        return getDataGraph(origin, null,
          namespaceService.getDataset(origin.toNamespaceKey()), origin.toNamespaceKey(),
          parentFilter, childrenFilter);
      } else {
        // rpc 1. get dataset for current view
        return getDataGraph(origin, null,
          namespaceService.getDataset(currentView.toNamespaceKey()), currentView.toNamespaceKey(),
          parentFilter, childrenFilter);
      }
    }
  }

  /**
   * Get count of datasets depending on given dataset
   * @param path path of saved dataset
   * @return count of all descendants.
   * @throws NamespaceException
   */
  public int getDescendantsCount(NamespaceKey path) throws NamespaceException {
    return getDescendantCounts(Collections.singletonList(path)).get(0);
  }

  /**
   * Get list of dataset paths depending on given dataset
   * @param path path of saved dataset
   * @return dataset paths of descendants.
   * @throws NamespaceException
   */
  public Iterable<DatasetPath> getDescendants(DatasetPath path) throws NamespaceException {
    FindByCondition condition = new FindByCondition()
      .setCondition(SearchQueryUtils.newTermQuery(DATASET_ALLPARENTS, path.toString()))
      .setLimit(1000);
    return Iterables.transform(namespaceService.find(condition), new Function<Entry<NamespaceKey, NameSpaceContainer>, DatasetPath>() {
      @Override
      public DatasetPath apply(Entry<NamespaceKey, NameSpaceContainer> input) {
        return new DatasetPath(input.getKey().getPathComponents());
      }
    });
  }
}
