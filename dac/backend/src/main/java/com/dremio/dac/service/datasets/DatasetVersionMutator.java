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

import static com.dremio.dac.service.datasets.DatasetDownloadManager.DATASET_DOWNLOAD_STORAGE_PLUGIN;
import static com.dremio.dac.util.DatasetsUtil.toVirtualDatasetUI;
import static com.dremio.dac.util.DatasetsUtil.toVirtualDatasetVersion;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_ID;
import static com.dremio.service.namespace.DatasetIndexKeys.MAPPING;
import static com.dremio.service.namespace.dataset.DatasetVersion.MAX_VERSION;
import static com.dremio.service.namespace.dataset.DatasetVersion.MIN_VERSION;
import static java.lang.String.format;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map.Entry;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.config.DremioConfig;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.model.common.RootEntity.RootType;
import com.dremio.dac.model.graph.DataGraph;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStore.FindByRange;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.datastore.Serializer;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.StringSerializer;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.BindingProvider;
import com.dremio.service.Initializer;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.job.proto.DownloadInfo;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * For creating/updating/deleting of dataset and dataset versions.
 */
public class DatasetVersionMutator {
  private static final Logger logger = LoggerFactory.getLogger(DatasetVersionMutator.class);

  private final NamespaceService namespaceService;
  private final JobsService jobsService;

  private DataGraphHandler dataGraphHandler;
  private DatasetDownloadManager datasetDownloadManager;
  private KVStore<VersionDatasetKey, VirtualDatasetVersion> datasetVersions;

  @Inject
  public DatasetVersionMutator(
      final InitializerRegistry init,
      final KVStoreProvider kv,
      final NamespaceService namespaceService,
      final JobsService jobsService) {
    this.namespaceService = namespaceService;
    this.jobsService = jobsService;
    this.datasetVersions = kv.getStore(VersionStoreCreator.class);
    this.dataGraphHandler = new DataGraphHandler(namespaceService, jobsService, datasetVersions);
    InitializerImpl initd = init.get(InitializerImpl.class);
    this.datasetDownloadManager = new DatasetDownloadManager(jobsService, initd.getDownloadRootDir(), initd.getFileSystem());
  }

  private void validate(DatasetPath path, VirtualDatasetUI ds) {
    if (ds.getSqlFieldsList() == null || ds.getSqlFieldsList().isEmpty()) {
      throw new IllegalArgumentException("SqlFields can't be null for " + path);
    }
    if (ds.getState() == null) {
      throw new IllegalArgumentException("state can't be null for " + path);
    }
  }


  public void putVersion(VirtualDatasetUI ds) throws DatasetNotFoundException, NamespaceException {
    DatasetPath path = new DatasetPath(ds.getFullPathList());
    validate(path, ds);
    ds.setCreatedAt(System.currentTimeMillis());
    final VersionDatasetKey datasetKey = new VersionDatasetKey(path, ds.getVersion());
    datasetVersions.put(datasetKey, toVirtualDatasetVersion(ds));
  }

  public void put(VirtualDatasetUI ds) throws DatasetNotFoundException, NamespaceException {
    DatasetPath path = new DatasetPath(ds.getFullPathList());
    validatePath(path);
    validate(path, ds);
    DatasetConfig datasetConfig = toVirtualDatasetVersion(ds).getDataset();
    namespaceService.addOrUpdateDataset(path.toNamespaceKey(), datasetConfig);
    ds.setId(datasetConfig.getId().getId());
    ds.setSavedVersion(datasetConfig.getVersion());
    // Update this version of dataset with new occ version of dataset config from namespace.
    putVersion(ds);
  }

  private void validatePath(DatasetPath path) {
    if (path.getRoot().getRootType() == RootType.TEMP) {
      throw new IllegalArgumentException("can not save dataset in tmp space");
    }
  }

  public VirtualDatasetUI renameDataset(DatasetPath oldPath, DatasetPath newPath)
      throws NamespaceException, DatasetNotFoundException, DatasetVersionNotFoundException {
    try {
      validatePath(newPath);
      VirtualDatasetVersion returnDataset = null;
      final DatasetConfig datasetConfig = namespaceService.renameDataset(oldPath.toNamespaceKey(), newPath.toNamespaceKey());
      // rename all old versions, find a version dataset matching with version in namespace
      for (final VirtualDatasetUI ds : getAllVersions(oldPath)) {
        datasetVersions.delete(new VersionDatasetKey(oldPath, ds.getVersion()));
        ds.setName(newPath.getDataset().getName());
        ds.setFullPathList(newPath.toPathList());
        VirtualDatasetVersion vvds = toVirtualDatasetVersion(ds);
        datasetVersions.put(new VersionDatasetKey(newPath, ds.getVersion()), vvds);
        if (datasetConfig.getVirtualDataset().getVersion().equals(vvds.getDataset().getVirtualDataset().getVersion())) {
          returnDataset = vvds;
        }
      }
      if (returnDataset == null) {
        throw new DatasetNotFoundException(newPath,
          format("Missing version %s after rename.", datasetConfig.getVirtualDataset().getVersion().toString()));
      }
      return toVirtualDatasetUI(returnDataset);
    } catch (NamespaceNotFoundException nfe) {
      throw new DatasetNotFoundException(oldPath, nfe);
    }
  }

  public VirtualDatasetUI getVersion(DatasetPath path, DatasetVersion version)
      throws DatasetVersionNotFoundException {
    VirtualDatasetUI virtualDatasetUI = toVirtualDatasetUI(datasetVersions.get(new VersionDatasetKey(path, version)));

    try {
      final DatasetConfig datasetConfig = namespaceService.getDataset(path.toNamespaceKey());
      virtualDatasetUI
          .setId(datasetConfig.getId().getId())
          .setSavedVersion(datasetConfig.getVersion());
    } catch (final NamespaceException ex) {
      logger.debug("dataset error for {}", path, ex);
    }

    if (virtualDatasetUI == null) {
      throw new DatasetVersionNotFoundException(path, version);
    }
    return virtualDatasetUI;
  }

  public Iterable<VirtualDatasetUI> getAllVersions(DatasetPath path) throws DatasetVersionNotFoundException {
    return Iterables.transform(datasetVersions.find(
        new FindByRange<>(new VersionDatasetKey(path, MIN_VERSION), false, new VersionDatasetKey(path, MAX_VERSION), false)),
      new Function<Entry<VersionDatasetKey, VirtualDatasetVersion>, VirtualDatasetUI> () {
        @Override
        public VirtualDatasetUI apply(Entry<VersionDatasetKey, VirtualDatasetVersion> input) {
          return toVirtualDatasetUI(input.getValue());
        }
      });
  }

  /**
   * @param path
   *          the id of the dataset
   * @return the latest saved version of the corresponding dataset
   * @throws DatasetNotFoundException
   *           if the path was not found
   */
  public VirtualDatasetUI get(DatasetPath path) throws DatasetNotFoundException, NamespaceException {
    try {
      final DatasetConfig datasetConfig = namespaceService.getDataset(path.toNamespaceKey());
      final VirtualDatasetVersion virtualDatasetVersion = datasetVersions.get(new VersionDatasetKey(path, datasetConfig.getVirtualDataset().getVersion()));
      if (virtualDatasetVersion == null) {
        throw new DatasetNotFoundException(path, format("Missing version %s.", datasetConfig.getVirtualDataset().getVersion().toString()));
      }
      final VirtualDatasetUI virtualDatasetUI = toVirtualDatasetUI(virtualDatasetVersion)
          .setId(datasetConfig.getId().getId())
          .setSavedVersion(datasetConfig.getVersion());
      return virtualDatasetUI;
    } catch (NamespaceNotFoundException nsnf) {
      throw new DatasetNotFoundException(path, nsnf);
    }
  }

  public VirtualDatasetUI get(DatasetPath path, DatasetVersion version) throws DatasetNotFoundException, NamespaceException {
    try {
      final DatasetConfig datasetConfig = namespaceService.getDataset(path.toNamespaceKey());
      final VirtualDatasetVersion virtualDatasetVersion = datasetVersions.get(new VersionDatasetKey(path, version));
      if (virtualDatasetVersion == null) {
        throw new DatasetNotFoundException(path, format("Missing version %s.", version.toString()));
      }
      final VirtualDatasetUI virtualDatasetUI =  toVirtualDatasetUI(virtualDatasetVersion)
          .setId(datasetConfig.getId().getId())
          .setSavedVersion(datasetConfig.getVersion());
      return virtualDatasetUI;
    } catch (NamespaceNotFoundException e) {
      throw new DatasetNotFoundException(path, format("Some path not found while looking for dataset %s, version %s.", path.toPathString(), version.toString()), e);
    }
  }

  public void deleteDataset(DatasetPath datasetPath, long version) throws DatasetNotFoundException, NamespaceException {
    try {
      namespaceService.deleteDataset(datasetPath.toNamespaceKey(), version);
    } catch (NamespaceNotFoundException nsnf) {
      throw new DatasetNotFoundException(datasetPath, nsnf);
    }
  }

  public NamespaceService getNamespaceService() {
    return namespaceService;
  }

  public int getJobsCount(NamespaceKey path) {
    return jobsService.getJobsCount(path);
  }

  public long getJobsCount(List<NamespaceKey> datasetPaths) {
    long jobCount = 0;
    for (Integer count : jobsService.getJobsCount(datasetPaths)) {
      if (count != null) {
        jobCount += count;
      }
    }
    return jobCount;
  }

  private static final SearchFieldSorting DEFAULT_SORTING = DATASET_ID.toSortField(SortOrder.DESCENDING);

  public List<DatasetConfig> searchDatasets(String query) {
    final SearchQuery searchQuery;
    if (query == null || query.isEmpty()) {
      searchQuery = SearchQueryUtils.newMatchAllQuery();
    } else {
      final ImmutableList.Builder<SearchQuery> builder = ImmutableList.builder();
      for (final String name : MAPPING.getSearchAllIndexKeys()) {
        final String value;
        if (query.contains("*")) {
          value = query;
        } else {
          value = String.format("*%s*", query);
        }
        builder.add(SearchQueryUtils.newWildcardQuery(name, value));
      }

      searchQuery = SearchQueryUtils.or(builder.build());
    }

    final FindByCondition condition = new FindByCondition()
        .setCondition(searchQuery)
        .setLimit(100)
        .addSorting(DEFAULT_SORTING);

    final List<DatasetConfig> datasets = Lists.newArrayList();
    for (Entry<NamespaceKey, NameSpaceContainer> entry : namespaceService.find(condition)) {
      if (entry.getValue().getType() == NameSpaceContainer.Type.DATASET) {
        datasets.add(entry.getValue().getDataset());
      }
    }
    return datasets;
  }

  public DataGraph getDataGraph(DatasetPath datasetPath, DatasetPath currentPath, String parentFilter, String childrenFilter) throws NamespaceException {
    return dataGraphHandler.getDataGraph(datasetPath, currentPath, parentFilter, childrenFilter);
  }

  public DataGraph getDataGraph(DatasetPath datasetPath, DatasetVersion version, DatasetPath currentPath, String parentFilter, String childrenFilter) throws NamespaceException {
    return dataGraphHandler.getDataGraph(datasetPath, version, currentPath, parentFilter, childrenFilter);
  }

  public int getDescendantsCount(NamespaceKey path) {
    try {
      return dataGraphHandler.getDescendantsCount(path);
    } catch (NamespaceException ne) {
      logger.error("Failed to get descendant counts for path " + path);
    }
    return 0;
  }

  public Iterable<DatasetPath> getDescendants(DatasetPath path) throws NamespaceException {
    return dataGraphHandler.getDescendants(path);
  }

  public Job prepareDownload(DatasetPath datasetPath, DatasetVersion datasetVersion, DownloadFormat downloadFormat,
                             int limit, String userName) throws DatasetVersionNotFoundException, IOException {
    // TODO check if user can access this dataset.
    final VirtualDatasetUI vds = getVersion(datasetPath, datasetVersion);
    return datasetDownloadManager.scheduleDownload(datasetPath, vds, downloadFormat, limit, userName);
  }

  public DownloadDataResponse downloadData(DownloadInfo downloadInfo, String userName) throws IOException {
    // TODO check if user can access this dataset.
    return datasetDownloadManager.getDownloadData(downloadInfo);
  }


  /**
   * Storage creator for dataset versions.
   */
  public static class VersionStoreCreator implements StoreCreationFunction<KVStore<VersionDatasetKey, VirtualDatasetVersion>> {

    @Override
    public KVStore<VersionDatasetKey, VirtualDatasetVersion> build(StoreBuildingFactory factory) {
      return factory.<VersionDatasetKey, VirtualDatasetVersion>newStore()
          .name("datasetVersions")
          .keySerializer(VersionDatasetKeySerializer.class)
          .valueSerializer(VirtualDatasetVersionSerializer.class)
          .build();
    }

  }

  /**
   * Directory initializer for downloads
   */
  public static class InitializerImpl implements Initializer<InitializerImpl> {

    private FileSystem fs;
    private URI downloadRootDir;

    @Override
    public InitializerImpl initialize(BindingProvider provider) throws ExecutionSetupException, IllegalArgumentException, IOException {
      final StoragePluginRegistry plugins = provider.lookup(StoragePluginRegistry.class);
      final DremioConfig config = provider.lookup(DremioConfig.class);

      final Configuration fsConf = new Configuration();
      this.downloadRootDir = config.getURI(DremioConfig.DOWNLOADS_PATH_STRING);
      this.fs = FileSystemWrapper.get(downloadRootDir, fsConf);
      fs.mkdirs(new Path(downloadRootDir.getPath()));

      // TODO: Make this SYSTEM_TABLE and update write pattern to write all files as system user.
      final SchemaMutability mutability = SchemaMutability.USER_TABLE;
      FileSystemConfig fileSystemConfig = new FileSystemConfig(downloadRootDir, mutability);
      plugins.createOrUpdate(DATASET_DOWNLOAD_STORAGE_PLUGIN, fileSystemConfig, null, true);
      return this;
    }

    public FileSystem getFileSystem() {
      return fs;
    }

    public URI getDownloadRootDir() {
      return downloadRootDir;
    }

  }

  private static final class VersionDatasetKeySerializer extends Serializer<VersionDatasetKey> {
    public VersionDatasetKeySerializer() {
    }

    @Override
    public String toJson(VersionDatasetKey v) throws IOException {
      return StringSerializer.INSTANCE.toJson(v.toString());
    }

    @Override
    public VersionDatasetKey fromJson(String v) throws IOException {
      return new VersionDatasetKey(StringSerializer.INSTANCE.fromJson(v));
    }

    @Override
    public byte[] convert(VersionDatasetKey v) {
      return StringSerializer.INSTANCE.convert(v.toString());
    }

    @Override
    public VersionDatasetKey revert(byte[] v) {
      return new VersionDatasetKey(StringSerializer.INSTANCE.revert(v));
    }
  }

  private static final class VirtualDatasetVersionSerializer extends Serializer<VirtualDatasetVersion> {
    private final Serializer<VirtualDatasetVersion> serializer = ProtostuffSerializer.of(VirtualDatasetVersion.getSchema());

    public VirtualDatasetVersionSerializer() {
    }

    @Override
    public String toJson(VirtualDatasetVersion v) throws IOException {
      return serializer.toJson(v);
    }

    @Override
    public VirtualDatasetVersion fromJson(String v) throws IOException {
      return serializer.fromJson(v);
    }

    @Override
    public byte[] convert(VirtualDatasetVersion v) {
      return serializer.convert(v);
    }

    @Override
    public VirtualDatasetVersion revert(byte[] v) {
      return serializer.revert(v);
    }
  }

  /**
   * key for versioned dataset store.
   */
  public static class VersionDatasetKey {
    private final DatasetPath path;
    private final DatasetVersion version;

    VersionDatasetKey(DatasetPath path, DatasetVersion version) {
      super();
      this.path = path;
      this.version = version;
    }

    VersionDatasetKey(String s) {
      String[] split = s.split("/");
      // TODO: validate
      this.path = new DatasetPath(split[0]);
      this.version = new DatasetVersion(split[1]);
    }

    @Override
    public String toString() {
      return getPath().toString() + "/" + version;
    }

    public DatasetPath getPath() {
      return path;
    }
  }
}
