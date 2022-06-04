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
package com.dremio.dac.service.datasets;

import static com.dremio.dac.service.datasets.DatasetDownloadManager.DATASET_DOWNLOAD_STORAGE_PLUGIN;
import static com.dremio.dac.util.DatasetsUtil.isTemporaryPath;
import static com.dremio.dac.util.DatasetsUtil.toVirtualDatasetUI;
import static com.dremio.dac.util.DatasetsUtil.toVirtualDatasetVersion;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_ALLPARENTS;
import static com.dremio.service.namespace.dataset.DatasetVersion.MAX_VERSION;
import static com.dremio.service.namespace.dataset.DatasetVersion.MIN_VERSION;
import static java.lang.String.format;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.dac.daemon.DACDaemonModule;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.common.RootEntity.RootType;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.dac.service.errors.DatasetNotFoundException;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStore.LegacyFindByRange;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.options.OptionManager;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.DownloadInfo;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * For creating/updating/deleting of dataset and dataset versions.
 */
public class DatasetVersionMutator {
  private static final Logger logger = LoggerFactory.getLogger(DatasetVersionMutator.class);

  protected final NamespaceService namespaceService;
  private final InitializerRegistry init;
  protected final JobsService jobsService;
  protected final CatalogService catalogService;

  private final LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> datasetVersions;
  protected final OptionManager optionManager;

  @Inject
  public DatasetVersionMutator(
      final InitializerRegistry init,
      final LegacyKVStoreProvider kv,
      final NamespaceService namespaceService,
      final JobsService jobsService,
      final CatalogService catalogService,
      final OptionManager optionManager) {
    this.namespaceService = namespaceService;
    this.jobsService = jobsService;
    this.datasetVersions = kv.getStore(VersionStoreCreator.class);
    this.catalogService = catalogService;
    this.init = init;
    this.optionManager = optionManager;
  }

  public DatasetDownloadManager downloadManager() {
    final FileSystemPlugin<?> downloadPlugin = catalogService.getSource(DATASET_DOWNLOAD_STORAGE_PLUGIN);
    final FileSystemPlugin<InternalFileConf> jobResultsPlugin = catalogService.getSource(DACDaemonModule.JOBS_STORAGEPLUGIN_NAME);
    return new DatasetDownloadManager(jobsService, namespaceService, downloadPlugin.getConfig().getPath(),
      downloadPlugin.getSystemUserFS(), jobResultsPlugin.getConfig().isPdfsBased(), optionManager);
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

  public void put(VirtualDatasetUI ds, NamespaceAttribute... attributes) throws DatasetNotFoundException, NamespaceException {
    DatasetPath path = new DatasetPath(ds.getFullPathList());
    validatePath(path);
    validate(path, ds);
    DatasetConfig datasetConfig = toVirtualDatasetVersion(ds).getDataset();
    namespaceService.addOrUpdateDataset(path.toNamespaceKey(), datasetConfig, attributes);
    ds.setId(datasetConfig.getId().getId());
    ds.setSavedTag(datasetConfig.getTag());
    // Update this version of dataset with new occ version of dataset config from namespace.
    putVersion(ds);
  }

  private void validatePath(DatasetPath path) {
    if (path.getRoot().getRootType() == RootType.TEMP) {
      throw new IllegalArgumentException("can not save dataset in tmp space");
    }
  }

  public VirtualDatasetUI renameDataset(final DatasetPath oldPath, final DatasetPath newPath)
      throws NamespaceException, DatasetNotFoundException, DatasetVersionNotFoundException {
    try {
      validatePath(newPath);
      VirtualDatasetVersion latestVersion = null; // the one that matches in the namespace
      final DatasetConfig datasetConfig =
          namespaceService.renameDataset(oldPath.toNamespaceKey(), newPath.toNamespaceKey());

      final List<VirtualDatasetUI> allVersions = FluentIterable.from(getAllVersions(oldPath)).toList();
      final Map<NameDatasetRef, NameDatasetRef> newPrevLinks = Maps.newHashMap();
      for (final VirtualDatasetUI ds : allVersions) {
        final NameDatasetRef previousVersion = ds.getPreviousVersion();
        if (previousVersion != null) {
          // Only rewrite the path if its equal to the old path.
          final String path = previousVersion.getDatasetPath().equals(oldPath.toString()) ?
            newPath.toString() :
            previousVersion.getDatasetPath();
          newPrevLinks.put(previousVersion,
              new NameDatasetRef(path)
                  .setDatasetVersion(previousVersion.getDatasetVersion()));
        }
      }
      // rename all old versions, link the previous version correctly
      for (final VirtualDatasetUI ds : allVersions) {
        datasetVersions.delete(new VersionDatasetKey(oldPath, ds.getVersion()));
        ds.setName(newPath.getDataset().getName());
        ds.setFullPathList(newPath.toPathList());
        final VirtualDatasetVersion vvds = toVirtualDatasetVersion(ds);
        // get returns null for the first
        vvds.setPreviousVersion(newPrevLinks.get(ds.getPreviousVersion()));
        datasetVersions.put(new VersionDatasetKey(newPath, ds.getVersion()), vvds);
        if (datasetConfig.getVirtualDataset().getVersion()
            .equals(vvds.getDataset().getVirtualDataset().getVersion())) {
          latestVersion = vvds;
        }
      }
      if (latestVersion == null) {
        throw new DatasetNotFoundException(newPath,
          format("Missing version %s after rename.", datasetConfig.getVirtualDataset().getVersion().toString()));
      }
      return toVirtualDatasetUI(latestVersion);
    } catch (NamespaceNotFoundException nfe) {
      throw new DatasetNotFoundException(oldPath, nfe);
    }
  }

  /**
   * For the given path and given version, get the entry from dataset versions store, and transform to
   * {@link VirtualDatasetUI}. If the entry is available in namespace, dataset id and saved version are also set in the
   * returned object. Note that this method does not throw if an entry is found in dataset version store, but not in
   * namespace (entry is not saved).
   *
   * @param path dataset path
   * @param version dataset version
   * @return virtual dataset UI
   * @throws DatasetVersionNotFoundException if dataset is not found in namespace and dataset versions store
   * @throws DatasetNotFoundException if dataset is found in namespace, but not in dataset versions store
   */
  public VirtualDatasetUI getVersion(DatasetPath path, DatasetVersion version)
      throws DatasetVersionNotFoundException, DatasetNotFoundException {
    VirtualDatasetUI virtualDatasetUI = toVirtualDatasetUI(datasetVersions.get(new VersionDatasetKey(path, version)));

    try {
      final DatasetConfig datasetConfig = namespaceService.getDataset(path.toNamespaceKey());
      if (virtualDatasetUI == null) {
        // entry exists in namespace but not in dataset versions; very likely an invalid request
        throw new DatasetNotFoundException(path, String.format("version [%s]", version));
      }

      virtualDatasetUI
          .setId(datasetConfig.getId().getId())
          .setSavedTag(datasetConfig.getTag());
    } catch (final NamespaceException ex) {
      logger.debug("dataset error for {}", path, ex);
    }

    if (virtualDatasetUI == null) {
      throw new DatasetVersionNotFoundException(path, version);
    }
    return virtualDatasetUI;
  }

  public VirtualDatasetVersion getVirtualDatasetVersion(DatasetPath path, DatasetVersion version) {
    return datasetVersions.get(new VersionDatasetKey(path, version));
  }

  public Iterable<VirtualDatasetUI> getAllVersions(DatasetPath path) throws DatasetVersionNotFoundException {
    return Iterables.transform(datasetVersions.find(
        new LegacyFindByRange<>(new VersionDatasetKey(path, MIN_VERSION), false, new VersionDatasetKey(path, MAX_VERSION), false)),
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
          .setSavedTag(datasetConfig.getTag());
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
          .setSavedTag(datasetConfig.getTag());
      return virtualDatasetUI;
    } catch (NamespaceNotFoundException e) {
      throw new DatasetNotFoundException(path, format("Some path not found while looking for dataset %s, version %s.", path.toPathString(), version.toString()), e);
    }
  }

  public void deleteDataset(DatasetPath datasetPath, String namespaceEntityVersion) throws DatasetNotFoundException, NamespaceException {
    try {
      namespaceService.deleteDataset(datasetPath.toNamespaceKey(), namespaceEntityVersion);
    } catch (NamespaceNotFoundException nsnf) {
      throw new DatasetNotFoundException(datasetPath, nsnf);
    }
  }

  public NamespaceService getNamespaceService() {
    return namespaceService;
  }

  /**
   * Get count of datasets depending on given dataset
   * @param path path of saved dataset
   * @return count of all descendants.
   * @throws NamespaceException
   */
  public int getDescendantsCount(NamespaceKey path) {
    try (TimedBlock b = Timer.time("getDescendantCounts")) {
      return namespaceService.getCounts(SearchQueryUtils.newTermQuery(DATASET_ALLPARENTS, path.toString())).get(0);
    } catch(NamespaceException e) {
      logger.error("Failed to get descendant counts for path " + path);
      return 0;
    }
  }

  /**
   * Get list of dataset paths depending on given dataset
   * @param path path of saved dataset
   * @return dataset paths of descendants.
   * @throws NamespaceException
   */
  public Iterable<DatasetPath> getDescendants(DatasetPath path) throws NamespaceException {
    LegacyFindByCondition condition = new LegacyFindByCondition()
      .setCondition(SearchQueryUtils.newTermQuery(DATASET_ALLPARENTS, path.toNamespaceKey().toString()))
      .setLimit(1000);
    return Iterables.transform(namespaceService.find(condition), new Function<Entry<NamespaceKey, NameSpaceContainer>, DatasetPath>() {
      @Override
      public DatasetPath apply(Entry<NamespaceKey, NameSpaceContainer> input) {
        return new DatasetPath(input.getKey().getPathComponents());
      }
    });
  }

  public int getJobsCount(NamespaceKey path) {
    JobCountsRequest.Builder builder = JobCountsRequest.newBuilder();
    builder.addDatasets(VersionedDatasetPath.newBuilder()
        .addAllPath(path.getPathComponents()));
    return jobsService.getJobCounts(builder.build()).getCountList().get(0);
  }

  public long getJobsCount(List<NamespaceKey> datasetPaths) {
    long jobCount = 0;
    final JobCountsRequest.Builder builder = JobCountsRequest.newBuilder();
    datasetPaths.forEach(datasetPath ->
        builder.addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(datasetPath.getPathComponents())));
    for (Integer count : jobsService.getJobCounts(builder.build()).getCountList()) {
      if (count != null) {
        jobCount += count;
      }
    }
    return jobCount;
  }

  public DownloadDataResponse downloadData(DownloadInfo downloadInfo,
                                           List<ArrowFileMetadata> resultMetadataList,
                                           String userName) throws IOException {
    // TODO check if user can access this dataset.
    return downloadManager().getDownloadData(downloadInfo, resultMetadataList);
  }

  /**
   * Deletes a dataset version using the path and version.
   * <p>This method is static to be easily accessible from maintenance tasks.</p>
   *
   * <br>
   * <p><b><em>Be aware that this method can delete any dataset version without
   * verifying if a history will be broken.</em></b></p>
   *
   * @param provider  the {@link LegacyKVStoreProvider} from where the store
   *                  of type {@link VersionStoreCreator} will be obtained.
   * @param path      the dataset path
   * @param version   the version of the dataset
   */
  public static void deleteDatasetVersion(LegacyKVStoreProvider provider, List<String> path, String version) {
    LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> store = provider.getStore(VersionStoreCreator.class);
    deleteDatasetVersion(store, path, version);
  }

  private static void deleteDatasetVersion(LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> store, List<String> path, String version) {
    final VersionDatasetKey key = new VersionDatasetKey(new DatasetPath(path), new DatasetVersion(version));
    deleteDatasetVersion(store, key);
  }

  private static void deleteDatasetVersion(LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> store, VersionDatasetKey datasetKey) {
    store.delete(datasetKey);
  }

  /**
   * Delete orphan dataset versions.
   *
   * <p>This method is static to be easily accessible from maintenance tasks.</p>
   *
   * <p>A dataset version is considered an orphan when:</p>
   * <ul>
   *   <li>if the dataset version is temporary, no Jobs are referencing the
   *   dataset version</li>
   *   <li>if a named dataset is recreated with the same name</li>
   * </ul>
   *
   * @param optionManagerProvider   Jobs KVStore.
   * @param datasetStore            Dataset versions KVStore
   * @param daysThreshold           Dataset versions older than the threshold days
   *                                will be deleted. This threshold is limited
   *                                to be greater than {@link ExecConstants#JOB_MAX_AGE_IN_DAYS}.
   *
   * @return The number of dataset version entries that were deleted.
   */
  public static long deleteOrphans(final Provider<OptionManager> optionManagerProvider,
    final LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> datasetStore, final int daysThreshold) {
    return deleteOrphans(optionManagerProvider, datasetStore, daysThreshold, false);
  }

  @VisibleForTesting
  public static long deleteOrphans(final Provider<OptionManager> optionManagerProvider,
    final LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> datasetStore, final int daysThreshold, boolean unsafe) {

    final OptionManager optionManager = optionManagerProvider.get();
    final long maxJobAgeInDays = unsafe ? daysThreshold : optionManager.getOption(ExecConstants.JOB_MAX_AGE_IN_DAYS);

    // Dremio has a background process that cleans Jobs older than 30 days (configurable).
    // To prevent deleting dataset versions still associated to existing jobs, the threshold is
    // limited to be at least 30 days.
    long threshold = Math.max(maxJobAgeInDays, daysThreshold);

    DatasetPath lastPath = null;
    final Set<DatasetPath> visitedNamedDatasets = new HashSet<>();
    final Set<VersionDatasetKey> namedVdsCandidatesToDelete = new HashSet<>();
    final long now = System.currentTimeMillis();
    long deleteCount = 0;
    for (Entry<VersionDatasetKey, VirtualDatasetVersion> virtualDataset : datasetStore.find()) {
      if (withinThreshold(threshold, now, virtualDataset)) {
        continue;
      }

      final VersionDatasetKey datasetKey = virtualDataset.getKey();

      if (isTemporaryPath(datasetKey.getPath().toPathList())) {
        deleteDatasetVersion(datasetStore, datasetKey);
        ++deleteCount;
      } else if (!visitedNamedDatasets.contains(datasetKey.getPath())) {
        visitedNamedDatasets.add(datasetKey.getPath());

        // Delete the candidates when the dataset path changes.
        // This will prevent to have a huge collection in memory.
        if (!datasetKey.getPath().equals(lastPath)) {
          lastPath = datasetKey.getPath();
          deleteCount += deleteAllOrphans(datasetStore, namedVdsCandidatesToDelete);
          namedVdsCandidatesToDelete.clear();
        }

        VersionDatasetKey start = new VersionDatasetKey(datasetKey.getPath(), MIN_VERSION);
        VersionDatasetKey end = new VersionDatasetKey(datasetKey.getPath(), MAX_VERSION);
        LegacyFindByRange<VersionDatasetKey> range = new LegacyFindByRange<>(start, true, end, true);
        List<Entry<VersionDatasetKey, VirtualDatasetVersion>> allVersionsForPath =
          StreamSupport.stream(datasetStore.find(range).spliterator(), false)
          .collect(Collectors.toList());
        namedVdsCandidatesToDelete.addAll(collectNamedOrphans(allVersionsForPath));
      }
    }

    // Remove the remaining candidates of the last dataset path
    if (!namedVdsCandidatesToDelete.isEmpty()) {
      deleteCount += deleteAllOrphans(datasetStore, namedVdsCandidatesToDelete);
    }

    return deleteCount;
  }

  private static long deleteAllOrphans(LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> datasetStore,
    Set<VersionDatasetKey> namedVdsCandidatesToDelete) {
    long deleteCount = 0;
    for (VersionDatasetKey versionDatasetKey : namedVdsCandidatesToDelete) {
      deleteDatasetVersion(datasetStore, versionDatasetKey.getPath().toPathList(), versionDatasetKey.getVersion().getVersion());
      ++deleteCount;
    }
    return deleteCount;
  }


  private static Collection<VersionDatasetKey> collectNamedOrphans(List<Entry<VersionDatasetKey, VirtualDatasetVersion>> datasetSamePath) {
    List<Entry<VersionDatasetKey, VirtualDatasetVersion>> vds = datasetSamePath.stream()
      .sorted(Comparator.comparingLong(o -> -o.getValue().getDataset().getCreatedAt())).collect(Collectors.toList());

    final Set<VersionDatasetKey> orphans = new HashSet<>(vds.size());
    boolean latestRoot = false;
    for (Entry<VersionDatasetKey, VirtualDatasetVersion> datasetEntry : vds) {
      if (!latestRoot) {
        if (datasetEntry.getValue().getPreviousVersion() == null) {
          latestRoot = true;
        }
        continue;
      }

      orphans.add(datasetEntry.getKey());
    }
    return orphans;
  }

  private static boolean withinThreshold(long daysThreshold, long now,
    Entry<VersionDatasetKey, VirtualDatasetVersion> next) {
    final long daysThresholdMillis = TimeUnit.DAYS.toMillis(daysThreshold);
    final Long dsCreation = next.getValue().getDataset().getCreatedAt();
    return dsCreation != null && now - dsCreation < daysThresholdMillis;
  }

  /**
   * Storage creator for dataset versions.
   */
  public static class VersionStoreCreator implements LegacyKVStoreCreationFunction<VersionDatasetKey, VirtualDatasetVersion> {

    @Override
    public LegacyKVStore<VersionDatasetKey, VirtualDatasetVersion> build(LegacyStoreBuildingFactory factory) {
      return factory.<VersionDatasetKey, VirtualDatasetVersion>newStore()
        .name("datasetVersions")
        .keyFormat(Format.wrapped(VersionDatasetKey.class, VersionDatasetKey::toString, VersionDatasetKey::new, Format.ofString()))
        .valueFormat(Format.ofProtostuff(VirtualDatasetVersion.class))
        .build();
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

    public VersionDatasetKey(String s) {
      final int pos = s.lastIndexOf('/');
      Preconditions.checkArgument(pos != -1, "version dataset key should include path and version separated by '/'");
      this.path = new DatasetPath(s.substring(0, pos));
      this.version = new DatasetVersion(s.substring(pos + 1));
    }

    @Override
    public String toString() {
      return getPath().toString() + "/" + version;
    }

    public DatasetPath getPath() {
      return path;
    }

    public DatasetVersion getVersion() {
      return version;
    }
  }
}
