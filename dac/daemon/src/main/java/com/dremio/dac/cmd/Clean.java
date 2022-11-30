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
package com.dremio.dac.cmd;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.datastore.CoreStoreProviderImpl.StoreWithId;
import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.jobs.ExternalCleaner;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.LocalJobsService.JobsStoreCreator;
import com.dremio.service.jobtelemetry.server.store.LocalProfileStore;
import com.dremio.service.jobtelemetry.server.store.LocalProfileStore.KVProfileStoreCreator;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.annotations.VisibleForTesting;

/**
 * Backup command line.
 */
@AdminCommand(value = "clean", description = "Cleans Dremio metadata")
public class Clean {

  /**
   * Command line options for db stats reporting and cleaning
   */
  @Parameters(separators = "=")
  static final class Options {
    @Parameter(names={"-h", "--help"}, description="show usage", help=true)
    private boolean help = false;

    @Parameter(names= {"-j", "--max-job-days"}, description="delete jobs older than provided number of days", required=false, validateWith=PositiveInteger.class)
    private int maxJobDays = Integer.MAX_VALUE;

    @Parameter(names= {"-p", "--delete-orphan-profiles"}, description="delete orphans profiles in kvstore", required=false)
    private boolean deleteOrphanProfiles = false;

    @Parameter(names= {"-o", "--delete-orphans"}, description="delete orphans records in kvstore (e.g. old splits)", required=false)
    private boolean deleteOrphans = false;

    @Parameter(names= {"-i", "--reindex-data"}, description="reindex data", required=false)
    private boolean reindexData = false;

    @Parameter(names= {"-c", "--compact"}, description="compact kvstore", required=false)
    private boolean compactKvStore = false;

    @Parameter(names = {"-d", "--delete-orphan-datasetversions"}, description = "delete dataset versions older than " +
      "the provided number of days", required = false, validateWith = PositiveInteger.class)
    private int datasetVersionsThresholdDays = Integer.MAX_VALUE;

    /**
     * Validates that value passed for --max-job-days(-j)
     * is positive
     */
    public static class PositiveInteger extends com.beust.jcommander.validators.PositiveInteger {

      /**
       * Validates parameter and throws exception if incorrect value or type for param.
       * Parameter value should be a positive integer
       * @param name Name of parameter
       * @param value Value of parameter
       * @throws ParameterException
       */
      @Override
      public void validate(String name, String value) throws ParameterException {

        try {
          super.validate(name, value);
        } catch (NumberFormatException | ParameterException e) {
          throw new ParameterException("Parameter " + name + " should be a positive integer (found " + value +")");
        }
      }
    }

    public boolean hasActiveOperation() {
      return maxJobDays != Integer.MAX_VALUE
        || datasetVersionsThresholdDays != Integer.MAX_VALUE
        || deleteOrphans
        || reindexData
        || compactKvStore;
    }

    public boolean isHelp() {
      return help;
    }

    public int getMaxJobDays() {
      return maxJobDays;
    }

    public boolean isDeleteOrphans() {
      return deleteOrphans;
    }

    public boolean isReindexData() {
      return reindexData;
    }

    public boolean isCompactKvStore() {
      return compactKvStore;
   }

   public int getDatasetVersionsThresholdDays() {
      return datasetVersionsThresholdDays;
   }

    public static Options parse(String[] cliArgs) {
      Options args = new Options();
      JCommander jc = JCommander.newBuilder()
        .addObject(args)
        .build();
      jc.setProgramName("dremio-admin clean");

      try {
        jc.parse(cliArgs);
      } catch (ParameterException p) {
        AdminLogger.log(p.getMessage());
        jc.usage();
        System.exit(1);
      }

      if(args.help){
        jc.usage();
      }
      return args;
    }
  }

  /**
   * Method to run operation that throws exceptions rather than exiting VM for test purposes.
   * @param args Command line arguments, same as main()
   * @throws Exception
   */
  public static void go(String[] args) throws Exception {
    final DACConfig dacConfig = DACConfig.newConfig();
    final Options options = Options.parse(args);

    if(options.help) {
      return;
    }

    if (!dacConfig.isMaster) {
      throw new UnsupportedOperationException("Cleanup should be run on master node");
    }

    Optional<LocalKVStoreProvider> providerOptional = CmdUtils.getKVStoreProvider(dacConfig.getConfig());
    if (!providerOptional.isPresent()) {
      AdminLogger.log("No KVStore detected.");
      return;
    }


    try (LocalKVStoreProvider provider = providerOptional.get()) {
      provider.start();

      Optional<Provider<OptionManager>> optionManagerProvider = CmdUtils.getOptionManager(provider, dacConfig.getConfig());
      if (!optionManagerProvider.isPresent()) {
        AdminLogger.log("No Option Manager configured.");
        return;
      }

      if (provider.getStores().size() == 0) {
        AdminLogger.log("No store stats available");
      }
      if(options.hasActiveOperation()) {
        AdminLogger.log("Initial Store Status.");
      } else {
        AdminLogger.log("No operation requested. ");
      }

      for(StoreWithId<?, ?> id : provider) {
        KVAdmin admin = id.getStore().getAdmin();
        AdminLogger.log(admin.getStats());
      }

      if(options.deleteOrphans) {
        deleteDatasetOrphans(provider.asLegacy());
        deleteSplitOrphans(provider.asLegacy());
        deleteCollaborationOrphans(provider.asLegacy());
      }

      if(options.maxJobDays < Integer.MAX_VALUE) {
        deleteOldJobsAndProfiles(provider.asLegacy(), options.maxJobDays);
      }

      if (options.deleteOrphanProfiles) {
        deleteOrphanProfiles(provider.asLegacy());
      }

      if (options.datasetVersionsThresholdDays < Integer.MAX_VALUE) {
        deleteOrphanDatasetVersions(optionManagerProvider.get(), provider.asLegacy(),
          options.datasetVersionsThresholdDays);
      }

      if(options.reindexData) {
        reindexData(provider);
      }

      if(options.compactKvStore) {
        compactStore(provider);
      }

      if(options.hasActiveOperation()) {
        AdminLogger.log("\n\nFinal Store Status.");
        for(StoreWithId<?, ?> id : provider.unwrap(LocalKVStoreProvider.class)) {
          KVAdmin admin = id.getStore().getAdmin();
          AdminLogger.log(admin.getStats());
        }
      }

      return;
    }
  }

  public static void main(String[] args) {
    try {
      go(args);
    } catch (Exception e) {
      AdminLogger.log("Failed to complete cleanup.", e);
      System.exit(1);
    }
  }

  /**
   * Offline profile deletion using LocalProfileStore.
   */
  static class OfflineProfileCleaner extends ExternalCleaner {
    private final LegacyKVStoreProvider provider;

    public OfflineProfileCleaner(LegacyKVStoreProvider provider) {
      this.provider = provider;
    }

    @Override
    public void doGo(JobAttempt jobAttempt) {
      LocalProfileStore.deleteOldProfile(provider, AttemptIdUtils.fromString(jobAttempt.getAttemptId()));
    }

  }

  static class OfflineTmpDatasetVersionsCleaner extends ExternalCleaner {
    private static final String TMP_PATH = "tmp";
    private static final String UNTITLED_PATH = "UNTITLED";
    @SuppressWarnings("deprecation")
    private final LegacyKVStoreProvider provider;

    @SuppressWarnings("deprecation")
    OfflineTmpDatasetVersionsCleaner(LegacyKVStoreProvider provider) {
      this.provider = provider;
    }

    @Override
    public void doGo(JobAttempt jobAttempt) {
      if (jobAttempt == null
        || jobAttempt.getInfo() == null
        || jobAttempt.getInfo().getDatasetPathList() == null || jobAttempt.getInfo().getDatasetPathList().isEmpty()
        || jobAttempt.getInfo().getDatasetVersion() == null || jobAttempt.getInfo().getDatasetVersion().isEmpty()) {
        return;
      }
      final List<String> path = new LinkedList<>(jobAttempt.getInfo().getDatasetPathList());
      if (isTmpDatasetVersion(path)) {
        final String version = jobAttempt.getInfo().getDatasetVersion();
        DatasetVersionMutator.deleteDatasetVersion(provider, path, version);
      }
    }

    private boolean isTmpDatasetVersion(List<String> path) {
      return path != null
        && path.size() == 2
        && TMP_PATH.equals(path.get(0))
        && UNTITLED_PATH.equals(path.get(1));
    }

  }

  /**
   * Method to delete jobs and their corresponding profiles older than provided number of maxDays.
   */
  private static void deleteOldJobsAndProfiles(LegacyKVStoreProvider provider, int maxDays) {
    AdminLogger.log("Deleting jobs details, profiles & dataset versions older {} days... ", maxDays);
    AdminLogger.log(deleteOldJobsAndProfiles(provider, maxDays, TimeUnit.DAYS));
  }

  @VisibleForTesting
  protected static String deleteOldJobsAndProfiles(LegacyKVStoreProvider provider, long time, TimeUnit timeUnit) {
    final List<ExternalCleaner> externalCleaners = Arrays.asList(
      new OfflineProfileCleaner(provider),
      new OfflineTmpDatasetVersionsCleaner(provider)
    );
    return LocalJobsService.deleteOldJobsAndDependencies(externalCleaners, provider, timeUnit.toMillis(time));
  }

  /**
   * Method to delete any orphan profiles (i.e a profile without a corresponding entry in the job store).
   */
  private static void deleteOrphanProfiles(LegacyKVStoreProvider provider) {
    AdminLogger.log("Deleting orphan profiles... ");
    long profilesDeleted = 0;
    final LegacyKVStore<AttemptId, QueryProfile> legacyProfileStore = provider.getStore(KVProfileStoreCreator.class);
    final LegacyIndexedStore<JobId, JobResult> legacyJobStore = provider.getStore(JobsStoreCreator.class);

    // full scan of the profile store
    for (Entry<AttemptId, QueryProfile> entry : legacyProfileStore.find()) {
      // convert attempt id to job id (attemptId = "{jobId}/{indexOfAttempt}")
      AttemptId attemptId = entry.getKey();
      JobId jobId = new JobId(AttemptIdUtils.toString(attemptId));
      // if the corresponding job id is not in job store, then it's an orphan
      if (!legacyJobStore.contains(jobId)) {
        legacyProfileStore.delete(attemptId);
        profilesDeleted++;
      }
    }
    AdminLogger.log("Completed. Deleted {} orphan profiles.", profilesDeleted);
  }

  private static void deleteSplitOrphans(LegacyKVStoreProvider provider) {
    AdminLogger.log("Deleting split orphans... ");
    NamespaceServiceImpl service = new NamespaceServiceImpl(provider);
    AdminLogger.log("Completed. Deleted {} orphans.",
      service.deleteSplitOrphans(PartitionChunkId.SplitOrphansRetentionPolicy.KEEP_CURRENT_VERSION_ONLY, true));
  }

  private static void deleteCollaborationOrphans(LegacyKVStoreProvider provider) {
    AdminLogger.log("Deleting collaboration orphans... ");
    AdminLogger.log("Completed. Deleted {} orphans.", CollaborationHelper.pruneOrphans(provider));
  }

  @VisibleForTesting
  protected static void deleteDatasetOrphans(LegacyKVStoreProvider provider) throws NamespaceException {
    AdminLogger.log("Deleting dataset orphans... ");

    int deleted = 0;
    NamespaceServiceImpl service = new NamespaceServiceImpl(provider);
    Set<String> rootPaths = new HashSet<>();
    List<SourceConfig> sourceConfigs = service.getSources();
    for (SourceConfig s: sourceConfigs) {
      rootPaths.add(s.getName());
    }
    List<SpaceConfig> spaceConfigs = service.getSpaces();
    for (SpaceConfig s: spaceConfigs) {
      rootPaths.add(s.getName());
    }
    List<HomeConfig> homeConfigs = service.getHomeSpaces();
    for (HomeConfig h: homeConfigs) {
      rootPaths.add(HomeName.getUserHomePath(h.getOwner()).getName());
    }

    List<DatasetConfig> datasets = service.getDatasets();
    for (DatasetConfig d: datasets) {
      List<String> fullPath = d.getFullPathList();
      if (fullPath.size() > 1 && !rootPaths.contains(fullPath.get(0))) {
        service.deleteEntity(new NamespaceKey(fullPath));
        deleted++;
      }
    }

    AdminLogger.log("Completed. Deleted {} orphans.", deleted);
  }

  private static void reindexData(LocalKVStoreProvider provider) throws Exception {
    for(StoreWithId s : provider) {
      AdminLogger.log("Reindexing {}... ", s.getId());
      s.getStore().getAdmin().reindex();
      AdminLogger.log("Completed.");
    }
  }

  private static void compactStore(LocalKVStoreProvider provider) throws Exception {
    for(StoreWithId s : provider) {
      AdminLogger.log("Compacting {}... ", s.getId());
      s.getStore().getAdmin().compactKeyValues();
      AdminLogger.log("Completed.");
    }
  }

  private static void deleteOrphanDatasetVersions(Provider<OptionManager> optionManagerProvider,
    LegacyKVStoreProvider provider, int daysThreshold) {
    final LegacyKVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> datasetStore =
      provider.getStore(DatasetVersionMutator.VersionStoreCreator.class);

    AdminLogger.log("Deleting dataset versions orphans...");
    long deleted = DatasetVersionMutator.deleteOrphans(optionManagerProvider, datasetStore, daysThreshold);
    AdminLogger.log("Completed. Deleted {} orphan dataset versions.", deleted);
  }

}
