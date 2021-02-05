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

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.datastore.CoreStoreProviderImpl.StoreWithId;
import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.LocalJobsService.JobsStoreCreator;
import com.dremio.service.jobs.LocalJobsService.ProfileCleanup;
import com.dremio.service.jobtelemetry.server.store.LocalProfileStore;
import com.dremio.service.jobtelemetry.server.store.LocalProfileStore.KVProfileStoreCreator;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.PartitionChunkId;


/**
 * Clean is the class responsible for cleaning Dremio metadata on command line.
 */
@AdminCommand(value = "clean", description = "Cleans Dremio metadata")
public class Clean {

  /**
   * Command line options for db stats reporting and cleaning
   */
  @Parameters(separators = "=")
  static final class Options {
    @Parameter(names = {"-h", "--help"}, description = "show usage", help = true)
    private boolean help = false;

    @Parameter(names = {"-j", "--max-job-days"}, description = "delete jobs older than provided number of days", required = false, validateWith = PositiveInteger.class)
    private int maxJobDays = Integer.MAX_VALUE;

    @Parameter(names = {"-p", "--delete-orphan-profiles"}, description = "delete orphans profiles in kvstore", required = false)
    private boolean deleteOrphanProfiles = false;

    @Parameter(names = {"-o", "--delete-orphans"}, description = "delete orphans records in kvstore (e.g. old splits)", required = false)
    private boolean deleteOrphans = false;

    @Parameter(names = {"-i", "--reindex-data"}, description = "reindex data", required = false)
    private boolean reindexData = false;

    @Parameter(names = {"-c", "--compact"}, description = "compact kvstore", required = false)
    private boolean compactKvStore = false;

    /**
     * PositiveInteger is responsible for validating if a given value defined
     * for the --max-job-days(-j) parameter is positive.
     */
    public static class PositiveInteger extends com.beust.jcommander.validators.PositiveInteger {

      /**
       * Validates if a given parameter value is a positive integer and throws an exception
       * if the value or its type are incorrect.
       *
       * @param name  the name of the parameter
       * @param value the string value of the parameter
       * @throws ParameterException If the defined value is not a positive integer
       */
      @Override
      public void validate(String name, String value) throws ParameterException {

        try {
          super.validate(name, value);
        } catch (NumberFormatException | ParameterException e) {
          throw new ParameterException("Parameter " + name + " should be a positive integer (found " + value + ")");
        }
      }
    }

    /**
     * Returns a flag which indicates if there is any active operation command requested.
     * <p>
     * Returns true if any of the parameters maxJobDays, deleteOrphans, reindexData or compactKvStore
     * were defined by the user clean command.
     *
     * @return a flag which indicates if any active operation command was requested
     */
    public boolean hasActiveOperation() {
      return maxJobDays != Integer.MAX_VALUE || deleteOrphans || reindexData || compactKvStore;
    }

    /**
     * Returns a flag which indicates if the help command was requested.
     *
     * @return a flag which indicates if the help command was requested
     */
    public boolean isHelp() {
      return help;
    }

    /**
     * Gets the maximum number of days that allows older files to be deleted.
     *
     * @return the maximum number of days that allows older files to be deleted
     */
    public int getMaxJobDays() {
      return maxJobDays;
    }

    /**
     * Returns a flag which indicates if split and collaboration orphans should be deleted
     * from the key-value stores.
     *
     * @return a flag which indicates if split and collaboration orphans should be deleted
     */
    public boolean isDeleteOrphans() {
      return deleteOrphans;
    }

    /**
     * Returns a flag which indicates if the keys and values should be re-indexed
     * on the key-value index stores.
     *
     * @return a flag which indicates if keys and values should be re-indexed
     */
    public boolean isReindexData() {
      return reindexData;
    }

    /**
     * Returns a flag which indicates if the keys and values should be compacted
     * on the key-value stores.
     *
     * @return a flag which indicates if keys and values should be compacted
     */
    public boolean isCompactKvStore() {
      return compactKvStore;
    }

    /**
     * Parses all command line parameters to an Options object.
     * <p>
     * If the parse cannot be executed successfully or the command is identified as
     * the --help command, shows the help usage summary output to the user.
     *
     * @param cliArgs the array of command line parameters
     * @return        an Option object representing all parsed command line parameters
     */
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

      if (args.help) {
        jc.usage();
      }
      return args;
    }
  }

  /**
   * Runs the clean command line operation to clean the internal Dremio metadata.
   * <p>
   * Useful for test purposes on operations that throws exceptions to avoid exiting VM.
   *
   * @param args the array of command line parameters
   * @throws UnsupportedOperationException If the clean command is run by a node
   *                                       that is not master
   * @throws Exception                     If any other exception is thrown on the running process
   */
  public static void go(String[] args) throws Exception {
    final DACConfig dacConfig = DACConfig.newConfig();
    final Options options = Options.parse(args);

    if (options.help) {
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

      if (provider.getStores().size() == 0) {
        AdminLogger.log("No store stats available");
      }
      if (options.hasActiveOperation()) {
        AdminLogger.log("Initial Store Status.");
      } else {
        AdminLogger.log("No operation requested. ");
      }

      for (StoreWithId<?, ?> id : provider) {
        KVAdmin admin = id.getStore().getAdmin();
        AdminLogger.log(admin.getStats());
      }

      if (options.deleteOrphans) {
        deleteSplitOrphans(provider.asLegacy());
        deleteCollaborationOrphans(provider.asLegacy());
      }

      if (options.maxJobDays < Integer.MAX_VALUE) {
        deleteOldJobsAndProfiles(provider.asLegacy(), options.maxJobDays);
      }

      if (options.deleteOrphanProfiles) {
        deleteOrphanProfiles(provider.asLegacy());
      }

      if (options.reindexData) {
        reindexData(provider);
      }

      if (options.compactKvStore) {
        compactStore(provider);
      }

      if (options.hasActiveOperation()) {
        AdminLogger.log("\n\nFinal Store Status.");
        for (StoreWithId<?, ?> id : provider.unwrap(LocalKVStoreProvider.class)) {
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
   * OfflineProfileCleanup is responsible for offline profile deletions using the LocalProfileStore.
   */
  private static class OfflineProfileCleanup implements ProfileCleanup {
    private final LegacyKVStoreProvider provider;

    public OfflineProfileCleanup(LegacyKVStoreProvider provider) {
      this.provider = provider;
    }

    /**
     * Deletes specific old profiles, based on a given identifier, from the local profile store.
     *
     * @param attemptId id associated with a profile attempt
     */
    @Override
    public void go(AttemptId attemptId) {
      LocalProfileStore.deleteOldProfile(provider, attemptId);
    }
  }

  /**
   * Deletes jobs and their corresponding profiles that are older than the provided maximum number
   * of days.
   *
   * @param provider the key-value store provider
   * @param maxDays  the maximum number of days that allows older files
   *                 to be deleted
   */
  private static void deleteOldJobsAndProfiles(LegacyKVStoreProvider provider, int maxDays) {
    AdminLogger.log("Deleting jobs details & profiles older {} days... ", maxDays);
    OfflineProfileCleanup offlineProfileCleanup = new OfflineProfileCleanup(provider);
    List<Long> result = LocalJobsService.deleteOldJobsAndProfiles(offlineProfileCleanup, provider, TimeUnit.DAYS.toMillis(maxDays));
    AdminLogger.log("Completed. Deleted {} jobs and {} profiles. Delete profile failures: [{}].", result.get(0), result.get(1), result.get(2));
  }

  /**
   * Deletes any profiles without a corresponding entry in the job key-value store (orphan profiles).
   *
   * @param provider the key-value store provider
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

  /**
   * Deletes any possible stale metadata about dataset splits that need to be cleaned up internally
   * (split orphans).
   *
   * @param provider the key-value store provider
   */
  private static void deleteSplitOrphans(LegacyKVStoreProvider provider) {
    AdminLogger.log("Deleting split orphans... ");
    NamespaceServiceImpl service = new NamespaceServiceImpl(provider);
    AdminLogger.log("Completed. Deleted {} orphans.",
      service.deleteSplitOrphans(PartitionChunkId.SplitOrphansRetentionPolicy.KEEP_CURRENT_VERSION_ONLY));
  }

  /**
   * Deletes any entries that are orphan in the collaboration stores.
   *
   * @param provider the key-value store provider
   */
  private static void deleteCollaborationOrphans(LegacyKVStoreProvider provider) {
    AdminLogger.log("Deleting collaboration orphans... ");
    AdminLogger.log("Completed. Deleted {} orphans.", CollaborationHelper.pruneOrphans(provider));
  }

  /**
   * Reindex any key-value stores that are considered an index store.
   *
   * @param provider the key-value store provider
   */
  private static void reindexData(LocalKVStoreProvider provider) throws Exception {
    for (StoreWithId s : provider) {
      AdminLogger.log("Reindexing {}... ", s.getId());
      s.getStore().getAdmin().reindex();
      AdminLogger.log("Completed.");
    }
  }

  /**
   * Compacts all the keys and values within each store on the provider.
   *
   * @param provider the key-value store provider
   */
  private static void compactStore(LocalKVStoreProvider provider) throws Exception {
    for (StoreWithId s : provider) {
      AdminLogger.log("Compacting {}... ", s.getId());
      s.getStore().getAdmin().compactKeyValues();
      AdminLogger.log("Completed.");
    }
  }
}
