/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.datastore.CoreStoreProviderImpl.StoreWithId;
import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.LocalJobsService.DeleteResult;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.PartitionChunkId;


/**
 * Backup command line.
 */
public class Clean{

  /**
   * Command line options for db stats reporting and cleaning
   */
  @Parameters(separators = "=")
  static final class Options {
    @Parameter(names={"-h", "--help"}, description="show usage", help=true)
    private boolean help = false;

    @Parameter(names= {"-j", "--max-job-days"}, description="delete jobs older than provided number of days", required=false, validateWith=PositiveInteger.class)
    private int maxJobDays = Integer.MAX_VALUE;

    @Parameter(names= {"-o", "--delete-orphans"}, description="delete orphans records in kvstore (e.g. old splits)", required=false)
    private boolean deleteOrphans = false;

    @Parameter(names= {"-i", "--reindex-data"}, description="reindex data", required=false)
    private boolean reindexData = false;

    @Parameter(names= {"-c", "--compact"}, description="compact kvstore", required=false)
    private boolean compactKvStore = false;

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
      public void validate(String name, String value) throws ParameterException {

        try {
          super.validate(name, value);
        } catch (NumberFormatException | ParameterException e) {
          throw new ParameterException("Parameter " + name + " should be a positive integer (found " + value +")");
        }
      }
    }

    public boolean hasActiveOperation() {
      return maxJobDays != Integer.MAX_VALUE || deleteOrphans || reindexData || compactKvStore;
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


      if(options.hasActiveOperation()) {
        AdminLogger.log("Initial Store Status.");

      } else {
        AdminLogger.log("No operation requested, printing store Stats.");
      }

      for(StoreWithId id : provider) {
        KVAdmin admin = id.getStore().getAdmin();
        AdminLogger.log(admin.getStats());
      }

      if(options.deleteOrphans) {
        deleteSplitOrphans(provider);
        deleteCollaborationOrphans(provider);
      }

      if(options.maxJobDays < Integer.MAX_VALUE) {
        deleteOldJobs(provider, options.maxJobDays);
      }

      if(options.reindexData) {
        reindexData(provider);
      }

      if(options.compactKvStore) {
        compactStore(provider);
      }

      if(options.hasActiveOperation()) {
        AdminLogger.log("\n\nFinal Store Status.");
        for(StoreWithId id : provider) {
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


  private static void deleteOldJobs(LocalKVStoreProvider provider, int maxDays) {
    AdminLogger.log("Deleting jobs details & profiles older {} days... ", maxDays);
    DeleteResult result = LocalJobsService.deleteOldJobs(provider, TimeUnit.DAYS.toMillis(maxDays));
    AdminLogger.log("Completed. Deleted {} jobs and {} profiles.", result.getJobsDeleted(), result.getProfilesDeleted());

  }

  private static void deleteSplitOrphans(LocalKVStoreProvider provider) {
    AdminLogger.log("Deleting split orphans... ");
    NamespaceServiceImpl service = new NamespaceServiceImpl(provider);
    AdminLogger.log("Completed. Deleted {} orphans.",
      service.deleteSplitOrphans(PartitionChunkId.SplitOrphansRetentionPolicy.KEEP_CURRENT_VERSION_ONLY));
  }

  private static void deleteCollaborationOrphans(LocalKVStoreProvider provider) {
    AdminLogger.log("Deleting collaboration orphans... ");
    AdminLogger.log("Completed. Deleted {} orphans.", CollaborationHelper.pruneOrphans(provider));
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

}
