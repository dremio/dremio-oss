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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.server.DACConfig;
import com.dremio.datastore.CoreStoreProviderImpl.StoreWithId;
import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.LocalJobsService.DeleteResult;
import com.dremio.service.namespace.NamespaceServiceImpl;


/**
 * Backup command line.
 */
public class Clean {
  /**
   * Command line options for db stats reporting and cleaning
   */
  @Parameters(separators = "=")
  static final class Options {
    @Parameter(names={"-h", "--help"}, description="show usage", help=true)
    private boolean help = false;

    @Parameter(names= {"-j", "--max-job-days"}, description="delete jobs older than provided number of days", required=false)
    private int maxJobDays = Integer.MAX_VALUE;

    @Parameter(names= {"-o", "--delete-orphans"}, description="delete orphans records in kvstore (e.g. old splits)", required=false)
    private boolean deleteOrphans = false;

    @Parameter(names= {"-i", "--reindex-data"}, description="reindex data", required=false)
    private boolean reindexData = false;

    @Parameter(names= {"-c", "--compact"}, description="compact kvstore", required=false)
    private boolean compactKvStore = false;

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
      jc.parse(cliArgs);
      if(args.help){
        jc.setProgramName("dremio-admin clean");
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

    final String dbDir = dacConfig.getConfig().getString(DremioConfig.DB_PATH_STRING);
    final SabotConfig sabotConfig = dacConfig.getConfig().getSabotConfig();
    final ScanResult classpathScan = ClassPathScanner.fromPrescan(sabotConfig);
    try (final LocalKVStoreProvider provider = new LocalKVStoreProvider(classpathScan, dbDir, false, true, false, true)) {
      provider.start();


      if(options.hasActiveOperation()) {
        System.out.println("Initial Store Status.");
      } else {
        System.out.println("No operation requested, printing store Stats.");
      }

      for(StoreWithId id : provider) {
        KVAdmin admin = id.getStore().getAdmin();
        System.out.println(admin.getStats());
      }

      if(options.deleteOrphans) {
        deleteSplitOrphans(provider);
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
        System.out.println("\n\nFinal Store Status.");
        for(StoreWithId id : provider) {
          KVAdmin admin = id.getStore().getAdmin();
          System.out.println(admin.getStats());
        }
      }

      return;
    }
  }


  public static void main(String[] args) {
    try {
      go(args);
    } catch (Exception e) {
      System.err.println("Failed to complete cleanup.");
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }


  private static void deleteOldJobs(LocalKVStoreProvider provider, int maxDays) {
    System.out.print(String.format("Deleting jobs details & profiles older %d days... ", maxDays));
    DeleteResult result = LocalJobsService.deleteOldJobs(provider, maxDays);
    System.out.println(String.format("Completed. Deleted %d jobs and %d profiles.", result.getJobsDeleted(), result.getProfilesDeleted()));
  }

  private static void deleteSplitOrphans(LocalKVStoreProvider provider) {
    System.out.print("Deleting split orphans... ");
    NamespaceServiceImpl service = new NamespaceServiceImpl(provider);
    System.out.println(String.format("Completed. Deleted %d orphans.", service.deleteSplitOrphans()));
  }

  private static void reindexData(LocalKVStoreProvider provider) throws Exception {
    for(StoreWithId s : provider) {
      System.out.print("Reindexing ");
      System.out.print(s.getId());
      System.out.print("... ");
      s.getStore().getAdmin().reindex();
      System.out.println("Completed.");
    }
  }

  private static void compactStore(LocalKVStoreProvider provider) throws Exception {
    for(StoreWithId s : provider) {
      System.out.print("Compacting ");
      System.out.print(s.getId());
      System.out.print("... ");
      s.getStore().getAdmin().compactKeyValues();
      System.out.println("Completed.");
    }
  }

}
