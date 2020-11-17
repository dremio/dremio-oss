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

import static com.dremio.dac.service.search.SearchIndexManager.CONFIG_KEY;

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.util.BackupRestoreUtil;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.services.configuration.ConfigurationStore;

/**
 * Restore command.
 */
@AdminCommand(value = "restore", description = "Restores Dremio metadata and user-uploaded files")
public class Restore {

  /**
   * Command line options for backup and restore
   */
  @Parameters(separators = "=")
  private static final class BackupManagerOptions {
    @Parameter(names={"-h", "--help"}, description="show usage", help=true)
    private boolean help = false;

    @Parameter(names= {"-r", "--restore"}, description="restore dremio metadata (deprecated, always true)")
    private boolean deprecatedRestore;

    @Parameter(names= {"-v", "--verify"}, description="verify backup contents (deprecated, noop)")
    private boolean deprecatedVerify = false;

    @Parameter(names= {"-d", "--backupdir"}, description="backup directory path. for example, /mnt/dremio/backups or hdfs://$namenode:8020/dremio/backups", required=true)
    private String backupDir = null;

    public static BackupManagerOptions parse(String[] cliArgs) {
      BackupManagerOptions args = new BackupManagerOptions();
      JCommander jc = JCommander.newBuilder().addObject(args).build();
      jc.setProgramName("dremio-admin restore");

      try {
        jc.parse(cliArgs);
      } catch (ParameterException p) {
        AdminLogger.log(p.getMessage());
        jc.usage();
        System.exit(1);
      }

      if(args.help){
        jc.usage();
        System.exit(0);
      }
      return args;
    }
  }

  public static void main(String[] args) throws Exception {
    final DACConfig dacConfig = DACConfig.newConfig();
    final BackupManagerOptions options = BackupManagerOptions.parse(args);
    if (!dacConfig.isMaster) {
      AdminLogger.log("Restore should be run on master node");
      System.exit(1);
    }

    Path backupDir = Path.of(options.backupDir);
    FileSystem fs = HadoopFileSystem.get(backupDir, new Configuration());
    try {
      BackupRestoreUtil.RestorationResults restorationResults =
        BackupRestoreUtil.restore(fs, backupDir, dacConfig);
      String backupPath = restorationResults.getStats().getBackupPath();
      long numTables = restorationResults.getStats().getTables();

      AdminLogger.log("Restored from backup at {}, {} dremio tables, {} uploaded files.",
        backupPath, numTables, restorationResults.getStats().getFiles());
      List<Exception> perFileExceptions = restorationResults.getExceptions();
      if (!perFileExceptions.isEmpty()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Per file exceptions:\n");
        for (Exception e : perFileExceptions) {
          builder.append(e.getMessage()).append("\n");
        }
        AdminLogger.log(builder.toString());
        System.exit(1);
      }
    } catch (Exception e) {
      AdminLogger.log("Restore failed. Please make sure the backup is compatible with this version.", e);
      System.exit(1);
    }

    final Optional<LocalKVStoreProvider> providerOptional = CmdUtils.getKVStoreProvider(dacConfig.getConfig());
    // clear searchLastRefresh so that search index can be rebuilt after restore
    if (providerOptional.isPresent()) {
      try (LocalKVStoreProvider provider = providerOptional.get()) {
        provider.start();
        final ConfigurationStore configStore = new ConfigurationStore(provider.asLegacy());
        configStore.delete(CONFIG_KEY);
      } catch (Exception e) {
        AdminLogger.log("Failed to clear catalog search index.", e);
        System.exit(1);
      }
    }
  }

}
