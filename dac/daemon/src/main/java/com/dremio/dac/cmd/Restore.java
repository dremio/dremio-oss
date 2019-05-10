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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.util.BackupRestoreUtil;
import com.dremio.dac.util.BackupRestoreUtil.BackupStats;

/**
 * Restore command.
 */
public class Restore {

  /**
   * Command line options for backup and restore
   */
  @Parameters(separators = "=")
  private static final class BackupManagerOptions {
    @Parameter(names={"-h", "--help"}, description="show usage", help=true)
    private boolean help = false;

    @Parameter(names= {"-r", "--restore"}, description="restore dremio metadata")
    private boolean restore;

    @Parameter(names= {"-v", "--verify"}, description="verify backup contents")
    private boolean verify;

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

  public static void main(String[] args) {
    final DACConfig dacConfig = DACConfig.newConfig();
    final BackupManagerOptions options = BackupManagerOptions.parse(args);
    String action = "";
    try {
      if (!dacConfig.isMaster) {
        throw new UnsupportedOperationException("Restore should be run on master node ");
      }
      Path backupDir = new Path(options.backupDir);
      FileSystem fs = backupDir.getFileSystem(new Configuration());

      if (options.restore) {
        action = "restore";
        BackupStats backupStats =  BackupRestoreUtil.restore(fs, backupDir, dacConfig);
        AdminLogger.log("Restored from backup at {}, dremio tables {}, uploaded files {}", backupStats.getBackupPath(), backupStats.getTables(), backupStats.getFiles());
      } else if (options.verify) {
        action = "verify";
        BackupRestoreUtil.validateBackupDir(fs, backupDir);
        AdminLogger.log("Verified checksum for backup at {}", fs.makeQualified(backupDir).toString());

      } else {
        throw new IllegalArgumentException("Missing option restore (-r) or verify (-v)");
      }
    } catch (Exception e) {
      AdminLogger.log("{} failed", action, e);
      System.exit(1);
    }
  }
}
