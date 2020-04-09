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

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.util.BackupRestoreUtil.BackupOptions;
import com.dremio.dac.util.BackupRestoreUtil.BackupStats;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.Path;

/**
 * Backup command line.
 */
@AdminCommand(value =  "backup", description = "Backs up Dremio metadata and user-uploaded files")
public class Backup {
  /**
   * Command line options for backup
   */
  @Parameters(separators = "=")
  static final class BackupManagerOptions {
    @Parameter(names={"-h", "--help"}, description="show usage", help=true)
    private boolean help = false;

    @Parameter(names= {"-d", "--backupdir"}, description="backup directory path. for example, /mnt/dremio/backups or hdfs://$namenode:8020/dremio/backups", required=true)
    private String backupDir = null;

    @Parameter(names= {"-l", "--local-attach"}, description="Attach locally to Dremio JVM to authenticate user. Not compatible with user/password options")
    private boolean localAttach = false;

    @Parameter(names= {"-u", "--user"}, description="username (admin)")
    private String userName = null;

    @Parameter(names= {"-p", "--password"}, description="password", password=true)
    private String password = null;

    @Parameter(names= {"-a", "--accept-all"}, description="accept all ssl certificates")
    private boolean acceptAll = false;

    @Parameter(names= {"-j", "--json"}, description="do json backup (defaults to binary)")
    private boolean json = false;

    @Parameter(names= {"-i", "--include-profiles"}, description="include profiles in backup")
    private boolean profiles = false;

  }

  public static BackupStats createBackup(
    DACConfig dacConfig,
    String userName,
    String password,
    boolean checkSSLCertificates,
    URI uri,
    boolean binary,
    boolean includeProfiles
    ) throws IOException, GeneralSecurityException {
    final WebClient client = new WebClient(dacConfig, userName, password, checkSSLCertificates);
    BackupOptions options = new BackupOptions(uri.toString(), binary, includeProfiles);
    return client.buildPost(BackupStats.class, "/backup", options);
  }

  private static boolean validateOnlineOption(BackupManagerOptions options) {
    return (options.userName != null) && (options.password != null);
  }
  public static void main(String[] args) {
    final DACConfig dacConfig = DACConfig.newConfig();
    final BackupManagerOptions options = new BackupManagerOptions();
    JCommander jc = JCommander.newBuilder().addObject(options).build();
    jc.setProgramName("dremio-admin backup");

    try {
      jc.parse(args);
    } catch (ParameterException p) {
      AdminLogger.log(p.getMessage());
      jc.usage();
      System.exit(1);
    }

    if(options.help) {
      jc.usage();
      System.exit(0);
    }

    if (options.localAttach && (options.userName != null || options.password != null)){
      AdminLogger.log("Do not pass username or password when running in local-attach mode");
      jc.usage();
      System.exit(1);
    }

    try {
      if (!dacConfig.isMaster) {
        throw new UnsupportedOperationException("Backup should be ran on master node. ");
      }

      // Make sure that unqualified paths are resolved locally first, and default filesystem
      // is pointing to file
      Path backupDir = Path.of(options.backupDir);
      final String scheme = backupDir.toURI().getScheme();
      if (scheme == null || "file".equals(scheme)) {
        backupDir = HadoopFileSystem.getLocal(new Configuration()).makeQualified(backupDir);
      }

      URI target = backupDir.toURI();

      if (options.localAttach) {
        String[] backupArgs = {"backup",options.backupDir, Boolean.toString(!options.json), Boolean.toString(options.profiles)};
        try {
          DremioAttach.main(backupArgs);
        } catch (NoClassDefFoundError error) {
          AdminLogger.log("A JDK is required to use local-attach mode. Please make sure JAVA_HOME is correctly configured");
        }
      } else {
        if (options.userName == null)  {
          options.userName = System.console().readLine("username: ");
        }
        if(options.password == null) {
          char[] pwd = System.console().readPassword("password: ");
          options.password = new String(pwd);
        }
        if (!validateOnlineOption(options)) {
          throw new ParameterException("User credential is required.");
        }
        BackupStats backupStats = createBackup(dacConfig, options.userName, options.password, !options.acceptAll, target, !options.json, options.profiles);
        AdminLogger.log("Backup created at {}, dremio tables {}, uploaded files {}",
          backupStats.getBackupPath(), backupStats.getTables(), backupStats.getFiles());
      }


    } catch (Exception e) {
      AdminLogger.log("Failed to create backup at {}:", options.backupDir, e);
      System.exit(1);
    }
  }
}
