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

import static java.lang.String.format;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.util.BackupRestoreUtil.BackupStats;

/**
 * Backup command line.
 */
public class Backup {
  private static final MediaType JSON = MediaType.APPLICATION_JSON_TYPE;

  /**
   * Command line options for backup
   */
  @Parameters(separators = "=")
  private static final class BackupManagerOptions {
    @Parameter(names={"-h", "--help"}, description="show usage", help=true)
    private boolean help = false;

    @Parameter(names= {"-d", "--backupdir"}, description="backup directory path. for example, /mnt/dremio/backups or hdfs://$namenode:8020/dremio/backups", required=true)
    private String backupDir = null;

    @Parameter(names= {"-u", "--user"}, description="username (admin)", password=true,
      echoInput=true /* user is prompted when password=true and parameter is required, but passwords are hidden,
       so enable echoing input */)
    private String userName = null;

    @Parameter(names= {"-p", "--password"}, description="password", password=true)
    private String password = null;

    @Parameter(names= {"-a", "--accept-all"}, description="accept all ssl certificates")
    private boolean acceptAll = false;
  }

  public static BackupStats createBackup(
    DACConfig dacConfig,
    String userName,
    String password,
    boolean checkSSLCertificates,
    URI uri)
      throws IOException, GeneralSecurityException {
    final WebClient client = new WebClient(dacConfig, userName, password, checkSSLCertificates);

    return client.buildPost(BackupStats.class, "/backup", uri.toString());
  }

  private static boolean validateOnlineOption(BackupManagerOptions options) {
    return (options.userName != null) && (options.password != null);
  }
  public static void main(String[] args) {
    final DACConfig dacConfig = DACConfig.newConfig();
    final BackupManagerOptions options = new BackupManagerOptions();
    JCommander jc = JCommander.newBuilder().addObject(options).build();
    jc.parse(args);
    if(options.help) {
      jc.usage();
      System.exit(0);
    }

    try {
      if (!dacConfig.isMaster) {
        throw new UnsupportedOperationException("Backup should be ran on master node. ");
      }

      // Make sure that unqualified paths are resolved locally first, and default filesystem
      // is pointing to file
      Path backupDir = new Path(options.backupDir);
      final String scheme = backupDir.toUri().getScheme();
      if (scheme == null || "file".equals(scheme)) {
        backupDir = backupDir.makeQualified(URI.create("file:///"), FileSystem.getLocal(new Configuration()).getWorkingDirectory());
      }

      URI target = backupDir.toUri();

      if (!validateOnlineOption(options)) {
        throw new ParameterException("User credential is required.");
      }
      BackupStats backupStats = createBackup(dacConfig, options.userName, options.password, !options.acceptAll, target);
      System.out.println(format("Backup created at %s, dremio tables %d, uploaded files %d",
        backupStats.getBackupPath(), backupStats.getTables(), backupStats.getFiles()));

    } catch(IOException e) {
      System.err.println(format("Failed to create backup at %s: %s ", options.backupDir, e.getMessage()));
      System.exit(1);
    } catch (Exception e) {
      System.err.println(format("Failed to create backup at %s: %s ", options.backupDir, e));
      System.exit(1);
    }
  }
}
