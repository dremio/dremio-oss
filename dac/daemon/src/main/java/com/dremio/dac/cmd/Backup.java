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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.dac.resource.BackupResource;
import com.dremio.dac.resource.ImmutableUploadsBackupOptions;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.util.BackupRestoreUtil;
import com.dremio.dac.util.BackupRestoreUtil.BackupOptions;
import com.dremio.dac.util.BackupRestoreUtil.BackupStats;
import com.dremio.datastore.CheckpointInfo;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.services.credentials.CredentialsService;

/**
 * Backup command line.
 */
@AdminCommand(value = "backup", description = "Backs up Dremio metadata and user-uploaded files")
public class Backup {

  private static final Logger LOGGER = LoggerFactory.getLogger(Backup.class);

  /**
   * Command line options for backup
   */
  @Parameters(separators = "=")
  static final class BackupManagerOptions {
    @Parameter(names = { "-h", "--help" }, description = "show usage", help = true)
    private boolean help = false;

    @Parameter(names = { "-d", "--backupdir" }, description = "backup directory path. for example, " +
      "/mnt/dremio/backups or hdfs://$namenode:8020/dremio/backups", required = true)
    private String backupDir = null;

    @Parameter(names = { "-l", "--local-attach" }, description = "Attach locally to Dremio JVM to authenticate user. " +
      "Not compatible with user/password options")
    private boolean localAttach = false;

    @Parameter(names = { "-u", "--user" }, description = "username (admin)")
    private String userName = null;

    @Parameter(names = { "-p", "--password" }, description = "password", password = true)
    private String password = null;

    @Parameter(names = { "-a", "--accept-all" }, description = "accept all ssl certificates")
    private boolean acceptAll = false;

    @Parameter(names = { "-j", "--json" }, description = "do json backup (defaults to binary)")
    private boolean json = false;

    @Parameter(names = { "-i", "--include-profiles" }, description = "include profiles in backup")
    private boolean profiles = false;

    @Parameter(names = { "-s", "--same-process" }, description = "execute backup using the same process as " +
      "dremio-admin and not Dremio Server process. This option should only be used with user/password options",
      hidden = true)
    private boolean sameProcess = false;

    @Parameter(names = {"-c", "--compression"}, description = "choose backup compression method. Available options : " +
      "snappy,lz4.", hidden = true)
    private String compression = "";

    @Parameter(names = {"-t", "--table"}, description = "backup only the table provided. Only works for \"json\" "
      + "backup (this backup cannot be restored)", hidden = true)
    private String table = "";

    @Parameter(names = {"-k", "--key"}, description = "backup only the specified key. The table parameter is "
      + "required when using this parameter. (this backup cannot be restored)", hidden = true)
    private String key = "";

  }

  public static BackupStats createBackup(
    DACConfig dacConfig,
    Provider<CredentialsService> credentialsServiceProvider,
    String userName,
    String password,
    boolean checkSSLCertificates,
    URI uri,
    boolean binary,
    boolean includeProfiles,
    String compression,
    String tableToBackup,
    String key
  ) throws IOException, GeneralSecurityException {
    final WebClient client = new WebClient(dacConfig, credentialsServiceProvider, userName, password,
      checkSSLCertificates);
    BackupOptions options = new BackupOptions(uri.toString(), binary, includeProfiles, compression, tableToBackup, key);
    return client.buildPost(BackupStats.class, "/backup", options);
  }

  static CheckpointInfo createCheckpoint(
    DACConfig dacConfig,
    Provider<CredentialsService> credentialsServiceProvider,
    String userName,
    String password,
    boolean checkSSLCertificates,
    URI uri,
    boolean binary,
    boolean includeProfiles
  ) throws IOException, GeneralSecurityException {
    final WebClient client = new WebClient(dacConfig, credentialsServiceProvider, userName, password,
      checkSSLCertificates);
    BackupOptions options = new BackupOptions(uri.toString(), binary, includeProfiles, "", "", "");
    return client.buildPost(CheckpointInfo.class, "/backup/checkpoint", options);
  }

  static BackupStats uploadsBackup(
    DACConfig dacConfig,
    Provider<CredentialsService> credentialsServiceProvider,
    String userName,
    String password,
    boolean checkSSLCertificates,
    URI uri,
    boolean includeProfiles
  ) throws IOException, GeneralSecurityException {
    final WebClient client = new WebClient(dacConfig, credentialsServiceProvider, userName, password,
      checkSSLCertificates);
    BackupResource.UploadsBackupOptions options = new ImmutableUploadsBackupOptions.Builder()
      .setBackupDestinationDirectory(uri.toString())
      .setIsIncludeProfiles(includeProfiles)
      .build();
    return client.buildPost(BackupStats.class, "/backup/uploads", options);
  }

  private static boolean validateOnlineOption(BackupManagerOptions options) {
    return (options.userName != null) && (options.password != null);
  }

  public static void main(String[] args) {
    try {
      final DACConfig dacConfig = DACConfig.newConfig();
      final BackupResult backupResult = doMain(args, dacConfig);
      int returnCode = backupResult.getExitStatus();
      if (returnCode != 0) {
        System.exit(returnCode);
      }
    } catch (Exception e) {
      AdminLogger.log("Failed to create backup", e);
      System.exit(1);
    }
  }

  public static BackupResult doMain(String[] args, DACConfig dacConfig) {
    final BackupManagerOptions options = new BackupManagerOptions();
    JCommander jc = JCommander.newBuilder().addObject(options).build();
    jc.setProgramName("dremio-admin backup");

    final ImmutableBackupResult.Builder result = new ImmutableBackupResult.Builder();
    try {
      jc.parse(args);
    } catch (ParameterException p) {
      AdminLogger.log(p.getMessage());
      jc.usage();
      return result.setExitStatus(1).build();
    }

    if(options.help) {
      jc.usage();
      return result.setExitStatus(0).build();
    }

    if (options.localAttach && (options.userName != null || options.password != null)) {
      AdminLogger.log("Do not pass username or password when running in local-attach mode");
      jc.usage();
      return result.setExitStatus(1).build();
    }

    final SabotConfig sabotConfig = dacConfig.getConfig().getSabotConfig();
    final ScanResult scanResult = ClassPathScanner.fromPrescan(sabotConfig);
    try (CredentialsService credentialsService = CredentialsService.newInstance(dacConfig.getConfig(), scanResult)) {
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
        LOGGER.info("Running backup attaching to existing Dremio Server processes");
        String[] backupArgs = {"backup",options.backupDir, Boolean.toString(!options.json), Boolean.toString(options.profiles)};
        try {
          DremioAttach.main(backupArgs);
        } catch (NoClassDefFoundError error) {
          AdminLogger.log(
            "A JDK is required to use local-attach mode. Please make sure JAVA_HOME is correctly configured");
        }
      } else {
        if (options.userName == null) {
          options.userName = System.console().readLine("username: ");
        }
        if (options.password == null) {
          char[] pwd = System.console().readPassword("password: ");
          options.password = new String(pwd);
        }
        if (!validateOnlineOption(options)) {
          throw new ParameterException("User credential is required.");
        }
        if (!options.table.isEmpty() && !options.json) {
          throw new ParameterException("One table backup works with json only.");
        }
        if (!options.key.isEmpty() && options.table.isEmpty()) {
          throw new ParameterException("Table parameter required when using the key parameter.");
        }

        final CredentialsService credService = options.acceptAll ? null : credentialsService;
        final boolean checkSSLCertificates = options.acceptAll;

        if (!options.sameProcess) {
          LOGGER.info("Running backup using REST API");
          BackupStats backupStats = createBackup(dacConfig, () -> credService, options.userName, options.password,
            checkSSLCertificates, target, !options.json, options.profiles, options.compression,
            options.table, options.key);
          AdminLogger.log("Backup created at {}, dremio tables {}, uploaded files {}",
            backupStats.getBackupPath(), backupStats.getTables(), backupStats.getFiles());
          result.setBackupStats(backupStats);
        } else {
          LOGGER.info("Running backup using Admin CLI process");
          result.setBackupStats(backupUsingCliProcess(dacConfig, options, credentialsService, target,
            checkSSLCertificates));
        }
      }
      return result.setExitStatus(0).build();
    } catch (Exception e) {
      AdminLogger.log("Failed to create backup at {}:", options.backupDir, e);
      return result.setExitStatus(1).build();
    }
  }

  private static BackupStats backupUsingCliProcess(DACConfig dacConfig, BackupManagerOptions options,
    CredentialsService credentialsService, URI target, boolean checkSSLCertificates) throws Exception {

    CheckpointInfo checkpoint = null;
    try {
      //backup using same process is a 3 steps process: create DB checkpoint, backup uploads and backup DB
      checkpoint = createCheckpoint(dacConfig, () -> credentialsService, options.userName,
        options.password,
        checkSSLCertificates, target, !options.json, !options.profiles);
      AdminLogger.log("Checkpoint created");


      final Path backupDestinationDirPath = Path.of(checkpoint.getBackupDestinationDir());
      final FileSystem fs = HadoopFileSystem.get(backupDestinationDirPath,
        new Configuration());
      final BackupOptions backupOptions = new BackupOptions(checkpoint.getBackupDestinationDir(), !options.json,
        options.profiles, options.compression, options.table, options.key);

      final Optional<LocalKVStoreProvider> optionalKvStoreProvider =
        CmdUtils.getReadOnlyKVStoreProvider(dacConfig.getConfig().withValue(DremioConfig.DB_PATH_STRING,
          checkpoint.getCheckpointPath()));
      if (!optionalKvStoreProvider.isPresent()) {
        throw new IllegalStateException("No KVStore detected");
      }

      try (final LocalKVStoreProvider localKVStoreProvider = optionalKvStoreProvider.get()) {
        localKVStoreProvider.start();
        final BackupStats tablesBackupStats = BackupRestoreUtil.createBackup(fs, backupOptions,
          localKVStoreProvider, null, checkpoint);
        final BackupStats uploadsBackupStats = uploadsBackup(dacConfig, () -> credentialsService, options.userName,
          options.password, checkSSLCertificates, backupDestinationDirPath.toURI(), options.profiles);
        final BackupStats backupStats = merge(uploadsBackupStats, tablesBackupStats);
        AdminLogger.log("Backup created at {}, dremio tables {}, uploaded files {}",
          backupStats.getBackupPath(), backupStats.getTables(), backupStats.getFiles());
        return backupStats;
      }
    } finally {
      if (checkpoint != null && StringUtils.isNotEmpty(checkpoint.getCheckpointPath())) {
        FileUtils.deleteDirectory(new File(checkpoint.getCheckpointPath()));
      }
    }
  }

  private static BackupStats merge(BackupStats uploadStats, BackupStats tablesStats) {
    return new BackupStats(uploadStats.getBackupPath(), tablesStats.getTables(), uploadStats.getFiles());
  }

  @Value.Immutable
  interface BackupResult {

    int getExitStatus();

    Optional<BackupStats> getBackupStats();

  }

}
