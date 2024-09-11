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
package com.dremio.dac.resource;

import com.dremio.config.DremioConfig;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.util.BackupRestoreUtil;
import com.dremio.dac.util.BackupRestoreUtil.BackupOptions;
import com.dremio.dac.util.BackupRestoreUtil.BackupStats;
import com.dremio.datastore.CheckpointInfo;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.service.namespace.NamespaceException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.apache.hadoop.conf.Configuration;
import org.immutables.value.Value;

/** Trigger backup */
@RestResource
@Secured
@RolesAllowed({"admin"})
@Path("/backup")
public class BackupResource {
  private final Provider<HomeFileTool> fileStore;

  private final Provider<LocalKVStoreProvider> localKVStoreProvider;
  private final DremioConfig dremioConfig;

  @Inject
  public BackupResource(
      DremioConfig dremioConfig,
      Provider<LegacyKVStoreProvider> kvStoreProviderProvider,
      Provider<HomeFileTool> fileStore) {
    this.dremioConfig = dremioConfig;
    this.localKVStoreProvider =
        () -> kvStoreProviderProvider.get().unwrap(LocalKVStoreProvider.class);
    this.fileStore = fileStore;
  }

  @POST
  public BackupStats createBackup(BackupOptions options)
      throws IOException, NamespaceException, GeneralSecurityException {
    final LocalKVStoreProvider kvStoreProvider = getLocalKVStoreProvider();

    final com.dremio.io.file.Path backupDirPath = options.getBackupDirAsPath();
    final FileSystem fs = HadoopFileSystem.get(backupDirPath, new Configuration());
    // Checking if directory already exists and that the daemon can access it
    BackupRestoreUtil.checkOrCreateDirectory(fs, backupDirPath);
    return BackupRestoreUtil.createBackup(
        fs, options, kvStoreProvider, fileStore.get().getConfForBackup(), dremioConfig, null, true);
  }

  @POST
  @Path("/checkpoint")
  public CheckpointInfo prepareCheckpoint(BackupOptions options) throws IOException {
    final LocalKVStoreProvider kvStoreProvider = getLocalKVStoreProvider();
    return BackupRestoreUtil.createCheckpoint(options.getBackupDirAsPath(), kvStoreProvider);
  }

  @POST
  @Path("/files")
  public BackupStats backupFiles(FilesBackupOptions options)
      throws IOException, GeneralSecurityException {
    return BackupRestoreUtil.backupFiles(
        dremioConfig, options.getBackupDestinationDirectory(), fileStore.get());
  }

  private LocalKVStoreProvider getLocalKVStoreProvider() {
    final LocalKVStoreProvider kvStoreProvider = localKVStoreProvider.get();
    if (kvStoreProvider == null) {
      throw new IllegalArgumentException("backups are created only on master node.");
    }
    return kvStoreProvider;
  }

  @Value.Immutable
  @JsonDeserialize(as = ImmutableFilesBackupOptions.class)
  public interface FilesBackupOptions {

    String getBackupDestinationDirectory();

    boolean isIncludeProfiles();
  }
}
