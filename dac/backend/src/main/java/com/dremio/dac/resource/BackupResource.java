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
package com.dremio.dac.resource;

import java.io.IOException;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.SecurityContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.util.BackupRestoreUtil;
import com.dremio.dac.util.BackupRestoreUtil.BackupStats;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;

/**
 * Trigger backup
 */
@RestResource
@Secured
@RolesAllowed({"admin"})
@Path("/backup")
public class BackupResource {
  private final Provider<HomeFileTool> fileStore;
  private final Provider<KVStoreProvider> kvStoreProviderProvider;

  @Inject
  public BackupResource(
    Provider<KVStoreProvider> kvStoreProviderProvider,
    Provider<HomeFileTool> fileStore,
    Provider<NamespaceService> namespaceService,
    SecurityContext securityContext) {
    this.kvStoreProviderProvider = kvStoreProviderProvider;
    this.fileStore = fileStore;
  }

  @POST
  public BackupStats createBackup(String backupDir) throws IOException, NamespaceException {
    final KVStoreProvider kvStoreProvider = kvStoreProviderProvider.get();
    if (!(kvStoreProvider instanceof LocalKVStoreProvider)) {
      throw new IllegalArgumentException("backups are created only on master node.");
    }

    final org.apache.hadoop.fs.Path backupDirPath = new org.apache.hadoop.fs.Path(backupDir);
    final FileSystem fs = backupDirPath.getFileSystem(new Configuration());
    // Checking if directory already exists and that the daemon can access it
    BackupRestoreUtil.checkOrCreateDirectory(fs, backupDirPath);
    return BackupRestoreUtil.createBackup(fs, backupDirPath, (LocalKVStoreProvider) kvStoreProvider, fileStore.get().getConf());

  }
}
