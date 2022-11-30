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
package com.dremio.dac.admin;

import static java.lang.String.format;

import java.security.Principal;

import javax.ws.rs.core.SecurityContext;

import org.apache.hadoop.conf.Configuration;

import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.resource.ExportProfilesParams;
import com.dremio.dac.resource.ExportProfilesResource;
import com.dremio.dac.server.admin.profile.ProfilesExporter;
import com.dremio.dac.util.BackupRestoreUtil;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.server.ContextService;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Singleton LocalAdmin class
 */

public final class LocalAdmin {

  private static final LocalAdmin INSTANCE = new LocalAdmin();
  private DACDaemon daemon = null;

  private LocalAdmin() {}

  public void setDaemon(DACDaemon daemon) {
    this.daemon = daemon;
  }

  public boolean isLocalAdmin() {
    if(daemon == null) {
      return false;
    }
    return daemon != null;
  }

  public static LocalAdmin getInstance() {
    return INSTANCE;
  }

  private LegacyKVStoreProvider getKVStoreProvider() throws UnsupportedOperationException {
    return daemon.getBindingProvider().lookup(ContextService.class).get().getKVStoreProvider();
  }

  private HomeFileTool getHomeFileTool() throws UnsupportedOperationException {
    daemon.getBindingCreator().bindIfUnbound(SecurityContext.class, new SecurityContext() {
      @Override
      public Principal getUserPrincipal() {
        return null;
      }

      @Override
      public boolean isUserInRole(String s) {
        return false;
      }

      @Override
      public boolean isSecure() {
        return false;
      }

      @Override
      public String getAuthenticationScheme() {
        return null;
      }
    });
    return daemon.getBindingProvider().lookup(HomeFileTool.class);
  }

  public void exportProfiles(ExportProfilesParams params)
    throws Exception {
    if (!isLocalAdmin()) {
      throw new UnsupportedOperationException("This operation is only supported to local admin");
    }
    ProfilesExporter exporter = ExportProfilesResource.getExporter(params);
    System.out.println(exporter.export(getKVStoreProvider())
      .retrieveStats(null, null, false));
  }

  public void backup(String path, String binaryStr, String includeProfilesStr) throws Exception {
    if (!isLocalAdmin()) {
      throw new UnsupportedOperationException("This operation is only supported to local admin");
    }
    Path backupDir = Path.of(path);
    final String scheme = backupDir.toURI().getScheme();
    if (scheme == null || "file".equals(scheme)) {
      backupDir = HadoopFileSystem.getLocal(new Configuration()).makeQualified(backupDir);
    }
    final FileSystem fs = HadoopFileSystem.get(backupDir, new Configuration());
    BackupRestoreUtil.checkOrCreateDirectory(fs, backupDir);
    BackupRestoreUtil.BackupOptions options = new BackupRestoreUtil.BackupOptions(path,
      Boolean.parseBoolean(binaryStr), Boolean.parseBoolean(includeProfilesStr));
    BackupRestoreUtil.BackupStats backupStats = BackupRestoreUtil.createBackup(fs, options,
      getKVStoreProvider().unwrap(LocalKVStoreProvider.class), LocalAdmin.getInstance().getHomeFileTool().getConf(),
      null);
    System.out.println(format("Backup created at %s, dremio tables %d, uploaded files %d",
    backupStats.getBackupPath(), backupStats.getTables(), backupStats.getFiles()));
  }
}
