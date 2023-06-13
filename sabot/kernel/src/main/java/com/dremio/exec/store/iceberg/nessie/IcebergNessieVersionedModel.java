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
package com.dremio.exec.store.iceberg.nessie;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.plugins.NessieClient;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

public class IcebergNessieVersionedModel extends IcebergBaseModel {
  private final List<String> tableKey;
  private final NessieClient nessieClient;
  private ResolvedVersionContext version;
  private final String userName;

  public IcebergNessieVersionedModel(List<String> tableKey,
                                     Configuration fsConf,
                                     FileSystem fs,
                                     final NessieClient nessieClient,
                                     OperatorContext context, // Used to create DremioInputFile (valid only for insert/ctas)
                                     ResolvedVersionContext version,
                                     SupportsIcebergMutablePlugin plugin,
                                     String userName) {
    super(null, fsConf, fs, context, null, plugin);

    this.tableKey = tableKey;
    this.nessieClient = nessieClient;
    this.userName = userName;

    Preconditions.checkNotNull(version);
    this.version = version;
  }

  @Override
  protected IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier) {
    IcebergNessieVersionedTableOperations tableOperations = new IcebergNessieVersionedTableOperations(
      context == null ? null : context.getStats(),
      plugin.createIcebergFileIO(fs, context, null, null, null),
      nessieClient,
      ((IcebergNessieVersionedTableIdentifier) tableIdentifier), getJobId(), userName);

    return new IcebergNessieVersionedCommand(tableIdentifier, configuration,  fs, tableOperations);
  }

  @Override
  public void refreshVersionContext() {
    VersionContext versionContext;
    switch (version.getType()) {
      case BRANCH:
        versionContext = VersionContext.ofBranch(version.getRefName());
        break;
      case TAG:
        versionContext = VersionContext.ofTag(version.getRefName());
        break;
      default:
        throw new UnsupportedOperationException("refreshVersionContext is supported for branch and tag ref types only");
    }
    version = nessieClient.resolveVersionContext(versionContext, getJobId());
  }

  @Override
  public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
    return new IcebergNessieVersionedTableIdentifier(tableKey, rootFolder, version);
  }

  private String getJobId() {
    String jobId = null;

    //context is only available for executors
    if (context != null) {
      jobId = QueryIdHelper.getQueryId(context.getFragmentHandle().getQueryId());
    }
    return jobId;
  }
}
