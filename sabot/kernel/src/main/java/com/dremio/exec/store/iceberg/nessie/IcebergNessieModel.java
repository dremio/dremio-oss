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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.projectnessie.client.api.NessieApiV1;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.io.file.FileSystem;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClientImpl;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Iceberg nessie model
 */
public class IcebergNessieModel extends IcebergBaseModel {
    private final NessieClient nessieClient;
    private final MutablePlugin plugin;

    public IcebergNessieModel(String namespace, Configuration configuration,
                              NessieApiV1 api,
                              FileSystem fs, OperatorContext context,
                              DatasetCatalogGrpcClient datasetCatalogGrpcClient,
                              MutablePlugin plugin) {
        super(namespace, configuration, fs, context, datasetCatalogGrpcClient, plugin);
        this.nessieClient = new NessieClientImpl(api);
        this.plugin = plugin;
    }

  protected IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier) {
    IcebergNessieTableOperations tableOperations = new IcebergNessieTableOperations((context == null ? null : context.getStats()),
      nessieClient,
      new DremioFileIO(fs, context, null, null, null, configuration, plugin),
      ((IcebergNessieTableIdentifier) tableIdentifier));
    return new IcebergNessieCommand(tableIdentifier, configuration, fs, tableOperations, plugin);
  }

    @Override
    public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
        return new IcebergNessieTableIdentifier(namespace, rootFolder);
    }

  @Override
  public void deleteTable(IcebergTableIdentifier tableIdentifier) {
    super.deleteTable(tableIdentifier);

  }

  private void deleteKey(IcebergNessieTableIdentifier icebergNessieTableIdentifier) {
    nessieClient.deleteCatalogEntry(
      getNessieKey(icebergNessieTableIdentifier.getTableIdentifier()),
      getDefaultBranch());
  }

  private List<String> getNessieKey(TableIdentifier tableIdentifier) {
    return Arrays.asList(
      tableIdentifier.namespace().toString(),
      tableIdentifier.name());
  }

  private ResolvedVersionContext getDefaultBranch() {
    try {
      return nessieClient.getDefaultBranch();
    } catch (NoDefaultBranchException e) {
      throw UserException.sourceInBadState(e)
        .message("No default branch set.")
        .buildSilently();
    }
  }

}
