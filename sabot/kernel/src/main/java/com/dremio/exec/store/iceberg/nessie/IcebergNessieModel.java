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

import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.projectnessie.client.api.NessieApiV1;

import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Iceberg nessie model
 */
public class IcebergNessieModel extends IcebergBaseModel {
    private final Provider<NessieApiV1> nessieApi;
    private final SupportsIcebergMutablePlugin plugin;

    public IcebergNessieModel(String namespace, Configuration configuration,
                              Provider<NessieApiV1> api,
                              FileSystem fs, OperatorContext context,
                              DatasetCatalogGrpcClient datasetCatalogGrpcClient,
                              SupportsIcebergMutablePlugin plugin) {
        super(namespace, configuration, fs, context, datasetCatalogGrpcClient, plugin);
        this.nessieApi = api;
        this.plugin = plugin;
    }

  @Override
  protected IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier) {
    IcebergNessieTableOperations tableOperations = new IcebergNessieTableOperations((context == null ? null : context.getStats()),
      nessieApi,
      plugin.createIcebergFileIO(fs, context, null, null, null),
      ((IcebergNessieTableIdentifier) tableIdentifier));
    return new IcebergNessieCommand(tableIdentifier, configuration, fs, tableOperations);
  }

    @Override
    public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
        return new IcebergNessieTableIdentifier(namespace, rootFolder);
    }

  @Override
  public void deleteTable(IcebergTableIdentifier tableIdentifier) {
    super.deleteTable(tableIdentifier);

  }
}
