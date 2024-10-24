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

import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import javax.annotation.Nullable;
import javax.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.client.api.NessieApiV2;

/** Iceberg nessie model */
public class IcebergNessieModel extends IcebergBaseModel {

  private final OptionManager optionManager;
  private final Provider<NessieApiV2> nessieApi;
  private final SupportsIcebergMutablePlugin plugin;

  public IcebergNessieModel(
      OptionManager optionManager,
      String namespace,
      Configuration configuration,
      Provider<NessieApiV2> nessieApi,
      FileIO fileIO,
      OperatorContext operatorContext,
      DatasetCatalogGrpcClient datasetCatalogGrpcClient,
      SupportsIcebergMutablePlugin plugin) {
    super(namespace, configuration, fileIO, operatorContext, datasetCatalogGrpcClient, plugin);
    this.optionManager = optionManager;
    this.nessieApi = nessieApi;
    this.plugin = plugin;
  }

  @Override
  protected IcebergCommand getIcebergCommand(
      IcebergTableIdentifier tableIdentifier, @Nullable IcebergCommitOrigin commitOrigin) {
    IcebergNessieTableOperations tableOperations =
        new IcebergNessieTableOperations(
            (operatorContext == null ? null : operatorContext.getStats()),
            nessieApi,
            fileIO,
            ((IcebergNessieTableIdentifier) tableIdentifier),
            commitOrigin,
            optionManager);
    return new IcebergNessieCommand(
        tableIdentifier, configuration, tableOperations, currentQueryId());
  }

  @Override
  protected IcebergCommand getIcebergCommand(
      IcebergTableIdentifier tableIdentifier,
      String tableLocation,
      @Nullable IcebergCommitOrigin commitOrigin) {
    IcebergNessieTableOperations tableOperations =
        new IcebergNessieTableOperations(
            (operatorContext == null ? null : operatorContext.getStats()),
            nessieApi,
            fileIO,
            ((IcebergNessieTableIdentifier) tableIdentifier),
            commitOrigin,
            optionManager);
    return new IcebergNessieCommand(
        tableLocation, configuration, tableOperations, currentQueryId());
  }

  @Override
  public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
    return new IcebergNessieTableIdentifier(namespace, rootFolder);
  }

  @Override
  public void deleteTable(IcebergTableIdentifier tableIdentifier) {
    super.deleteTable(tableIdentifier);
  }

  public Provider<NessieApiV2> getNessieApi() {
    return nessieApi;
  }
}
