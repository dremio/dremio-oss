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
import org.projectnessie.client.api.NessieApiV1;

import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;


public class IcebergNessieVersionedModel extends IcebergBaseModel {
  private final NessieApiV1 nessieClient;
  private final VersionContext versionContext;
  private final String nessieKey;

  public IcebergNessieVersionedModel(String namespace, Configuration configuration,
                                     final NessieApiV1 nessieClient, FileSystem fs,
                                     OperatorContext context, List<String> dataset,
                                     DatasetCatalogGrpcClient datasetCatalogGrpcClient,
                                     VersionedDatasetAccessOptions versionedDatasetAccessOptions) {
    super(namespace, configuration, fs, context, dataset, datasetCatalogGrpcClient);

    this.nessieClient = nessieClient;
    Preconditions.checkState(versionedDatasetAccessOptions != null);
    Preconditions.checkState(versionedDatasetAccessOptions.isVersionContextSpecified());
    this.versionContext = versionedDatasetAccessOptions.getVersionContext().get();

    this.nessieKey = versionedDatasetAccessOptions.getVersionedTableKey();
  }

  protected IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier) {
    return new IcebergNessieVersionedCommand(tableIdentifier, this.configuration,
      this.nessieClient, fs, context, dataset);
  }

  @Override
  public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
    return new IcebergNessieVersionedTableIdentifier(namespace, rootFolder, versionContext, nessieKey);
  }

}
