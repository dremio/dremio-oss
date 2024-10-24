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
package com.dremio.exec.store.iceberg.hadoop;

import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergBaseCommand;
import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.sabot.exec.context.OperatorContext;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;

/** Entry point for Hadoop based Iceberg tables */
public class IcebergHadoopModel extends IcebergBaseModel {
  private final SupportsIcebergMutablePlugin plugin;

  public IcebergHadoopModel(SupportsIcebergMutablePlugin plugin) {
    this(
        EMPTY_NAMESPACE,
        plugin.getFsConfCopy(),
        plugin.createIcebergFileIO(plugin.getSystemUserFS(), null, null, null, null),
        null,
        null,
        plugin);
  }

  public IcebergHadoopModel(
      String namespace,
      Configuration configuration,
      FileIO fileIO,
      OperatorContext operatorContext,
      DatasetCatalogGrpcClient datasetCatalogGrpcClient,
      SupportsIcebergMutablePlugin plugin) {
    super(namespace, configuration, fileIO, operatorContext, datasetCatalogGrpcClient, plugin);
    this.plugin = plugin;
  }

  @Override
  protected IcebergCommand getIcebergCommand(
      IcebergTableIdentifier tableIdentifier, @Nullable IcebergCommitOrigin commitOrigin) {
    TableOperations tableOperations =
        new IcebergHadoopTableOperations(
            new Path(((IcebergHadoopTableIdentifier) tableIdentifier).getTableFolder()),
            configuration,
            fileIO);
    return new IcebergBaseCommand(
        configuration,
        ((IcebergHadoopTableIdentifier) tableIdentifier).getTableFolder(),
        tableOperations,
        currentQueryId());
  }

  @Override
  protected IcebergCommand getIcebergCommand(
      IcebergTableIdentifier tableIdentifier,
      String tableLocation,
      @Nullable IcebergCommitOrigin commitOrigin) {
    TableOperations tableOperations =
        new IcebergHadoopTableOperations(
            new Path(((IcebergHadoopTableIdentifier) tableIdentifier).getTableFolder()),
            configuration,
            fileIO);
    return new IcebergBaseCommand(configuration, tableLocation, tableOperations, currentQueryId());
  }

  @Override
  public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
    return new IcebergHadoopTableIdentifier(namespace, rootFolder);
  }
}
