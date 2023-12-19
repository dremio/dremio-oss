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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.ExecConstants.ICEBERG_CATALOG_TYPE_KEY;
import static com.dremio.exec.ExecConstants.ICEBERG_NAMESPACE_KEY;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.iceberg.hadoop.IcebergHadoopModel;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieModel;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Returns appropriate Iceberg model given catalog type
 */
public final class IcebergModelCreator {
  public static final String DREMIO_NESSIE_DEFAULT_NAMESPACE = "dremio.default";

  private IcebergModelCreator() {}

  //TODO  REFACTOR : Remove last param in this method.
  public static IcebergModel createIcebergModel(
    Configuration configuration,
    SabotContext sabotContext,
    FileIO fileIO,
    OperatorContext operatorContext,
    SupportsIcebergMutablePlugin plugin)
  {
    // if parameter is not set then using Hadoop as default
    IcebergCatalogType catalogType = getIcebergCatalogType(configuration, sabotContext);
    String namespace = configuration.get(ICEBERG_NAMESPACE_KEY, DREMIO_NESSIE_DEFAULT_NAMESPACE);

    switch (catalogType) {
      case NESSIE:
        return new IcebergNessieModel(
          sabotContext.getOptionManager(),
          namespace,
          configuration,
          sabotContext.getNessieApiProvider(),
          fileIO,
          operatorContext,
          new DatasetCatalogGrpcClient(sabotContext.getDatasetCatalogBlockingStub().get()), plugin);
      case HADOOP:
        return new IcebergHadoopModel(namespace, configuration, fileIO, operatorContext,
          new DatasetCatalogGrpcClient(sabotContext.getDatasetCatalogBlockingStub().get()), plugin);
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static IcebergCatalogType getIcebergCatalogType(Configuration configuration, SabotContext context) {
    String icebergCatalogType = configuration.get(ICEBERG_CATALOG_TYPE_KEY, IcebergCatalogType.HADOOP.name()).toUpperCase();
    IcebergCatalogType catalogType;
    try {
      catalogType = IcebergCatalogType.valueOf(icebergCatalogType);
    } catch (IllegalArgumentException iae) {
      catalogType = IcebergCatalogType.UNKNOWN;
    }
    return catalogType;
  }
}
