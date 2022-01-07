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

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.exec.store.iceberg.hadoop.IcebergHadoopModel;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieModel;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedModel;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Returns appropriate Iceberg model given catalog type
 */
public class IcebergModelCreator {
  public static String DREMIO_NESSIE_DEFAULT_NAMESPACE = "dremio.default";

  public static IcebergModel createIcebergModel(
    Configuration configuration,
    SabotContext context,
    FileSystem fs, /* if fs is null it will use Iceberg HadoopFileIO class else it will use DremioFileIO class */
    OperatorContext operatorContext,
    List<String> dataset,
    VersionedDatasetAccessOptions versionedDatasetAccessOptions) {
    // if parameter is not set then using Hadoop as default
    IcebergCatalogType catalogType = getIcebergCatalogType(configuration, context);
    String namespace = configuration.get(ICEBERG_NAMESPACE_KEY, DREMIO_NESSIE_DEFAULT_NAMESPACE);

    boolean versionContextPresent = versionedDatasetAccessOptions != null && versionedDatasetAccessOptions.isVersionContextSpecified();
    if (versionContextPresent) {
      return new IcebergNessieVersionedModel(namespace, configuration, context.getNessieClientProvider().get(),
         fs, operatorContext, dataset,
        new DatasetCatalogGrpcClient(context.getDatasetCatalogBlockingStub().get()), versionedDatasetAccessOptions);
    }

    switch (catalogType) {
      case NESSIE:
        return new IcebergNessieModel(namespace, configuration, context.getNessieContentsApiBlockingStub(),
          context.getNessieTreeApiBlockingStub(), fs, operatorContext, dataset,
          new DatasetCatalogGrpcClient(context.getDatasetCatalogBlockingStub().get()));
      case HADOOP:
        return new IcebergHadoopModel(namespace, configuration, fs, operatorContext, dataset,
          new DatasetCatalogGrpcClient(context.getDatasetCatalogBlockingStub().get()));
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
