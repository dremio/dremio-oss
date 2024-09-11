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

import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.plugins.NessieClient;
import com.google.common.base.Preconditions;
import java.util.function.Function;
import org.apache.iceberg.io.FileIO;

/**
 * Builder for IcebergViewOperations
 *
 * @see com.dremio.exec.store.iceberg.nessie.IcebergViewOperations
 */
public class IcebergViewOperationsBuilder {
  private IcebergViewMetadata.SupportedIcebergViewSpecVersion viewSpecVersion;
  private String viewMetadataWarehouseLocation;
  private NessieClient nessieClient;
  private String userName;
  private String catalogName;
  private FileIO fileIO;
  private Function<String, IcebergViewMetadata> metadataLoader;
  private IcebergNessieFilePathSanitizer pathSanitizer;

  public IcebergViewOperationsBuilder withViewSpecVersion(
      IcebergViewMetadata.SupportedIcebergViewSpecVersion viewSpecVersion) {
    this.viewSpecVersion = viewSpecVersion;
    return this;
  }

  public IcebergViewOperationsBuilder withMetadataLoader(
      Function<String, IcebergViewMetadata> metadataLoader) {
    this.metadataLoader = metadataLoader;
    return this;
  }

  public IcebergViewOperationsBuilder withSanitizer(IcebergNessieFilePathSanitizer pathSanitizer) {
    this.pathSanitizer = pathSanitizer;
    return this;
  }

  public IcebergViewOperationsBuilder withViewMetadataWarehouseLocation(String warehouseLocation) {
    this.viewMetadataWarehouseLocation = warehouseLocation;
    return this;
  }

  public IcebergViewOperationsBuilder withNessieClient(NessieClient nessieClient) {
    this.nessieClient = nessieClient;
    return this;
  }

  public IcebergViewOperationsBuilder withUserName(String userName) {
    this.userName = userName;
    return this;
  }

  public IcebergViewOperationsBuilder withCatalogName(String catalogName) {
    this.catalogName = catalogName;
    return this;
  }

  public IcebergViewOperationsBuilder withFileIO(FileIO fileIO) {
    this.fileIO = fileIO;
    return this;
  }

  public IcebergViewOperationsImpl build() {
    Preconditions.checkNotNull(fileIO, "FileIO must be set for performing View Operation");
    return new IcebergViewOperationsImpl(
        viewSpecVersion,
        viewMetadataWarehouseLocation,
        nessieClient,
        userName,
        catalogName,
        fileIO,
        metadataLoader,
        pathSanitizer);
  }

  public static IcebergViewOperationsBuilder newViewOps() {
    return new IcebergViewOperationsBuilder();
  }
}
