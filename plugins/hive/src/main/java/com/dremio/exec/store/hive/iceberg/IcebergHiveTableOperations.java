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

package com.dremio.exec.store.hive.iceberg;

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.io.FileIO;

public class IcebergHiveTableOperations extends BaseMetastoreTableOperations {

  private final FileIO fileIO;
  private final IcebergHiveTableIdentifier hiveTableIdentifier;

  public IcebergHiveTableOperations(FileIO fileIO, IcebergHiveTableIdentifier hiveTableIdentifier) {
    this.fileIO = fileIO;
    this.hiveTableIdentifier = hiveTableIdentifier;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = this.hiveTableIdentifier.getMetadataLocation();
    refreshFromMetadataLocation(metadataLocation, 2);
  }
}
