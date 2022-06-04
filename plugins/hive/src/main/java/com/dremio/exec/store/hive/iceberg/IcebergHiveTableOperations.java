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
  private final String metadataLocation;

  public IcebergHiveTableOperations(FileIO fileIO, String metadataLocation) {
    this.fileIO = fileIO;
    this.metadataLocation = metadataLocation;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  protected void doRefresh() {
    refreshFromMetadataLocation(metadataLocation, 2);
  }
}
