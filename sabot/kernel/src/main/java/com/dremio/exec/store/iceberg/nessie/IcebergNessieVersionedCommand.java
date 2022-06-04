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

import org.apache.hadoop.conf.Configuration;

import com.dremio.exec.store.iceberg.model.IcebergBaseCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;

public class IcebergNessieVersionedCommand extends IcebergBaseCommand {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergNessieVersionedCommand.class);
  private final IcebergNessieVersionedTableOperations versionedTableOperations;

  public IcebergNessieVersionedCommand(IcebergTableIdentifier tableIdentifier,
                                       Configuration configuration,
                                       FileSystem fs,
                                       IcebergNessieVersionedTableOperations tableOperations) {
    super(configuration, ((IcebergNessieVersionedTableIdentifier) tableIdentifier).getTableFolder(), fs, tableOperations);
    this.versionedTableOperations = tableOperations;
  }

  public void deleteTable() {
    //super.deleteTable(); // TODO Okay to only delete in Nessie?
    versionedTableOperations.deleteKey();
  }

}
