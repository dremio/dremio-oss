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

/**
 * Iceberg Nessie catalog
 */
class IcebergNessieCommand extends IcebergBaseCommand {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergNessieCommand.class);
  private final IcebergNessieTableOperations nessieTableOperations;

  public IcebergNessieCommand(IcebergTableIdentifier tableIdentifier,
                              Configuration configuration,
                              FileSystem fs,
                              IcebergNessieTableOperations tableOperations) {
    super(configuration, ((IcebergNessieTableIdentifier) tableIdentifier).getTableFolder(), fs, tableOperations);
    this.nessieTableOperations = tableOperations;
  }

  @Override
  public void deleteTable() {
    RuntimeException ex = null;
    try {
      nessieTableOperations.deleteKey();
    } catch (RuntimeException e) {
      ex = e;
    } finally {
      try {
        super.deleteTable();
      } catch (RuntimeException e) {
        if (ex != null) {
          e.addSuppressed(ex);
        }
        ex = e;
      }
    }

    if (ex != null) {
      throw ex;
    }
  }


  @Override
  public void deleteTableRootPointer(){
    nessieTableOperations.deleteKey();
  }


}
