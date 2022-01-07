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

import org.apache.iceberg.catalog.TableIdentifier;

import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;

public class IcebergNessieVersionedTableIdentifier implements IcebergTableIdentifier {
  private final TableIdentifier tableIdentifier;
  private final String tableFolder;
  private final VersionContext versionContext;
  private final String nessieKey;

  public IcebergNessieVersionedTableIdentifier(String namespace,
                                               String rootFolder,
                                               VersionContext versionContext,
                                               String nessieKey) {
    this.tableFolder = rootFolder;
    this.tableIdentifier = TableIdentifier.of(namespace, rootFolder);
    this.versionContext = versionContext;
    this.nessieKey = nessieKey;

  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public String getTableFolder() {
    return tableFolder;
  }

  public VersionContext getVersionContext() {
    return versionContext;
  }

  public String getNessieKey() {
    return nessieKey;
  }

}
