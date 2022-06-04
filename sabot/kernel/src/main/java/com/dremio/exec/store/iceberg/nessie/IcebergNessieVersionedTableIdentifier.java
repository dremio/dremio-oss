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

import org.apache.iceberg.catalog.TableIdentifier;

import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;

/**
 * Holds the information necessary to identify a versioned table in Nessie.
 *
 * For an example query, "SELECT * FROM sourceA.folder1.folder2.tableZ", the
 * source plugin name "sourceA" would be stripped, and the tableKey would be
 * "folder1.folder2.tableZ". Use an example root path of: "/root/path"
 *
 * In Iceberg, the key is split up into Namespace and Name, where Name is the
 * leaf node:
 *   Namespace = folder1.folder2
 *   Name = tableZ
 *
 * In Nessie (for the branch/tag specified by VersionContext):
 *   ContentsKey = folder1.folder2.tableZ
 *
 * In the FileSystem:
 *   Table Path = /root/path/folder1/folder2/tableZ
 */
public class IcebergNessieVersionedTableIdentifier implements IcebergTableIdentifier {
  private final List<String> tableKey;
  private final String rootFolder;
  private final ResolvedVersionContext version;

  /**
   * @param tableKey The list of table key elements.
   *                 From the example above, this would be {"folder1", "folder2", "tableZ"}
   * @param rootFolder The root folder to use. A trailing slash is optional and
   *                   will be added automatically. From the example above,
   *                   this would be "/root/path"
   * @param version The version to use.
   */
  public IcebergNessieVersionedTableIdentifier(List<String> tableKey,
                                               String rootFolder,
                                               ResolvedVersionContext version) {
    if (!rootFolder.endsWith("/")) {
      this.rootFolder = rootFolder + "/";
    } else {
      this.rootFolder = rootFolder;
    }
    this.tableKey = tableKey;
    this.version = version;
  }

  /**
   * @return The table key, e.g. {"folder1", "folder2", "tableZ"}
   */
  public List<String> getTableKey() {
    return tableKey;
  }

  /**
   * @return The Iceberg TableIdentifier which is split into Namespace and Name,
   * e.g. Namespace = folder1.folder2
   *  *   Name = tableZ
   */
  public TableIdentifier getTableIdentifier() {
    return TableIdentifier.of(tableKey.toArray(new String[]{}));
  }

  /**
   * @return The full table path, including the root folder prefix,
   * e.g. "/root/path/folder1/folder2/tableZ"
   */
  public String getTableFolder() {
    return rootFolder + String.join("/", tableKey);
  }

  /**
   * @return The version.
   */
  public ResolvedVersionContext getVersion() {
    return version;
  }
}
