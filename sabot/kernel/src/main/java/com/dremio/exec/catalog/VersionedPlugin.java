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
package com.dremio.exec.catalog;

import java.util.List;
import java.util.stream.Stream;

import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.plugins.ExternalNamespaceEntry;

/**
 * Versioning-specific methods for the Catalog interface.
 */
public interface VersionedPlugin {
  /**
   Supported version entity types in Sonar
   */
  public enum EntityType {
    UNKNOWN,
    ICEBERG_TABLE,
    ICEBERG_VIEW;
  }

  /**
   * Resolves a version context with the underlying versioned catalog server.
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the versioned catalog server does not have a default branch set
   * @throws ReferenceTypeConflictException If the requested version type does not match the server
   */
  ResolvedVersionContext resolveVersionContext(VersionContext versionContext);

  /**
   * List all table entries under the given path and subpaths for the given version.
   *
   * @param catalogPath Acts as the namespace filter. It will act as the root namespace.
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found.
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set.
   * @throws ReferenceTypeConflictException If the requested version does not match the server.
   */
  Stream<ExternalNamespaceEntry> listTablesIncludeNested(List<String> catalogPath, VersionContext version);

  /**
   * List all view entries under the given path and subpaths for the given version.
   *
   * @param catalogPath Acts as the namespace filter. It will act as the root namespace.
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found.
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set.
   * @throws ReferenceTypeConflictException If the requested version does not match the server.
   */
  Stream<ExternalNamespaceEntry> listViewsIncludeNested(List<String> catalogPath, VersionContext version);

  /**
   * Gets the type of object - eg type of Table, type of View etc
   */
  EntityType getType(List<String> key, ResolvedVersionContext version);
}
