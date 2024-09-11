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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.Wrapper;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.NamespaceAlreadyExistsException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundByTimestampException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.plugins.MergeBranchOptions;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.catalog.View;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.projectnessie.model.MergeResponse;

/** Versioning-specific methods for the Catalog interface. */
public interface VersionedPlugin extends Wrapper {
  /** Supported version entity types in Sonar */
  enum EntityType {
    UNKNOWN,
    ICEBERG_TABLE,
    ICEBERG_VIEW,
    FOLDER,
    UDF
  }

  /**
   * Resolves a version context with the underlying versioned catalog server.
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the versioned catalog server does not have a default branch
   *     set
   * @throws ReferenceTypeConflictException If the requested version type does not match the server
   * @throws ReferenceNotFoundByTimestampException If the given reference cannot be found via
   *     timestamp
   */
  ResolvedVersionContext resolveVersionContext(VersionContext versionContext)
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException;

  /**
   * List all table entries under the given path and subpaths for the given version.
   *
   * @param catalogPath Acts as the namespace filter. It will act as the root namespace.
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   * @throws ReferenceNotFoundException If the given reference cannot be found.
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set.
   * @throws ReferenceTypeConflictException If the requested version does not match the server.
   */
  Stream<ExternalNamespaceEntry> listTablesIncludeNested(
      List<String> catalogPath, VersionContext version);

  /**
   * List all view entries under the given path and subpaths for the given version.
   *
   * @param catalogPath Acts as the namespace filter. It will act as the root namespace.
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   * @throws ReferenceNotFoundException If the given reference cannot be found.
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set.
   * @throws ReferenceTypeConflictException If the requested version does not match the server.
   */
  Stream<ExternalNamespaceEntry> listViewsIncludeNested(
      List<String> catalogPath, VersionContext version);

  /** Gets DataplaneTableInfo object that is being used in sys."tables" */
  Stream<DataplaneTableInfo> getAllTableInfo();

  /** Gets DataplaneViewInfo object that is being used in sys.views */
  Stream<DataplaneViewInfo> getAllViewInfo();

  /** Gets Table object that is being used in INFORMATION_SCHEMA."TABLES" */
  Stream<Table> getAllInformationSchemaTableInfo(SearchQuery searchQuery);

  /** Gets View object that is being used in INFORMATION_SCHEMA.VIEWS */
  Stream<View> getAllInformationSchemaViewInfo(SearchQuery searchQuery);

  /** Gets Schema object that is being used in INFORMATION_SCHEMA.SCHEMATA */
  Stream<Schema> getAllInformationSchemaSchemataInfo(SearchQuery searchQuery);

  /** Gets TableSchema object that is being used in INFORMATION_SCHEMA.COLUMNS */
  Stream<TableSchema> getAllInformationSchemaColumnInfo(SearchQuery searchQuery);

  /*
   * Gets the type of object - eg type of Table, type of View etc
   */
  @Nullable
  EntityType getType(List<String> catalogKey, ResolvedVersionContext version);

  /** Gets contentId for the given key and version */
  @Nullable
  String getContentId(List<String> catalogKey, ResolvedVersionContext version);

  /** Checks that a commit hash exists in the server. */
  boolean commitExists(String commitHash);

  /** List all branches. */
  Stream<ReferenceInfo> listBranches();

  /** List all tags. */
  Stream<ReferenceInfo> listTags();

  /** List all references (both branches and tags). */
  Stream<ReferenceInfo> listReferences();

  /**
   * List all changes for the given version.
   *
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists)
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceTypeConflictException If the requested version type does not match the server
   */
  Stream<ChangeInfo> listChanges(VersionContext version);

  /**
   * List only entries under the given path for the given version. Streams entries.
   *
   * @param catalogPath Acts as the namespace filter. It will scope entries to this namespace.
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceTypeConflictException If the requested version type does not match the server
   */
  Stream<ExternalNamespaceEntry> listEntries(List<String> catalogPath, VersionContext version)
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException;

  /**
   * List only entries under the given path for the given version.
   *
   * @param catalogPath Acts as the namespace filter. It will scope entries to this namespace.
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   * @param options Options, including pagination parameters.
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceTypeConflictException If the requested version type does not match the server
   */
  VersionedListResponsePage listEntriesPage(
      List<String> catalogPath, VersionContext version, VersionedListOptions options)
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException;

  /**
   * List all entries under the given path and subpaths for the given version.
   *
   * @param catalogPath Acts as the namespace filter. It will act as the root namespace.
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   * @throws ReferenceNotFoundException If the given reference cannot be found.
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set.
   * @throws ReferenceConflictException If the requested version does not match the server.
   */
  Stream<ExternalNamespaceEntry> listEntriesIncludeNested(
      List<String> catalogPath, VersionContext version)
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException;

  /**
   * Create a namespace by the given path for the given version
   *
   * @param namespaceKey The namespacekey that is used to create a folder in Nessie.
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   * @throws NamespaceAlreadyExistsException If the namespace already exists.
   * @throws ReferenceNotFoundException If the given reference cannot be found.
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set.
   * @throws ReferenceConflictException If the requested version does not match the server.
   */
  void createNamespace(NamespaceKey namespaceKey, VersionContext version);

  /**
   * Create a branch from the given source reference.
   *
   * @param sourceVersion If the version is NOT_SPECIFIED, the default branch is used (if it exists)
   * @throws ReferenceAlreadyExistsException If the reference already exists.
   * @throws ReferenceNotFoundException If the given source reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceTypeConflictException If the requested version type does not match the server
   */
  void createBranch(String branchName, VersionContext sourceVersion);

  /**
   * Create a tag from the given source reference.
   *
   * @param sourceVersion If the version is NOT_SPECIFIED, the default branch is used (if it exists)
   * @throws ReferenceAlreadyExistsException If the reference already exists
   * @throws ReferenceNotFoundException If the given source reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceTypeConflictException If the requested version type does not match the server
   */
  void createTag(String tagName, VersionContext sourceVersion);

  /**
   * Drop the given branch.
   *
   * @throws ReferenceConflictException If the drop has conflict on the given branch
   * @throws ReferenceNotFoundException If the given branch cannot be found
   */
  void dropBranch(String branchName, String branchHash);

  /**
   * Drop the given tag.
   *
   * @throws ReferenceConflictException If the drop has conflict on the given tag
   * @throws ReferenceNotFoundException If the given tag cannot be found
   */
  void dropTag(String tagName, String tagHash);

  /**
   * Merge the source branch into target branch.
   *
   * @param sourceBranchName The source branch we are merging from
   * @param targetBranchName The target branch we are merging into
   * @param mergeBranchOptions Options being used in Nessie's merge branch builder
   * @throws ReferenceConflictException If the target branch hash changes during merging
   * @throws ReferenceNotFoundException If the source/target branch cannot be found
   */
  MergeResponse mergeBranch(
      String sourceBranchName, String targetBranchName, MergeBranchOptions mergeBranchOptions);

  /**
   * Update the reference for the given branch.
   *
   * @param branchName The branch we want to update the reference
   * @param sourceVersion The source reference name
   * @throws ReferenceConflictException If the branch hash or source reference hash changes during
   *     update
   * @throws ReferenceNotFoundException If the given branch or source reference cannot be found
   */
  void assignBranch(String branchName, VersionContext sourceVersion)
      throws ReferenceConflictException, ReferenceNotFoundException;

  /**
   * Update the reference for the given tag.
   *
   * @param tagName The tag we want to update the reference
   * @param sourceVersion The reference we want to update to
   * @throws ReferenceConflictException If the tag hash or source reference hash changes during
   *     update
   * @throws ReferenceNotFoundException If the given tag or source reference cannot be found
   */
  void assignTag(String tagName, VersionContext sourceVersion)
      throws ReferenceConflictException, ReferenceNotFoundException;

  /**
   * Deletes an Empty Folder by the given path for the given version
   *
   * @param namespaceKey The namespacekey that is used to create a folder in Nessie.
   * @param sourceVersion If the version is NOT_SPECIFIED, the default branch is used (if it
   *     exists).
   * @throws ReferenceNotFoundException If the given reference cannot be found.
   * @throws UserException If the requested folder to be deleted is not empty.
   */
  void deleteFolder(NamespaceKey namespaceKey, VersionContext sourceVersion)
      throws ReferenceNotFoundException, UserException;

  /**
   * Gets the default branch for this plugin
   *
   * @throws NoDefaultBranchException If the default branch cannot be found .
   * @throws UnAuthenticatedException If Nessie configured with the plugin is unreachable due to
   *     authentication error.
   */
  String getDefaultBranch() throws NoDefaultBranchException, UnAuthenticatedException;

  /** Gets the name of this plugin */
  String getName();

  /** Returns the catalog Id. */
  default String getCatalogId() {
    return null;
  }

  Optional<FunctionConfig> getFunction(CatalogEntityKey functionKey);

  List<FunctionConfig> getFunctions(VersionContext versionContext);
}
