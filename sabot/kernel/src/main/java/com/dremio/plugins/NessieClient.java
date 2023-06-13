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
package com.dremio.plugins;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.iceberg.view.ViewVersionMetadata;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.model.IcebergView;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.NessieNamespaceAlreadyExistsException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;

/**
 * Client interface to communicate with Nessie.
 */
public interface NessieClient extends AutoCloseable {

  /**
   * Get the default branch.
   *
   * @throws NoDefaultBranchException If there is no default branch on the source
   */
  ResolvedVersionContext getDefaultBranch();

  /**
   * Resolves a version context with the underlying versioned catalog server.
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceTypeConflictException If the requested version type does not match the server
   */
  ResolvedVersionContext resolveVersionContext(VersionContext versionContext);

  /**
   * Executor enabled method for retrieving resolveVersionContext. JobID is used for referencing the context.
   */
  ResolvedVersionContext resolveVersionContext(VersionContext versionContext, String jobId);

  /**
   * Checks that a commit hash exists in Nessie.
   */
  boolean commitExists(String commitHash);

  /**
   * List all branches.
   */
  Stream<ReferenceInfo> listBranches();

  /**
   * List all tags.
   */
  Stream<ReferenceInfo> listTags();

  /**
   * List all references (both branches and tags).
   */
  Stream<ReferenceInfo> listReferences();

  /**
   * List all changes for the given version.
   *
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists)
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceTypeConflictException If the requested version type does not match the server
   */
  Stream<ChangeInfo> listChanges(VersionContext version);

  enum NestingMode {
    INCLUDE_NESTED,
    SAME_DEPTH_ONLY
  }

  /**
   * List all entries under the given path and subpaths for the given version.
   *
   * @param catalogPath Acts as the namespace filter. It will act as the root namespace.
   * @param resolvedVersion If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   * @param nestingMode whether to include nested elements
   * @param contentTypeFilter optional content type to filter for (null or empty means no filtering)
   * @param celFilter optional CEL filter
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found.
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set.
   * @throws ReferenceTypeConflictException If the requested version does not match the server.
   */
  Stream<ExternalNamespaceEntry> listEntries(
    @Nullable List<String> catalogPath,
    ResolvedVersionContext resolvedVersion,
    NestingMode nestingMode,
    @Nullable Set<ExternalNamespaceEntry.Type> contentTypeFilter,
    @Nullable String celFilter
  );

  /**
   * Create a namespace by the given path for the given version.
   *
   * @param namespacePathList the namespace we are going to create.
   * @param version           If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   * @throws NessieNamespaceAlreadyExistsException If the namespace already exists.
   * @throws ReferenceNotFoundException            If the given source reference cannot be found
   * @throws NoDefaultBranchException              If the Nessie server does not have a default branch set
   * @throws ReferenceTypeConflictException        If the requested version type does not match the server
   */
  void createNamespace(List<String> namespacePathList, VersionContext version);

  /**
   * Deletes an empty namespace by the given path for the given version.
   *
   * @param namespacePathList the namespace we are going to delete.
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists).
   *
   * @throws ReferenceNotFoundException If the given source reference cannot be found
   * @throws UserException If the nessie namespace is not empty
   */
  void deleteNamespace(List<String> namespacePathList, VersionContext version);

  /**
   * Create a branch from the given source reference.
   *
   * @param sourceVersion If the version is NOT_SPECIFIED, the default branch is used (if it exists)
   *
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
   *
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
   * @param targetBranchName The target branch we are merging int
   * @throws ReferenceConflictException If the target branch hash changes during merging
   * @throws ReferenceNotFoundException If the source/target branch cannot be found
   */
  void mergeBranch(String sourceBranchName, String targetBranchName);

  /**
   * Update the reference for the given branch.
   *
   * @param branchName The branch we want to update the reference
   * @param sourceVersion The source reference name
   * @throws ReferenceConflictException If the branch hash or source reference hash changes during update
   * @throws ReferenceNotFoundException If the given branch or source reference cannot be found
   */
  void assignBranch(String branchName, VersionContext sourceVersion);

  /**
   * Update the reference for the given tag.
   *
   * @param tagName The tag we want to update the reference
   * @param sourceVersion The source reference name
   * @throws ReferenceConflictException If the tag hash or source reference hash changes during update
   * @throws ReferenceNotFoundException If the given tag or source reference cannot be found
   */
  void assignTag(String tagName, VersionContext sourceVersion);

  /**
   * Gets the metadata location for the given reference.
   *
   * @param catalogKey The catalog key
   * @param version    The source reference name
   * @param jobId      The JobId of the query
   *                   Note : JobId param is only used when Executor calls this API. It sends the jobId to the controlplane
   *                   to  lookup the userId .
   * @throws ReferenceConflictException If the tag hash or source reference hash changes during update
   * @throws ReferenceNotFoundException If the given tag or source reference cannot be found*
   */
  String getMetadataLocation(List<String> catalogKey, ResolvedVersionContext version, String jobId);

  /**
   * Return the dialect for the given view.
   *
   * @param catalogKey The path for the given view
   * @param version The resolved version used as a reference
   * @return Optional<String> containing the dialect if the view's dialect is not null
   */
  Optional<String> getViewDialect(List<String> catalogKey, ResolvedVersionContext version);


  /**
   * Commits to the table.
   *
   * @param catalogKey                The catalog key
   * @param newMetadataLocation       The new metadata location for the give catalog key
   * @param nessieClientTableMetadata The table metadata
   * @param version                   The source reference name
   * @param baseContentId             The content id of the object that we started the commit operation on
   * @param jobId                     The JobId of the query
   * @param userName                  The username executing the query
   *                                  Note : JobId param is only used when Executor calls this API. It sends the jobId to the controlplane
   *                                  to  lookup the userId .
   * @throws ReferenceConflictException If the tag hash or source reference hash changes during update
   * @throws ReferenceNotFoundException If the given tag or source reference cannot be found*
   */
  void commitTable(
    List<String> catalogKey,
    String newMetadataLocation,
    NessieClientTableMetadata nessieClientTableMetadata,
    ResolvedVersionContext version,
    String baseContentId,
    String jobId,
    String userName);

  void commitView(
    List<String> catalogKey,
    String newMetadataLocation,
    IcebergView icebergView,
    ViewVersionMetadata metadata,
    String dialect,
    ResolvedVersionContext version,
    String baseContentId,
    String userName);

  void deleteCatalogEntry(List<String> catalogKey, ResolvedVersionContext version, String userName);

  /**
   *
   * @param tableKey
   * @param version
   * @return Optional<IcebergTable>
   */
  VersionedPlugin.EntityType getVersionedEntityType(List<String> tableKey, ResolvedVersionContext version);

  /**
   * Gets the Content for the given reference.
   *
   * @param tableKey The catalog key
   * @param version  The resolved version context
   * @param jobId    The JobId of the query
   *                 Note : JobId param is only used when Executor calls this API. It sends the jobId to the controlplane
   *                 to  lookup the userId .
   * @return A stable id that remains constant for the lifetime of a versioned object (until it is dropped)
   * This corresponds to the ContentId in Nessie for the table with key = catalogKey at version
   * @throws ReferenceConflictException If the tag hash or source reference hash changes during update
   * @throws ReferenceNotFoundException If the given tag or source reference cannot be found*
   */
  String getContentId(List<String> tableKey, ResolvedVersionContext version, String jobId);

  NessieApi getNessieApi();

  // Overridden to remove 'throws Exception' as per NessieApi interface
  @Override
  void close();
}
