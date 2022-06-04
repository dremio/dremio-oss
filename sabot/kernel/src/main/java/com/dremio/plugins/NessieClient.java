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
import java.util.stream.Stream;

import org.apache.iceberg.TableMetadata;

import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundException;

/**
 * Client interface to communicate with Nessie.
 */
public interface NessieClient {

  /**
   * Get the default branch.
   *
   * @throws NoDefaultBranchException If there is no default branch on the source
   */
  ResolvedVersionContext getDefaultBranch() throws NoDefaultBranchException;

  /**
   * Resolves a version context with the underlying versioned catalog server.
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceConflictException If the requested version does not match the server
   */
  ResolvedVersionContext resolveVersionContext(VersionContext versionContext)
    throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException;

  /**
   * List all branches.
   */
  Stream<ReferenceInfo> listBranches();

  /**
   * List all tags.
   */
  Stream<ReferenceInfo> listTags();

  /**
   * List all changes for the given version.
   *
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists)
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceConflictException If the requested version does not match the server
   */
  Stream<ChangeInfo> listChanges(VersionContext version)
    throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException;

  /**
   * List entries under a given path for the given version.
   *
   * @param version If the version is NOT_SPECIFIED, the default branch is used (if it exists)
   *
   * @throws ReferenceNotFoundException If the given reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceConflictException If the requested version does not match the server
   */
  List<ExternalNamespaceEntry> listEntries(List<String> catalogPath, VersionContext version)
    throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException;

  /**
   * Create a branch from the given source reference.
   *
   * @param sourceVersion If the version is NOT_SPECIFIED, the default branch is used (if it exists)
   *
   * @throws ReferenceAlreadyExistsException If the reference already exists.
   * @throws ReferenceNotFoundException If the given source reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceConflictException If the requested version does not match the server
   */
  void createBranch(String branchName, VersionContext sourceVersion)
    throws ReferenceAlreadyExistsException, ReferenceNotFoundException,
      NoDefaultBranchException, ReferenceConflictException;

  /**
   * Create a tag from the given source reference.
   *
   * @param sourceVersion If the version is NOT_SPECIFIED, the default branch is used (if it exists)
   *
   * @throws ReferenceAlreadyExistsException If the reference already exists
   * @throws ReferenceNotFoundException If the given source reference cannot be found
   * @throws NoDefaultBranchException If the Nessie server does not have a default branch set
   * @throws ReferenceConflictException If the requested version does not match the server
   */
  void createTag(String tagName, VersionContext sourceVersion)
    throws ReferenceAlreadyExistsException, ReferenceNotFoundException,
      NoDefaultBranchException, ReferenceConflictException;

  /**
   * Drop the given branch.
   *
   * @throws ReferenceConflictException If the drop has conflict on the given branch
   * @throws ReferenceNotFoundException If the given branch cannot be found
   */
  void dropBranch(String branchName, String branchHash)
      throws ReferenceConflictException, ReferenceNotFoundException;

  /**
   * Drop the given tag.
   *
   * @throws ReferenceConflictException If the drop has conflict on the given tag
   * @throws ReferenceNotFoundException If the given tag cannot be found
   */
  void dropTag(String tagName, String tagHash)
      throws ReferenceConflictException, ReferenceNotFoundException;

  /**
   * Merge the source branch into target branch.
   *
   * @param sourceBranchName The source branch we are merging from
   * @param targetBranchName The target branch we are merging int
   * @throws ReferenceConflictException If the target branch hash changes during merging
   * @throws ReferenceNotFoundException If the source/target branch cannot be found
   */
  void mergeBranch(String sourceBranchName, String targetBranchName)
      throws ReferenceConflictException, ReferenceNotFoundException;

  /**
   * Update the reference for the given branch.
   *
   * @param branchName The branch we want to update the reference
   * @param sourceReferenceName The source reference name
   * @throws ReferenceConflictException If the branch hash or source reference hash changes during update
   * @throws ReferenceNotFoundException If the given branch or source reference cannot be found
   */
  void assignBranch(String branchName, String sourceReferenceName)
      throws ReferenceConflictException, ReferenceNotFoundException;

  /**
   * Update the reference for the given tag.
   *
   * @param tagName The tag we want to update the reference
   * @param sourceReferenceName The source reference name
   * @throws ReferenceConflictException If the tag hash or source reference hash changes during update
   * @throws ReferenceNotFoundException If the given tag or source reference cannot be found
   */
  void assignTag(String tagName, String sourceReferenceName)
      throws ReferenceConflictException, ReferenceNotFoundException;

  String getMetadataLocation(List<String> catalogKey, ResolvedVersionContext version);

  void commitOperation(List<String> catalogKey, String newMetadataLocation, TableMetadata metadata, ResolvedVersionContext version);

  void deleteCatalogEntry(List<String> catalogKey, ResolvedVersionContext version);
}
