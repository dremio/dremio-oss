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

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Implementation of the NessieClient interface for REST.
  */
public class NessieClientImpl implements NessieClient {
  private static final Logger logger = LoggerFactory.getLogger(NessieClientImpl.class);
  private final NessieApiV1 nessieApi;

  public NessieClientImpl(NessieApiV1 nessieApi) {
    this.nessieApi = nessieApi;
  }

  @Override
  public ResolvedVersionContext getDefaultBranch() throws NoDefaultBranchException {
    try {
      Branch defaultBranch = nessieApi.getDefaultBranch();
      return ResolvedVersionContext.ofBranch(defaultBranch.getName(), defaultBranch.getHash());
    } catch (NessieNotFoundException e) {
      throw new NoDefaultBranchException(e.getCause());
    }
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(VersionContext versionContext)
    throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    Preconditions.checkNotNull(versionContext);

    switch (versionContext.getType()) {
      case UNSPECIFIED:
        return getDefaultBranch();
      case REF:
        // TODO: Handle bare commit case
        Reference ref = getReference(versionContext);
        if (ref instanceof Branch){
          return ResolvedVersionContext.ofBranch(ref.getName(), ref.getHash());
        }
        if (ref instanceof Tag){
          return ResolvedVersionContext.ofTag(ref.getName(), ref.getHash());
        }
        throw new IllegalStateException(String.format("Reference type %s is not supported", ref.getClass().getName()));
      case BRANCH:
        Reference branch = getReference(versionContext);
        if (!(branch instanceof Branch)){
          throw new ReferenceConflictException();
        }
        return ResolvedVersionContext.ofBranch(branch.getName(), branch.getHash());
      case TAG:
        Reference tag = getReference(versionContext);
        if (!(tag instanceof Tag)){
          throw new ReferenceConflictException();
        }
        return ResolvedVersionContext.ofTag(tag.getName(), tag.getHash());
      case BARE_COMMIT:
        // TODO: Can we get a new type for this? Fallthrough to exception for now
      default:
        throw new IllegalStateException("Unexpected value: " + versionContext.getType());
    }
  }

  @Override
  public Stream<ReferenceInfo> listBranches() {
    return nessieApi.getAllReferences()
        .get()
        .getReferences()
        .stream()
        .filter(ref -> ref instanceof Branch)
        .map(ref -> new ReferenceInfo("Branch", ref.getName(), ref.getHash()));
  }

  @Override
  public Stream<ReferenceInfo> listTags() {
    return nessieApi.getAllReferences()
        .get()
        .getReferences()
        .stream()
        .filter(ref -> ref instanceof Tag)
        .map(ref -> new ReferenceInfo("Tag", ref.getName(), ref.getHash()));
  }

  @Override
  public Stream<ChangeInfo> listChanges(VersionContext version)
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
    try {
      return nessieApi.getCommitLog()
          .reference(toRef(resolvedVersion))
          .get()
          .getLogEntries()
          .stream()
          .map(log -> new ChangeInfo(
              log.getCommitMeta().getHash(),
              log.getCommitMeta().getAuthor(),
              (log.getCommitMeta().getAuthorTime() != null) ? log.getCommitMeta().getAuthorTime().toString() : "",
              log.getCommitMeta().getMessage()));
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e.getCause());
    }
  }

  @Override
  public List<ExternalNamespaceEntry> listEntries(List<String> catalogPath, VersionContext version)
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    try {
      ResolvedVersionContext resolvedVersion = resolveVersionContext(version);

      final GetEntriesBuilder requestBuilder = nessieApi.getEntries()
        .reference(toRef(resolvedVersion));

      int depth = (catalogPath != null && !catalogPath.isEmpty())
        ? catalogPath.size() + 1
        : 1;
      requestBuilder.namespaceDepth(depth);

      if (depth > 1) {
        // TODO: Escape "."s within individual path names
        requestBuilder.filter(String.format("entry.namespace.matches('%s(\\\\.|$)')", String.join("\\\\.", catalogPath)));
      }

      return requestBuilder
        .get()
        .getEntries()
        .stream()
        .map(entry -> ExternalNamespaceEntry.of(entry.getType().toString(), entry.getName().getElements()))
        .collect(Collectors.toList());
    } catch (NessieNotFoundException e) {
      throw UserException.dataReadError(e).buildSilently();
    }
  }

  @Override
  public void createBranch(String branchName, VersionContext sourceVersion)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException,
        NoDefaultBranchException, ReferenceConflictException {
    ResolvedVersionContext resolvedSourceVersion = resolveVersionContext(sourceVersion);
    try {
      nessieApi
          .createReference()
          // Note: createReference uses the resolved commit hash to specify the exact commit to create at
          .reference(Branch.of(branchName, resolvedSourceVersion.getCommitHash()))
          .create();
    } catch (NessieConflictException e) {
      /* Note the discrepancy here between NessieConflictException and ReferenceAlreadyExistsException.
       * NessieReferenceAlreadyExistException extends NessieConflictException, but it is the only
       * exception we actually expect to catch here and signals "already exists". Thus, we convert
       * it to the more-precise ReferenceAlreadyExistsException.
       *
       * Note also that createBranch's signature includes "throws ReferenceConflictException", which
       * at first glance would make more sense here. In this case, ReferenceConflictException is
       * only thrown by the above call to resolveVersionContext. This is used to signal that the
       * caller requested a tag but got a branch, or vice versa. */
      throw new ReferenceAlreadyExistsException(e.getCause()); // TODO: Are we sure this should be e.getCause() and not just e? Many places
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e.getCause());
    }
  }

  @Override
  public void createTag(String tagName, VersionContext sourceVersion)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException,
        NoDefaultBranchException, ReferenceConflictException {
    ResolvedVersionContext resolvedSourceVersion = resolveVersionContext(sourceVersion);
    try {
      nessieApi
          .createReference()
          // Note: createReference uses the resolved commit hash to specify the exact commit to create at
          .reference(Tag.of(tagName, resolvedSourceVersion.getCommitHash()))
          .create();
    } catch (NessieConflictException e) {
      /* Note the discrepancy here between NessieConflictException and ReferenceAlreadyExistsException.
       * NessieReferenceAlreadyExistException extends NessieConflictException, but it is the only
       * exception we actually expect to catch here and signals "already exists". Thus, we convert
       * it to the more-precise ReferenceAlreadyExistsException.
       *
       * Note also that createTag's signature includes "throws ReferenceConflictException", which
       * at first glance would make more sense here. In this case, ReferenceConflictException is
       * only thrown by the above call to resolveVersionContext. This is used to signal that the
       * caller requested a tag but got a branch, or vice versa. */
      throw new ReferenceAlreadyExistsException(e.getCause());
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e.getCause());
    }
  }

  @Override
  public void dropBranch(String branchName, String branchHash)
      throws ReferenceConflictException, ReferenceNotFoundException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(branchName));
    Preconditions.checkNotNull(branchHash);
    if (branchHash.isEmpty()) {
      try {
        branchHash = nessieApi.getReference()
            .refName(branchName)
            .get()
            .getHash();
      } catch (NessieNotFoundException e) {
        throw new ReferenceNotFoundException(e.getCause());
      }
    }

    try {
      nessieApi.deleteBranch()
          .branchName(branchName)
          .hash(branchHash)
          .delete();
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e.getCause());
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e.getCause());
    }
  }

  @Override
  public void dropTag(String tagName, String tagHash)
      throws ReferenceConflictException, ReferenceNotFoundException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tagName));
    Preconditions.checkNotNull(tagHash);
    if (tagHash.isEmpty()) {
      try {
        tagHash = nessieApi.getReference()
            .refName(tagName)
            .get()
            .getHash();
      } catch (NessieNotFoundException e) {
        throw new ReferenceNotFoundException(e.getCause());
      }
    }

    try {
      nessieApi.deleteTag()
          .tagName(tagName)
          .hash(tagHash)
          .delete();
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e.getCause());
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e.getCause());
    }
  }

  @Override
  public void mergeBranch(String sourceBranchName, String targetBranchName)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      final String targetHash = nessieApi.getReference().refName(targetBranchName).get().getHash();
      final String sourceHash = nessieApi.getReference().refName(sourceBranchName).get().getHash();

      nessieApi.mergeRefIntoBranch()
          .branchName(targetBranchName)
          .hash(targetHash)
          .fromRefName(sourceBranchName)
          .fromHash(sourceHash)
          .merge();
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e.getCause());
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e.getCause());
    }
  }

  @Override
  public void assignBranch(String branchName, String sourceReferenceName)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      final String branchHash = nessieApi.getReference().refName(branchName).get().getHash();
      final Reference reference =
          nessieApi.getReference().refName(requireNonNull(sourceReferenceName)).get();

      nessieApi
          .assignBranch()
          .branchName(branchName)
          .hash(branchHash)
          .assignTo(reference)
          .assign();
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e.getCause());
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e.getCause());
    }
  }

  @Override
  public void assignTag(String tagName, String sourceReferenceName)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      final String tagHash = nessieApi.getReference().refName(tagName).get().getHash();
      final Reference reference =
          nessieApi.getReference().refName(requireNonNull(sourceReferenceName)).get();

      nessieApi
          .assignTag()
          .tagName(tagName)
          .hash(tagHash)
          .assignTo(reference)
          .assign();
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e.getCause());
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e.getCause());
    }
  }

  @Override
  public String getMetadataLocation(List<String> catalogKey, ResolvedVersionContext version) {
    final ContentKey contentKey = ContentKey.of(catalogKey);
    String metadataLocation = null;
    Content content = null;

    try {
      content = nessieApi.getContent()
          .key(contentKey)
          .reference(toRef(version))
          .get()
          .get(contentKey);
    } catch (NessieNotFoundException e) {
      logger.error("Failed to get metadata location for table: {}", contentKey, e);
      if (e.getErrorCode() == ErrorCode.REFERENCE_NOT_FOUND // TODO: Cleanup
          || e.getErrorCode() != ErrorCode.CONTENT_NOT_FOUND) {
        throw UserException.dataReadError(e).buildSilently();
      }
    }

    if (content instanceof IcebergTable) {
      IcebergTable icebergTable = (IcebergTable) content;
      metadataLocation = icebergTable.getMetadataLocation();
    }
    logger.debug("Metadata location of table: {}, is {}", contentKey, metadataLocation);
    return metadataLocation;
  }

  @Override
  public void commitOperation(
      List<String> catalogKey,
      String newMetadataLocation,
      TableMetadata metadata,
      ResolvedVersionContext version) {
    Preconditions.checkArgument(version.isBranch());
    final ContentKey contentKey = ContentKey.of(catalogKey);
    try {
      nessieApi
          .commitMultipleOperations()
          .branch((Branch) toRef(version))
          .operation(Operation.Put.of(
            contentKey,
            IcebergTable.of(
              newMetadataLocation,
              metadata.currentSnapshot().snapshotId(),
              metadata.currentSchemaId(),
              metadata.defaultSpecId(),
              metadata.sortOrder().orderId())))
          .commitMeta(CommitMeta.fromMessage("Put key: " + contentKey))
          .commit();
    } catch (NessieConflictException e) {
      throw new CommitFailedException(e, "Failed to commit operation");
    } catch (NessieNotFoundException e) {
      throw UserException.dataReadError(e).buildSilently();
    }
  }

  @Override
  public void deleteCatalogEntry(List<String> catalogKey, ResolvedVersionContext version) {
    Preconditions.checkArgument(version.isBranch());

    final Reference versionRef = toRef(version);
    final ContentKey contentKey = ContentKey.of(catalogKey);

    // Check if reference exists to give back a proper error
    // TODO: Get the expected commit from the getContents and provide that to the commitMultipleOperations
    // So the deleteKey is atomic.
    try {
      Map<ContentKey, Content> map = nessieApi.getContent()
          .key(contentKey)
          .refName(versionRef.getName())
          .get();
      Content content = map.get(contentKey);
      if (content == null) {
        throw UserException.validationError()
          .message(String.format("Key not found in nessie for branch [%s] %s",
            version.getRefName(),
            catalogKey))
          .buildSilently();
      }
      logger.debug("Content of table: {}, is {}", contentKey, content.unwrap(IcebergTable.class).get().getMetadataLocation());
    } catch (NessieNotFoundException e) {
      logger.debug("Tried to delete key : {} but it was not found in nessie ", catalogKey);
      throw UserException.validationError(e)
        .message(String.format("Key not found in nessie for  %s", catalogKey))
        .buildSilently();
    }
    try {
      nessieApi
          .commitMultipleOperations()
          .branchName(versionRef.getName())
          .hash(versionRef.getHash())
          .operation(Operation.Delete.of(contentKey))
          .commitMeta(CommitMeta.fromMessage("Deleting key: " + contentKey))
          .commit();
    } catch (NessieNotFoundException e) {
      // TODO: Check why this never gets thrown for a key that does not exist on a branch
      logger.debug("Tried to delete key : {} but it was not found in nessie ", catalogKey);
      throw UserException.validationError(e)
          .message(String.format("Version reference not found in nessie for  %s", catalogKey))
          .buildSilently();
    } catch (NessieConflictException e) {
      logger.debug("The versioned table entry {} could not be removed from Nessie", catalogKey);
      throw UserException.concurrentModificationError(e)
          .message(String.format("Failed to drop table %s", catalogKey))
          .buildSilently();
    }
  }

  private Reference getReference(VersionContext versionContext)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(versionContext);

    Reference reference;
    try {
      reference = nessieApi.getReference()
        .refName(versionContext.getRefName())
        .get();
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e.getCause());
    }

    return reference;
  }

  private Reference toRef(ResolvedVersionContext resolvedVersionContext) {
    Preconditions.checkNotNull(resolvedVersionContext);
    switch (resolvedVersionContext.getType()) {
      case BRANCH:
        return Branch.of(resolvedVersionContext.getRefName(), resolvedVersionContext.getCommitHash());
      case TAG:
        return Tag.of(resolvedVersionContext.getRefName(), resolvedVersionContext.getCommitHash());
      case BARE_COMMIT:
        // TODO: Can we get a new type for this? Fallthrough to exception for now
      default:
        throw new IllegalStateException("Unexpected value: " + resolvedVersionContext.getType());
    }
  }
}
