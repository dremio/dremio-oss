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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.context.RequestContext;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.NessieNamespaceAlreadyExistsException;
import com.dremio.exec.store.NessieUnAuthorizedException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.telemetry.api.metrics.MetricsInstrumenter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.opentelemetry.extension.annotations.WithSpan;

/**
 * Implementation of the NessieClient interface for REST.
  */
public class NessieClientImpl implements NessieClient {

  private static final Logger logger = LoggerFactory.getLogger(NessieClientImpl.class);
  private static final String DETACHED = "DETACHED";
  private static final String SQL_TEXT = "N/A";

  private final NessieApiV1 nessieApi;
  private RequestContext requestContext = RequestContext.current();
  private static final MetricsInstrumenter metrics = new MetricsInstrumenter(NessieClient.class);

  private final LoadingCache<ImmutablePair<ContentKey, ResolvedVersionContext>, Content> nessieContentsCache = CacheBuilder
    .newBuilder()
    .maximumSize(1000) // items
    .softValues()
    .expireAfterAccess(1, TimeUnit.HOURS)
    .build(new NessieContentsCacheLoader());

  public NessieClientImpl(NessieApiV1 nessieApi) {
    this.nessieApi = nessieApi;
  }

  public NessieClientImpl(NessieApiV1 nessieApi, RequestContext requestContext) {
    this.nessieApi = nessieApi;
    this.requestContext = requestContext;
  }

  @Override
  @WithSpan
  public ResolvedVersionContext getDefaultBranch() {
    try {
      return requestContext.call(() -> {
        Branch defaultBranch = nessieApi.getDefaultBranch();
        return ResolvedVersionContext.ofBranch(defaultBranch.getName(), defaultBranch.getHash());
      });
    } catch (NessieNotFoundException e) {
      throw new NoDefaultBranchException(e);
    } catch (NessieNotAuthorizedException e) {
      throw new NessieUnAuthorizedException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public ResolvedVersionContext resolveVersionContext(VersionContext versionContext) {
    try {
      return requestContext.call(() -> {
        Preconditions.checkNotNull(versionContext);
        return metrics.log("resolveVersionContext", () -> resolveVersionContextHelper(versionContext));
      });
    } catch (IllegalStateException | ReferenceTypeConflictException | ReferenceNotFoundException | NoDefaultBranchException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ResolvedVersionContext resolveVersionContextHelper(VersionContext versionContext) {
    switch (versionContext.getType()) {
      case UNSPECIFIED:
        return getDefaultBranch();
      case REF:
        if (matchesCommitPattern(versionContext.getValue())) {
          return ResolvedVersionContext.ofBareCommit(versionContext.getValue());
        }
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
          throw new ReferenceTypeConflictException();
        }
        return ResolvedVersionContext.ofBranch(branch.getName(), branch.getHash());
      case TAG:
        Reference tag = getReference(versionContext);
        if (!(tag instanceof Tag)){
          throw new ReferenceTypeConflictException();
        }
        return ResolvedVersionContext.ofTag(tag.getName(), tag.getHash());
      case BARE_COMMIT:
        return ResolvedVersionContext.ofBareCommit(versionContext.getValue());
      default:
        throw new IllegalStateException("Unexpected value: " + versionContext.getType());
    }
  }

  /**
   * Note: Nessie does not provide a published specification for their commit hashes, so this
   * function is based only on implementation details and may be subject to change.
   *
   * See model/src/main/java/org/projectnessie/model/Validation.java in Nessie codebase.
   */
  private boolean matchesCommitPattern(String commitHash) {
    if (Strings.isNullOrEmpty(commitHash)) {
      logger.debug("Null or empty string provided when trying to match Nessie commit pattern.");
      return false; // Defensive, shouldn't be possible
    }
    if (commitHash.length() < 8 || commitHash.length() > 64) {
      logger.debug("Provided string {} does not match Nessie commit pattern (wrong length).", commitHash);
      return false;
    }
    if (!Lists.charactersOf(commitHash).stream().allMatch(c -> Character.digit(c, 16) >= 0)) {
      logger.debug("Provided string {} does not match Nessie commit pattern (not hexadecimal).", commitHash);
      return false;
    }
    logger.debug("Provided string {} matches Nessie commit pattern.", commitHash);
    return true;
  }

  @Override
  @WithSpan
  public boolean commitExists(String commitHash) {
    try {
      return requestContext.call(() -> {
        // TODO: Nessie might add a specific API for this in a later version: https://github.com/projectnessie/nessie/issues/1804
        nessieApi.getCommitLog()
          .refName(DETACHED)
          .hashOnRef(commitHash)
          .fetch(FetchOption.MINIMAL) // Might be slightly faster
          .maxRecords(1) // Might be slightly faster
          .get();
        return true;
      });
    } catch (NessieNotFoundException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listBranches() {
    try {
      return requestContext.call(() ->
        nessieApi.getAllReferences()
        .get()
        .getReferences()
        .stream()
        .filter(ref -> ref instanceof Branch)
        .map(ref -> new ReferenceInfo("Branch", ref.getName(), ref.getHash())));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listTags() {
    try {
      return requestContext.call(() ->
        nessieApi.getAllReferences()
          .get()
          .getReferences()
          .stream()
          .filter(ref -> ref instanceof Tag)
          .map(ref -> new ReferenceInfo("Tag", ref.getName(), ref.getHash())));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public Stream<ChangeInfo> listChanges(VersionContext version) {
    try {
      return requestContext.call(() -> {
          ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
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
      });
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    } catch (IllegalStateException | ReferenceTypeConflictException | ReferenceNotFoundException | NoDefaultBranchException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public Stream<ExternalNamespaceEntry> listEntries(List<String> catalogPath, VersionContext version) {
    return listEntries(catalogPath, version, false);
  }

  @Override
  @WithSpan
  public Stream<ExternalNamespaceEntry> listEntriesIncludeNested(List<String> catalogPath, VersionContext version) {
    return listEntries(catalogPath, version, true);
  }

  private Stream<ExternalNamespaceEntry> listEntries(List<String> catalogPath, VersionContext version, boolean shouldIncludeNestedTables) {
    try {
      return requestContext.call(() -> {
        ResolvedVersionContext resolvedVersion = resolveVersionContext(version);

        final GetEntriesBuilder requestBuilder = nessieApi.getEntries()
          .reference(toRef(resolvedVersion));

        int depth = (catalogPath != null && !catalogPath.isEmpty())
          ? catalogPath.size() + 1
          : 1;

        if (!shouldIncludeNestedTables) {
          requestBuilder.namespaceDepth(depth);
        }

        if (depth > 1) {
          // TODO: Escape "."s within individual path names
          requestBuilder.filter(String.format("entry.namespace.matches('%s(\\\\.|$)')", String.join("\\\\.", catalogPath)));
        }

        return requestBuilder
          .get()
          .getEntries()
          .stream()
          .map(entry -> ExternalNamespaceEntry.of(entry.getType().toString(), entry.getName().getElements()));
      });
    } catch (IllegalStateException | ReferenceTypeConflictException | ReferenceNotFoundException | NoDefaultBranchException e) {
      throw e;
    } catch (NessieNotFoundException e) {
      throw UserException.dataReadError(e).buildSilently();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void createNamespace(String namespacePath, VersionContext version) {
    try {
      requestContext.call(
          () -> {
            metrics.log("createNamespace", () -> createNamespaceHelper(namespacePath, version));
            return null;
          });
    } catch (IllegalStateException
        | ReferenceTypeConflictException
        | ReferenceNotFoundException
        | NoDefaultBranchException
        | NessieNamespaceAlreadyExistsException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createNamespaceHelper(String namespacePath, VersionContext version) {
    try {
      ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
      nessieApi
          .createNamespace()
          .reference(toRef(resolvedVersion))
          .namespace(namespacePath)
          .create();
    } catch (IllegalStateException
        | ReferenceTypeConflictException
        | ReferenceNotFoundException
        | NoDefaultBranchException e) {
      throw e;
    } catch (org.projectnessie.error.NessieNamespaceAlreadyExistsException e) {
      throw new NessieNamespaceAlreadyExistsException(e);
    } catch (NessieReferenceNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void createBranch(String branchName, VersionContext sourceVersion) {
    try {
      requestContext.call(() -> {
        ResolvedVersionContext resolvedSourceVersion = resolveVersionContext(sourceVersion);
        createReferenceHelper(
          Branch.of(
            branchName,
            resolvedSourceVersion.getCommitHash()),
          resolvedSourceVersion.getRefName());
        return null;
      });
    } catch (IllegalStateException | ReferenceAlreadyExistsException | ReferenceTypeConflictException |
      ReferenceNotFoundException | NoDefaultBranchException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void createTag(String tagName, VersionContext sourceVersion) {
    try {
      requestContext.call(() -> {
        ResolvedVersionContext resolvedSourceVersion = resolveVersionContext(sourceVersion);
        createReferenceHelper(
          Tag.of(
            tagName,
            resolvedSourceVersion.getCommitHash()),
          resolvedSourceVersion.getRefName());
        return null;
      });
    } catch (IllegalStateException | ReferenceAlreadyExistsException |
      ReferenceTypeConflictException | ReferenceNotFoundException | NoDefaultBranchException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Note: Nessie's createReference Java API is currently quite confusing.
   *
   * @param reference
   *   - reference.name -> Name of reference to be created
   *   - reference.commitHash -> Hash to create reference at (optional, but always used here)
   *   - reference type defines whether Branch or Tag is created
   * @param sourceRefName Name of source reference to create new reference from
   *                      Use "DETACHED" here for BARE_COMMIT
   */
  private void createReferenceHelper(Reference reference, String sourceRefName) {
    try {
      requestContext.call(() -> {
        nessieApi.createReference()
          .reference(reference)
          .sourceRefName(sourceRefName)
          .create();
        return null;
      });
    } catch (NessieReferenceAlreadyExistsException e) {
      throw new ReferenceAlreadyExistsException(e);
    } catch (NessieConflictException e) {
      // The only NessieConflictException expected here is NessieReferenceAlreadyExistsException caught above
      throw new IllegalStateException(e);
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void dropBranch(String branchName, String branchHash) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(branchName));
    Preconditions.checkNotNull(branchHash);

    try {
      final String[] finalBranchHash = {branchHash};
      requestContext.call(() -> {
        // Empty branchHash implies force drop, look up current hash
        if (finalBranchHash[0].isEmpty()) {
          finalBranchHash[0] = nessieApi.getReference()
            .refName(branchName)
            .get()
            .getHash();
        }

        nessieApi.deleteBranch()
          .branchName(branchName)
          .hash(finalBranchHash[0])
          .delete();
        return null;
      });
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void dropTag(String tagName, String tagHash) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tagName));
    Preconditions.checkNotNull(tagHash);

    try {
      final String[] finalTagHash = {tagHash};
      requestContext.call(() -> {
        // Empty tagHash implies force drop, look up current hash
        if (finalTagHash[0].isEmpty()) {
          finalTagHash[0] = nessieApi.getReference()
            .refName(tagName)
            .get()
            .getHash();
        }

        nessieApi.deleteTag()
          .tagName(tagName)
          .hash(finalTagHash[0])
          .delete();
        return null;
      });
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void mergeBranch(String sourceBranchName, String targetBranchName) {
    try {
      requestContext.call(() -> {
        final String targetHash = nessieApi.getReference().refName(targetBranchName).get().getHash();
        final String sourceHash = nessieApi.getReference().refName(sourceBranchName).get().getHash();

        nessieApi.mergeRefIntoBranch()
          .branchName(targetBranchName)
          .hash(targetHash)
          .fromRefName(sourceBranchName)
          .fromHash(sourceHash)
          .merge();
        return null;
      });
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void assignBranch(String branchName, VersionContext sourceContext) {
    try {
      requestContext.call(() -> {
        final String branchHash = nessieApi.getReference().refName(branchName).get().getHash();
        final Reference reference = getReference(sourceContext);
        nessieApi
          .assignBranch()
          .branchName(branchName)
          .hash(branchHash)
          .assignTo(reference)
          .assign();
        return null;
      });
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException | ReferenceNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void assignTag(String tagName, VersionContext sourceVersion) {
    try {
      requestContext.call(() -> {
        final String tagHash = nessieApi.getReference().refName(tagName).get().getHash();
        final Reference reference = getReference(sourceVersion);
        nessieApi
          .assignTag()
          .tagName(tagName)
          .hash(tagHash)
          .assignTo(reference)
          .assign();
        return null;
      });
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException | ReferenceNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public String getMetadataLocation(List<String> catalogKey, ResolvedVersionContext version) {
    try {
      return requestContext.call(() ->
        metrics.log("nessieGetContents", () -> getMetadataLocationHelper(catalogKey, version)));
    } catch (UserException | IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getMetadataLocationHelper(List<String> catalogKey, ResolvedVersionContext version) {
    final ContentKey contentKey = ContentKey.of(catalogKey);
    String metadataLocation = null;
    Content content = getContent(contentKey, version);
    if (content != null) {
      if (content instanceof IcebergTable) {
        IcebergTable icebergTable = (IcebergTable) content;
        metadataLocation = icebergTable.getMetadataLocation();
        logger.debug("Metadata location of table: {}, is {}", contentKey, metadataLocation);
      } else if (content instanceof IcebergView) {
        IcebergView icebergView = (IcebergView) content;
        metadataLocation = icebergView.getMetadataLocation();
        logger.debug("Metadata location of view: {}, is {}", contentKey, metadataLocation);
      }
    }
    return metadataLocation;
  }

  @Override
  @WithSpan
  public Optional<String> getViewDialect(List<String> catalogKey, ResolvedVersionContext version) {
    try {
      return requestContext.call(() ->
        metrics.log("nessieGetViewDialect", () -> getViewDialectHelper(catalogKey, version)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<String> getViewDialectHelper(List<String> catalogKey, ResolvedVersionContext version) {
    final ContentKey contentKey = ContentKey.of(catalogKey);
    final Content content = getContent(contentKey, version);

    if (!(content instanceof IcebergView)) {
      return Optional.empty();
    }

    final String dialect = ((IcebergView) content).getDialect();

    return (dialect == null) ? Optional.empty() : Optional.of(dialect);
  }

  private Content getContent(ContentKey contentKey, ResolvedVersionContext version) {
    Content content = null;
    try {
      content = nessieContentsCache.getUnchecked(ImmutablePair.of(contentKey, version));
      if (content != null && !(content instanceof IcebergTable) && !(content instanceof IcebergView)) {
        logger.warn(
          "Unexpected content type from Nessie for key {} : type : {} ",
          contentKey,
          content.getType());
      }
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof NullMetadataException) {
        return null;
      }
      Throwables.throwIfInstanceOf(e.getCause(), UserException.class);
      throw e;
    }
    return content;
  }

  private Optional<Content> getIcebergContentsHelper(ContentKey contentKey, ResolvedVersionContext version) {
    try {
      return requestContext.call(() -> {
        Content content = nessieApi.getContent()
          .key(contentKey)
          .reference(toRef(version))
          .get()
          .get(contentKey);
        logger.debug("Content for key '{}' at '{}': Content type :{} content {}",
          contentKey,
          version,
          content == null ? "null" : content.getType(),
          content == null ? "null" : content);

        if (content == null) {
          logger.warn("Content from Nessie for key {} return null ", contentKey);
          return Optional.empty();
        }
        if (!(content instanceof IcebergTable) && !(content instanceof IcebergView)) {
          logger.warn("Unexpected content type from Nessie for key {} : type : {} ", contentKey, content.getType());
        }
        return Optional.of(content);
      });
    } catch (NessieNotFoundException e) {
      logger.error("Failed to get metadata location for table: {}", contentKey, e);
      if (e.getErrorCode() == ErrorCode.REFERENCE_NOT_FOUND // TODO: Cleanup
        || e.getErrorCode() != ErrorCode.CONTENT_NOT_FOUND) {
        throw UserException.dataReadError(e).buildSilently();
      }
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return Optional.empty();
  }

  @Override
  @WithSpan
  public void commitTable(
      List<String> catalogKey,
      String newMetadataLocation,
      TableMetadata metadata,
      ResolvedVersionContext version) {
    try {
      requestContext.call(() -> {
        metrics.log(
          "commitTable",
          () -> commitTableHelper(catalogKey, newMetadataLocation, metadata, version));
        return null;
      });
    } catch (UserException | IllegalStateException | CommitFailedException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void commitTableHelper(
      List<String> catalogKey,
      String newMetadataLocation,
      TableMetadata metadata,
      ResolvedVersionContext version) {
    Preconditions.checkArgument(version.isBranch());

    logger.debug("Committing new metadatalocation {} snapshotId {} currentSchemaId {} defaultSpecId {} sortOrder {} for key {}",
      newMetadataLocation,
      metadata.currentSnapshot().snapshotId(),
      metadata.currentSchemaId(),
      metadata.defaultSpecId(),
      metadata.sortOrder(),
      catalogKey);

    final ContentKey contentKey = ContentKey.of(catalogKey);
    final IcebergTable newTable =
        ImmutableIcebergTable.builder()
            .metadataLocation(newMetadataLocation)
            .snapshotId(metadata.currentSnapshot().snapshotId())
            .schemaId(metadata.currentSchemaId())
            .specId(metadata.defaultSpecId())
            .sortOrderId(metadata.sortOrder().orderId())
            .build();

    commitOperationHelper(contentKey, newTable, version);
  }

  private void commitOperationHelper(
      ContentKey contentKey,
      Content content,
      ResolvedVersionContext version) {
    try {
      nessieApi
          .commitMultipleOperations()
          .branch((Branch) toRef(version))
          .operation(Operation.Put.of(contentKey, content))
          .commitMeta(CommitMeta.fromMessage("Put key: " + contentKey))
          .commit();
    } catch (NessieConflictException e) {
      throw new CommitFailedException(e, "Failed to commit operation");
    } catch (NessieNotFoundException e) {
      throw UserException.dataReadError(e).buildSilently();
    }
  }

  @Override
  @WithSpan
  public void commitView(
      List<String> catalogKey,
      String newMetadataLocation,
      IcebergView icebergView,
      ViewVersionMetadata metadata,
      String dialect,
      ResolvedVersionContext version) {
    try {
      requestContext.call(() -> {
        metrics.log(
          "commitView",
          () ->
            commitViewHelper(
              catalogKey, newMetadataLocation, icebergView, metadata, dialect, version));
        return null;
      });
    } catch (UserException | IllegalStateException | CommitFailedException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void commitViewHelper(
      List<String> catalogKey,
      String newMetadataLocation,
      IcebergView icebergView,
      ViewVersionMetadata metadata,
      String dialect,
      ResolvedVersionContext version) {
    Preconditions.checkArgument(version.isBranch());

    logger.debug(
        "Committing new metadatalocation {} versionId {} schemaId {} dialect {} sqlText {} for key {}",
        newMetadataLocation,
        metadata.currentVersionId(),
        metadata.definition().schema().schemaId(),
        dialect,
        metadata.definition().sql(),
        catalogKey);

    final ContentKey contentKey = ContentKey.of(catalogKey);
    ImmutableIcebergView.Builder viewBuilder = ImmutableIcebergView.builder();
    if (icebergView != null) {
      viewBuilder.id(icebergView.getId());
      logger.debug("The view id {} for key {}", icebergView.getId(), contentKey);
    }

    final IcebergView newView =
        viewBuilder
            .metadataLocation(newMetadataLocation)
            .versionId(metadata.currentVersionId())
            .schemaId(metadata.definition().schema().schemaId())
            .dialect(dialect)
            .sqlText(SQL_TEXT)
            .build();

    commitOperationHelper(contentKey, newView, version);
  }

  @Override
  @WithSpan
  public void deleteCatalogEntry(List<String> catalogKey, ResolvedVersionContext version) {
    Preconditions.checkArgument(version.isBranch());
    try {
      requestContext.call(() -> {
        metrics.log("deleteCatalogEntry", () -> deleteCatalogEntryHelper(catalogKey, version));
        return null;
      });
    } catch (UserException | IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void deleteCatalogEntryHelper(List<String> catalogKey, ResolvedVersionContext version) {
    final Reference versionRef = toRef(version);
    final ContentKey contentKey = ContentKey.of(catalogKey);
    logger.debug("Deleting entry in Nessie for key {} ", contentKey);
    // Check if reference exists to give back a proper error
    // TODO(DX-44309): Get the expected commit from the getContents and provide that to the commitMultipleOperations
    // So the deleteKey is atomic.
    String metadataLocation = getMetadataLocation(catalogKey, version);
    if(metadataLocation == null){
      logger.debug("Tried to delete key : {} but it was not found in nessie ", catalogKey);
      throw UserException.validationError()
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
      // TODO: DX-46713 Cleanup and add doc for the nessie client
      logger.debug("Tried to delete key : {} but it was not found in nessie ", catalogKey);
      throw UserException.validationError(e)
        .message(String.format("Version reference not found in nessie for  %s", catalogKey))
        .buildSilently();
    } catch (NessieConflictException e) {
      logger.debug("The catalog entry {} could not be removed from Nessie", catalogKey);
      throw UserException.concurrentModificationError(e)
        .message(String.format("Failed to drop catalog entry %s", catalogKey))
        .buildSilently();
    }
  }

  @Override
  public VersionedPlugin.EntityType getVersionedEntityType(List<String> tableKey, ResolvedVersionContext version) {
    try {
      return requestContext.call(() ->
        metrics.log("IcebergGetContents", () -> getVersionedEntityTypeHelper(tableKey, version)));
    } catch (IllegalStateException | UserException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private VersionedPlugin.EntityType getVersionedEntityTypeHelper(List<String> catalogKey, ResolvedVersionContext version) {
    final ContentKey contentKey = ContentKey.of(catalogKey);
    Content content = getContent(contentKey, version);
    if (content != null) {
      switch (content.getType()) {
        case ICEBERG_TABLE:
          return VersionedPlugin.EntityType.ICEBERG_TABLE;
        case ICEBERG_VIEW:
          return VersionedPlugin.EntityType.ICEBERG_VIEW;
        default:
          throw new IllegalStateException("Unsupported entity: " + content.getType());
      }
    }
    return VersionedPlugin.EntityType.UNKNOWN;
  }

  private Reference getReference(VersionContext versionContext) {
    Preconditions.checkNotNull(versionContext);

    Reference reference;
    try {
      reference = nessieApi.getReference()
        .refName(versionContext.getValue())
        .get();
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
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
        return Branch.of(DETACHED, resolvedVersionContext.getCommitHash());
      default:
        throw new IllegalStateException("Unexpected value: " + resolvedVersionContext.getType());
    }
  }

  @VisibleForTesting
  void setRequestContext(RequestContext requestContext) {
    this.requestContext = requestContext;
  }

  private class NessieContentsCacheLoader extends CacheLoader<ImmutablePair<ContentKey, ResolvedVersionContext>, Content> {
    @Override
    public Content load(ImmutablePair<ContentKey, ResolvedVersionContext> pair) {
      ContentKey contentkey = pair.left;
      ResolvedVersionContext version = pair.right;

      Optional<Content> icebergContent = getIcebergContentsHelper(contentkey, version);
      if (icebergContent == null || !icebergContent.isPresent()) {
        throw new NullMetadataException();
      }
      return icebergContent.get();
    }
  }

  public static final class NullMetadataException extends RuntimeException {
  }
}
