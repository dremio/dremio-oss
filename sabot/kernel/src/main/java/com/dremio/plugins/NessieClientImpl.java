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

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Validation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.context.RequestContext;
import com.dremio.context.UsernameContext;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.ConnectionRefusedException;
import com.dremio.exec.store.HttpClientRequestException;
import com.dremio.exec.store.NessieNamespaceAlreadyExistsException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.telemetry.api.metrics.MetricsInstrumenter;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Implementation of the NessieClient interface for REST.
  */
public class NessieClientImpl implements NessieClient {

  private static final Logger logger = LoggerFactory.getLogger(NessieClientImpl.class);
  private static final String SQL_TEXT = "N/A";
  static final String BRANCH_REFERENCE = "Branch";
  static final String TAG_REFERENCE = "Tag";

  private final NessieApiV1 nessieApi;
  private final boolean produceImplicitNamespaces;

  private static final MetricsInstrumenter metrics = new MetricsInstrumenter(NessieClient.class);

  private final LoadingCache<ImmutablePair<ContentKey, ResolvedVersionContext>, Content> nessieContentCache;

  public NessieClientImpl(NessieApiV1 nessieApi) {
    this(nessieApi, true);
  }

  public NessieClientImpl(NessieApiV1 nessieApi, boolean produceImplicitNamespaces) {
    this.nessieApi = nessieApi;
    this.produceImplicitNamespaces = produceImplicitNamespaces;
    this.nessieContentCache = Caffeine
      .newBuilder()
      .maximumSize(1000) // items
      .softValues()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new NessieContentCacheLoader());
  }

  private class NessieContentCacheLoader implements CacheLoader<ImmutablePair<ContentKey, ResolvedVersionContext>, Content> {
    @Override
    public Content load(ImmutablePair<ContentKey, ResolvedVersionContext> pair) throws Exception {
      return metrics.log("loadNessieContent",
        () -> loadNessieContent(pair.left, pair.right)).orElse(null);
    }
  }

  @Nullable
  private Content getContent(ContentKey contentKey, ResolvedVersionContext version) {
    return metrics.log("getNessieContent",
      () -> nessieContentCache.get(ImmutablePair.of(contentKey, version)));
  }

  @Override
  @WithSpan
  public ResolvedVersionContext getDefaultBranch() {
    try {
      Branch defaultBranch = nessieApi.getDefaultBranch();
      return ResolvedVersionContext.ofBranch(defaultBranch.getName(), defaultBranch.getHash());
    } catch (NessieNotFoundException e) {
      throw new NoDefaultBranchException(e);
    } catch (NessieNotAuthorizedException e) {
      throw new UnAuthenticatedException(e, "Unable to authenticate to the Nessie server. Make sure that the token is valid and not expired");
    } catch (HttpClientException e) {
      logger.error("Unable to get the default branch from the Nessie", e);
      if (e.getCause() instanceof ConnectException) {
        throw new ConnectionRefusedException(e, "Connection refused while connecting to the Nessie Server.");
      }

      throw new HttpClientRequestException(e, "Failed to get the default branch from Nessie");
    }
  }

  @Override
  @WithSpan
  public ResolvedVersionContext resolveVersionContext(VersionContext versionContext) {
    return metrics.log("resolveVersionContext", () -> resolveVersionContextHelper(versionContext));
  }

  @Override
  @WithSpan
  public ResolvedVersionContext resolveVersionContext(VersionContext versionContext, String jobId) {
    return metrics.log("resolveVersionContext", () -> resolveVersionContextHelper(versionContext));
  }

  private ResolvedVersionContext resolveVersionContextHelper(VersionContext versionContext) {
    Preconditions.checkNotNull(versionContext);
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

  private static boolean matchesCommitPattern(String commitHash) {
    if (Strings.isNullOrEmpty(commitHash)) {
      logger.debug("Null or empty string provided when trying to match Nessie commit pattern.");
      return false; // Defensive, shouldn't be possible
    }
    if (!Validation.isValidHash(commitHash)) {
      logger.debug("Provided string {} does not match Nessie commit pattern.", commitHash);
      return false;
    }
    logger.debug("Provided string {} matches Nessie commit pattern.", commitHash);
    return true;
  }

  @Override
  @WithSpan
  public boolean commitExists(String commitHash) {
    try {
      nessieApi.getCommitLog()
        .reference(Detached.of(commitHash))
        .fetch(FetchOption.MINIMAL) // Might be slightly faster
        .maxRecords(1) // Might be slightly faster
        .get();

      return true;
    } catch (NessieNotFoundException e) {
      return false;
    }
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listBranches() {
    return listReferences(ReferenceType.BRANCH);
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listTags() {
    return listReferences(ReferenceType.TAG);
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listReferences() {
    return listReferences(null);
  }

  private Stream<ReferenceInfo> listReferences(@Nullable ReferenceType typeFilter) {
    GetAllReferencesBuilder builder = nessieApi.getAllReferences();
    if (typeFilter != null) {
      // i.e. refType == 'BRANCH'
      builder.filter(String.format("refType == '%s'", typeFilter.name()));
    }
    return builder.get().getReferences()
      .stream()
      .map(ref -> toReferenceInfo(ref, typeFilter));
  }

  private static ReferenceInfo toReferenceInfo(Reference ref, @Nullable ReferenceType typeFilter) {
    if (typeFilter != null && ref.getType() != typeFilter) {
      throw new IllegalStateException("Nessie responded with wrong reference type: " +
        ref + " expected: " + typeFilter);
    }
    String type = ref.getType() == ReferenceType.BRANCH ? BRANCH_REFERENCE : TAG_REFERENCE;
    return new ReferenceInfo(type, ref.getName(), ref.getHash());
  }

  @Override
  @WithSpan
  public Stream<ChangeInfo> listChanges(VersionContext version) {
    try {
      ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
      return nessieApi.getCommitLog()
        .reference(toRef(resolvedVersion))
        .get()
        .getLogEntries()
        .stream()
        .map(NessieClientImpl::toChangeInfo);
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    }
  }

  private static ChangeInfo toChangeInfo(LogEntry log) {
    CommitMeta commitMeta = log.getCommitMeta();
    String authorTime = commitMeta.getAuthorTime() != null
      ? commitMeta.getAuthorTime().toString()
      : "";
    return new ChangeInfo(
      commitMeta.getHash(),
      commitMeta.getAuthor(),
      authorTime,
      commitMeta.getMessage()
    );
  }

  @Override
  @WithSpan
  public Stream<ExternalNamespaceEntry> listEntries(
    @Nullable List<String> catalogPath,
    ResolvedVersionContext version,
    NestingMode nestingMode,
    @Nullable Set<ExternalNamespaceEntry.Type> contentTypeFilter,
    @Nullable String celFilter) {
    return metrics.log("listEntries",
      () -> listEntriesHelper(catalogPath, version, nestingMode, contentTypeFilter, celFilter));
  }

  private Stream<ExternalNamespaceEntry> listEntriesHelper(
    @Nullable List<String> catalogPath,
    ResolvedVersionContext resolvedVersion,
    NestingMode nestingMode,
    @Nullable Set<ExternalNamespaceEntry.Type> contentTypeFilter,
    @Nullable String celFilter) {
    try {
      final GetEntriesBuilder requestBuilder = nessieApi.getEntries()
        .reference(toRef(resolvedVersion));

      List<String> filterTerms = new ArrayList<>();
      int depth = 1;
      if (catalogPath != null) {
        depth += catalogPath.size();
        if (depth > 1) {
          filterTerms.add(String.format("entry.encodedKey.startsWith('%s.')", Namespace.of(catalogPath).name()));
        }
      }
      if (nestingMode == NestingMode.SAME_DEPTH_ONLY) {
        if (produceImplicitNamespaces) {
          // namespaceDepth causes implicit namespaces to be returned
          // namespaceDepth is not supported in Nessie REST API V2
          requestBuilder.namespaceDepth(depth);
        } else {
          filterTerms.add(String.format("size(entry.keyElements) == %d", depth));
        }
      }
      if (contentTypeFilter != null && !contentTypeFilter.isEmpty()) {
        // build filter string i.e. entry.contentType in ['ICEBERG_TABLE', 'DELTA_LAKE_TABLE']
        String setElements = contentTypeFilter.stream()
          .map(ExternalNamespaceEntry.Type::toNessieContentType)
          .map(Content.Type::name)
          .map(typeName -> String.format("'%s'", typeName))
          .collect(Collectors.joining(", "));
        filterTerms.add(String.format("entry.contentType in [%s]", setElements));
      }
      if (celFilter != null) {
        filterTerms.add(celFilter);
      }
      if (!filterTerms.isEmpty()) {
        String combinedFilter = filterTerms.stream()
          .map(term -> String.format("(%s)", term))
          .collect(Collectors.joining(" && "));
        requestBuilder.filter(combinedFilter);
      }

      final List<ExternalNamespaceEntry> externalNamespaceEntries = new ArrayList<>();
      // Don't switch to stream().map! Due to the thread it may use, proper RequestContext may not be present!
      requestBuilder.stream().forEach(entry -> externalNamespaceEntries.add(toExternalNamespaceEntry(entry, resolvedVersion)));
      return externalNamespaceEntries.stream();
    } catch (NessieNotFoundException e) {
      throw UserException.dataReadError(e).buildSilently();
    }
  }

  private ExternalNamespaceEntry toExternalNamespaceEntry(Entry entry,
    ResolvedVersionContext resolvedVersion) {
    List<String> catalogKey = entry.getName().getElements();
    String contentId = entry.getContentId();
    if (contentId == null && !Content.Type.NAMESPACE.equals(entry.getType())) {
      // use content from response if available, otherwise try loading
      // note: content is not available unless explicity requested
      // note: implicit namespaces have no contentId, so there is no need to try loading
      Content content = entry.getContent();
      if (content == null) {
        content = getContent(ContentKey.of(catalogKey), resolvedVersion);
        if (logger.isWarnEnabled()) {
          String contentInfo = "null";
          if (content != null) {
            contentInfo = content.getType() + " - " + content.getId();
          }
          logger.warn("Slow nessie listEntries content load (catalogKey: {}, version: {}): {}",
            catalogKey, resolvedVersion, contentInfo);
        }
      }
      if (content != null) {
        contentId = content.getId();
      }
    }
    String type = entry.getType().toString();
    if (contentId == null) {
      return ExternalNamespaceEntry.of(type, catalogKey);
    }
    TableVersionContext tableVersionContext = TableVersionContext.of(resolvedVersion);
    return ExternalNamespaceEntry.of(type, catalogKey, contentId, tableVersionContext);
  }

  @Override
  @WithSpan
  public void createNamespace(List<String> namespacePathList, VersionContext version) {
    metrics.log("createNamespace", () -> createNamespaceHelper(namespacePathList, version));
  }

  private void createNamespaceHelper(List<String> namespacePathList, VersionContext version) {
    try {
      ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
      if (!resolvedVersion.isBranch()) {
        throw UserException.validationError()
          .message("Cannot create folders for non-branch references.")
          .buildSilently();
      }
      final UsernameContext usernameContext = RequestContext.current().get(UsernameContext.CTX_KEY);
      final String authorName = usernameContext != null ? usernameContext.getUserName() : null;

      // we are checking if the namespace already exists in nessie.
      // if we already have the content, we are creating duplicate namespace so we are throwing an error.
      ContentKey contentKey = ContentKey.of(namespacePathList);
      Content content = getContent(contentKey, resolvedVersion);
      if (content != null) {
        throw new NessieNamespaceAlreadyExistsException(String.format("Folder %s already exists", contentKey.toPathString()));
      }
      nessieApi
        .commitMultipleOperations()
        .branch((Branch) toRef(resolvedVersion))
        .operation(Operation.Put.of(contentKey, Namespace.of(namespacePathList)))
        .commitMeta(CommitMeta.builder()
          .author(authorName)
          .message("Create namespace key: " + contentKey)
          .build())
        .commit();
    } catch (org.projectnessie.error.NessieNamespaceAlreadyExistsException e) {
      logger.error("Failed to create namespace as Namespace already exists", e);
      throw new NessieNamespaceAlreadyExistsException(e);
    } catch (NessieReferenceNotFoundException e) {
      logger.error("Failed to create namespace due to Reference not found", e);
      throw new ReferenceNotFoundException(e);
    } catch (NessieConflictException e) {
      if (e instanceof NessieReferenceConflictException) {
        throw UserException.validationError().message(e.getMessage()).buildSilently();
      }
      logger.error("Failed to create namespace due to Nessie conflict", e);
      throw new RuntimeException(e);
    } catch (NessieNotFoundException e) {
      logger.error("Failed to create namespace due to Nessie not found", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void deleteNamespace(List<String> namespacePathList, VersionContext version) {
    metrics.log("deleteNamespace", () -> deleteNamespaceHelper(namespacePathList, version));
  }

  private void deleteNamespaceHelper(List<String> namespacePathList, VersionContext version) {
    ContentKey contentKey = ContentKey.of(namespacePathList);
    try {
      ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
      if (!resolvedVersion.isBranch()) {
        throw UserException.validationError()
          .message("Cannot delete folders for non-branch references.")
          .buildSilently();
      }
      final UsernameContext usernameContext = RequestContext.current().get(UsernameContext.CTX_KEY);
      final String authorName = usernameContext != null ? usernameContext.getUserName() : null;
      boolean isNamespaceNotEmpty = listEntries(namespacePathList, resolvedVersion,
        NestingMode.SAME_DEPTH_ONLY, null, null)
        .findAny().isPresent();

      if (isNamespaceNotEmpty) {
        throw UserException.validationError().message("Folder '%s' is not empty", contentKey.toPathString())
          .buildSilently();
      }

      nessieApi
        .commitMultipleOperations()
        .branch((Branch) toRef(resolvedVersion))
        .operation(Operation.Delete.of(contentKey))
        .commitMeta(CommitMeta.builder()
          .author(authorName)
          .message("Delete namespace key: " + contentKey)
          .build())
        .commit();
    } catch (NessieNamespaceNotFoundException e) {
      logger.warn("NessieNamespaceNotFound from path {}.", namespacePathList);
      return;
    } catch (NessieReferenceNotFoundException e) {
      logger.error("Failed to delete namespace as Reference not found", e);
      throw new ReferenceNotFoundException(e);
    } catch (NessieNamespaceNotEmptyException e) {
      logger.error("Failed to delete namespace as Namespace not empty", e);
      throw UserException.validationError().message("Folder '%s' is not empty", contentKey.toPathString())
        .buildSilently();
    } catch (NessieConflictException e) {
      logger.error("Failed to create namespace due to Nessie conflict", e);
      throw new RuntimeException(e);
    } catch (NessieNotFoundException e) {
      logger.error("Failed to create namespace due to Nessie not found", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  @WithSpan
  public void createBranch(String branchName, VersionContext sourceVersion) {
    ResolvedVersionContext resolvedSourceVersion = resolveVersionContext(sourceVersion);
    Branch branch = getBranch(branchName, resolvedSourceVersion);
    createReferenceHelper(branch, resolvedSourceVersion.getRefName());
  }

  protected Branch getBranch(String branchName, ResolvedVersionContext resolvedSourceVersion) {
    try {
      return Branch.of(branchName, resolvedSourceVersion.getCommitHash());
    } catch (IllegalArgumentException e) {
      throw UserException.validationError().message("Invalid branch name: %s. %s", branchName, e.getMessage())
        .buildSilently();
    }
  }

  @Override
  @WithSpan
  public void createTag(String tagName, VersionContext sourceVersion) {
    ResolvedVersionContext resolvedSourceVersion = resolveVersionContext(sourceVersion);
    Tag tag = getTag(tagName, resolvedSourceVersion);
    createReferenceHelper(tag, resolvedSourceVersion.getRefName());
  }

  protected Tag getTag(String tagName, ResolvedVersionContext resolvedSourceVersion) {
    try {
      return Tag.of(tagName, resolvedSourceVersion.getCommitHash());
    } catch (IllegalArgumentException e) {
      throw UserException.validationError().message("Invalid tag name: %s. %s", tagName, e.getMessage())
        .buildSilently();
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
      nessieApi.createReference()
        .reference(reference)
        .sourceRefName(sourceRefName)
        .create();
    } catch (NessieReferenceAlreadyExistsException e) {
      throw new ReferenceAlreadyExistsException(e);
    } catch (NessieConflictException e) {
      // The only NessieConflictException expected here is NessieReferenceAlreadyExistsException caught above
      throw new IllegalStateException(e);
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    }
  }

  @Override
  @WithSpan
  public void dropBranch(String branchName, String branchHash) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(branchName));
    Preconditions.checkNotNull(branchHash);

    try {
      // Empty branchHash implies force drop, look up current hash
      if (branchHash.isEmpty()) {
        branchHash = nessieApi.getReference()
          .refName(branchName)
          .get()
          .getHash();
      }

      nessieApi.deleteBranch()
        .branchName(branchName)
        .hash(branchHash)
        .delete();
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    } catch (NessieBadRequestException e) {
      throw UserException.validationError().message("Cannot drop the branch '%s'. %s", branchName, e.getMessage())
        .buildSilently();
    }
  }

  @Override
  @WithSpan
  public void dropTag(String tagName, String tagHash) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tagName));
    Preconditions.checkNotNull(tagHash);

    try {
      // Empty tagHash implies force drop, look up current hash
      if (tagHash.isEmpty()) {
        tagHash = nessieApi.getReference()
          .refName(tagName)
          .get()
          .getHash();
      }

      nessieApi.deleteTag()
        .tagName(tagName)
        .hash(tagHash)
        .delete();
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    }
  }

  @Override
  @WithSpan
  public void mergeBranch(String sourceBranchName, String targetBranchName) {
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
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    }
  }

  @Override
  @WithSpan
  public void assignBranch(String branchName, VersionContext sourceContext) {
    try {
      final String branchHash = nessieApi.getReference().refName(branchName).get().getHash();
      ResolvedVersionContext resolvedVersion = resolveVersionContext(sourceContext);
      nessieApi
        .assignBranch()
        .branchName(branchName)
        .hash(branchHash)
        .assignTo(toRef(resolvedVersion))
        .assign();
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException | ReferenceNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    }
  }

  @Override
  @WithSpan
  public void assignTag(String tagName, VersionContext sourceVersion) {
    try {
      final String tagHash = nessieApi.getReference().refName(tagName).get().getHash();
      ResolvedVersionContext resolvedVersion = resolveVersionContext(sourceVersion);
      nessieApi
        .assignTag()
        .tagName(tagName)
        .hash(tagHash)
        .assignTo(toRef(resolvedVersion))
        .assign();
    } catch (NessieConflictException e) {
      throw new ReferenceConflictException(e);
    } catch (NessieNotFoundException | ReferenceNotFoundException e) {
      throw new ReferenceNotFoundException(e);
    }
  }

  @Override
  @WithSpan
  public String getMetadataLocation(List<String> catalogKey, ResolvedVersionContext version, String jobId) {
    return metrics.log("nessieGetMetadataLocation",
      () -> getMetadataLocationHelper(catalogKey, version));
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
    return metrics.log("nessieGetViewDialect", () -> getViewDialectHelper(catalogKey, version));
  }

  private Optional<String> getViewDialectHelper(List<String> catalogKey, ResolvedVersionContext version) {
    final ContentKey contentKey = ContentKey.of(catalogKey);
    final Content content = getContent(contentKey, version);

    if (!(content instanceof IcebergView)) {
      return Optional.empty();
    }
    final String dialect = ((IcebergView) content).getDialect();
    return Optional.ofNullable(dialect);
  }

  @WithSpan
  private Optional<Content> loadNessieContent(ContentKey contentKey, ResolvedVersionContext version) {
    String logPrefix = String.format("Load of Nessie content (key: %s, version: %s)",
      contentKey, version);
    Content content;
    try {
      content = nessieApi.getContent()
        .key(contentKey)
        .reference(toRef(version))
        .get()
        .get(contentKey);
    } catch (NessieNotFoundException e) {
      if (e.getErrorCode() == ErrorCode.CONTENT_NOT_FOUND) {
        logger.warn("{} returned CONTENT_NOT_FOUND", logPrefix);
        return Optional.empty();
      }
      logger.error("{} failed", logPrefix, e);
      throw UserException.dataReadError(e).buildSilently();
    }
    if (content == null) {
      logger.warn("{} returned null", logPrefix);
      return Optional.empty();
    }
    logger.debug("{} returned content type: {}, content: {}",
      logPrefix, content.getType(), content);
    if (!(content instanceof IcebergTable
      || content instanceof IcebergView
      || content instanceof Namespace)) {
      logger.warn("{} returned unexpected content type: {} ", logPrefix, content.getType());
    }
    return Optional.of(content);
  }

  private void ensureOperationOnBranch(ResolvedVersionContext version) {
    if (!version.isBranch()) {
      throw new IllegalArgumentException(
        "Requested operation is not supported for non-branch reference: " + version);
    }
  }

  @Override
  @WithSpan
  public void commitTable(
    List<String> catalogKey,
    String newMetadataLocation,
    NessieClientTableMetadata nessieClientTableMetadata,
    ResolvedVersionContext version,
    String baseContentId,
    String jobId,
    String userName) {
    metrics.log(
      "commitTable",
      () -> commitTableHelper(catalogKey, newMetadataLocation, nessieClientTableMetadata, version, baseContentId, userName));
  }

  private void commitTableHelper(
    List<String> catalogKey,
    String newMetadataLocation,
    NessieClientTableMetadata nessieClientTableMetadata,
    ResolvedVersionContext version,
    String baseContentId,
    String userName) {
    ensureOperationOnBranch(version);
    ContentKey contentKey = ContentKey.of(catalogKey);

    logger.debug("Committing new metadatalocation {} snapshotId {} currentSchemaId {} defaultSpecId {} sortOrder {} for key {} and id {}",
      newMetadataLocation,
      nessieClientTableMetadata.getSnapshotId(),
      nessieClientTableMetadata.getCurrentSchemaId(),
      nessieClientTableMetadata.getDefaultSpecId(),
      nessieClientTableMetadata.getSortOrderId(),
      catalogKey,
      ((baseContentId == null) ? "new object (null id) " : baseContentId));

    ImmutableIcebergTable.Builder newTableBuilder = ImmutableIcebergTable.builder()
      .metadataLocation(newMetadataLocation)
      .snapshotId(nessieClientTableMetadata.getSnapshotId())
      .schemaId(nessieClientTableMetadata.getCurrentSchemaId())
      .specId(nessieClientTableMetadata.getDefaultSpecId())
      .sortOrderId(nessieClientTableMetadata.getSortOrderId());
    if (baseContentId != null) {
      newTableBuilder.id(baseContentId);
    }
    commitOperationHelper(contentKey, newTableBuilder.build(), version, userName);
  }

  private void commitOperationHelper(
      ContentKey contentKey,
      Content content,
      ResolvedVersionContext version,
      String userName) {

    //TODO (DX-59840): Remove the UsernameContext and get the info from userName in DCS after testing there
    final UsernameContext usernameContext = RequestContext.current().get(UsernameContext.CTX_KEY);
    final String authorName = usernameContext != null ? usernameContext.getUserName() : userName;

    try {
      nessieApi
          .commitMultipleOperations()
          .branch((Branch) toRef(version))
          .operation(Operation.Put.of(contentKey, content))
          .commitMeta(CommitMeta.builder()
            .author(authorName)
            .message("Put key: " + contentKey)
            .build())
          .commit();
    } catch (NessieConflictException e) {
      if (e instanceof NessieReferenceConflictException) {
        throw UserException.validationError().message(e.getMessage()).buildSilently();
      }
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
    ResolvedVersionContext version,
    String baseContentId,
    String userName) {
    metrics.log(
      "commitView",
      () ->
        commitViewHelper(
          catalogKey, newMetadataLocation, icebergView, metadata, dialect, version, baseContentId, userName));
  }

  private void commitViewHelper(
    List<String> catalogKey,
    String newMetadataLocation,
    IcebergView icebergView,
    ViewVersionMetadata metadata,
    String dialect,
    ResolvedVersionContext version,
    String baseContentId,
    String userName) {
    ensureOperationOnBranch(version);
    ContentKey contentKey = ContentKey.of(catalogKey);
    logger.debug(
        "Committing new metadatalocation {} versionId {} schemaId {} dialect {} sqlText {} for key {} id {}",
        newMetadataLocation,
        metadata.currentVersionId(),
        metadata.definition().schema().schemaId(),
        dialect,
        metadata.definition().sql(),
        catalogKey,
        ((baseContentId == null) ? "new object (null id) " : baseContentId));

    ImmutableIcebergView.Builder viewBuilder = ImmutableIcebergView.builder();
    if (icebergView != null && icebergView.getId() != null) {
      viewBuilder.id(icebergView.getId());
      logger.debug("The view id {} for key {}", icebergView.getId(), contentKey);
    }

    ImmutableIcebergView.Builder newViewBuilder = viewBuilder
      .metadataLocation(newMetadataLocation)
      .versionId(metadata.currentVersionId())
      .schemaId(metadata.definition().schema().schemaId())
      .dialect(dialect)
      .sqlText(SQL_TEXT);
    if (baseContentId != null) {
      newViewBuilder.id(baseContentId);
    }
    commitOperationHelper(contentKey, newViewBuilder.build(), version, userName);
  }

  @Override
  @WithSpan
  public void deleteCatalogEntry(List<String> catalogKey, ResolvedVersionContext version, String userName) {
    metrics.log("deleteCatalogEntry", () -> deleteCatalogEntryHelper(catalogKey, version, userName));
  }

  private void deleteCatalogEntryHelper(List<String> catalogKey, ResolvedVersionContext version, String userName) {
    ensureOperationOnBranch(version);
    final Reference versionRef = toRef(version);
    final ContentKey contentKey = ContentKey.of(catalogKey);
    logger.debug("Deleting entry in Nessie for key {} ", contentKey);
    // Check if reference exists to give back a proper error
    String metadataLocation = getMetadataLocation(catalogKey, version, null);
    if(metadataLocation == null){
      logger.debug("Tried to delete key : {} but it was not found in nessie ", catalogKey);
      throw UserException.validationError()
        .message(String.format("Key not found in nessie for  %s", catalogKey))
        .buildSilently();
    }

    //TODO (DX-59840): Remove the UsernameContext and get the info from userName in DCS after testing there
    final UsernameContext usernameContext = RequestContext.current().get(UsernameContext.CTX_KEY);
    final String authorName = usernameContext != null ? usernameContext.getUserName() : userName;

    try {
      nessieApi
        .commitMultipleOperations()
        .branchName(versionRef.getName())
        .hash(versionRef.getHash())
        .operation(Operation.Delete.of(contentKey))
        .commitMeta(CommitMeta.builder()
          .author(authorName)
          .message("Deleting key: " + contentKey)
          .build())
        .commit();
    } catch (NessieNotFoundException e) {
      logger.debug("Tried to delete key : {} but it was not found in nessie ", catalogKey);
      throw UserException.validationError(e)
        .message(String.format("Version reference not found in nessie for %s", catalogKey))
        .buildSilently();
    } catch (NessieConflictException e) {
      logger.debug("The catalog entry {} could not be removed from Nessie", catalogKey);
      throw UserException.concurrentModificationError(e)
        .message(String.format("Failed to drop catalog entry %s", catalogKey))
        .buildSilently();
    }
  }

  @Override
  @WithSpan
  public VersionedPlugin.EntityType getVersionedEntityType(List<String> tableKey, ResolvedVersionContext version) {
    return metrics.log("nessieGetVersionedEntityType",
      () -> getVersionedEntityTypeHelper(tableKey, version));
  }

  private VersionedPlugin.EntityType getVersionedEntityTypeHelper(List<String> catalogKey, ResolvedVersionContext version) {
    final ContentKey contentKey = ContentKey.of(catalogKey);
    Content content = getContent(contentKey, version);
    if (content != null) {
      if (Content.Type.ICEBERG_TABLE.equals(content.getType())) {
        return VersionedPlugin.EntityType.ICEBERG_TABLE;
      } else if (Content.Type.ICEBERG_VIEW.equals(content.getType())) {
        return VersionedPlugin.EntityType.ICEBERG_VIEW;
      } else if (Content.Type.NAMESPACE.equals(content.getType())) {
        return VersionedPlugin.EntityType.FOLDER;
      } else {
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
      String error = versionContext.toStringFirstLetterCapitalized() + " is not found";
      logger.error(error, e);
      throw new ReferenceNotFoundException(error);
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
        return Detached.of(resolvedVersionContext.getCommitHash());
      default:
        throw new IllegalStateException("Unexpected value: " + resolvedVersionContext.getType());
    }
  }

  @Override
  @WithSpan
  public String getContentId(List<String> tableKey, ResolvedVersionContext version, String jobId) {
    final ContentKey contentKey = ContentKey.of(tableKey);
    Content content = getContent(contentKey, version);
    if (content == null) {
      // content will be null for implicitly created folders/namespaces.
      return null;
    }
    return content.getId();
  }

  @Override
  public NessieApi getNessieApi() {
    return nessieApi;
  }

  @Override
  public void close() {
    nessieApi.close();
  }
}
