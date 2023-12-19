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

import static com.dremio.plugins.NessieClientOptions.NESSIE_CONTENT_CACHE_SIZE_ITEMS;
import static com.dremio.plugins.NessieClientOptions.NESSIE_CONTENT_CACHE_TTL_MINUTES;

import java.net.ConnectException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.viewdepoc.ViewVersionMetadata;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.VM;
import com.dremio.common.exceptions.UserException;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.context.UsernameContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.catalog.VersionedPlugin.EntityType;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.ConnectionRefusedException;
import com.dremio.exec.store.HttpClientRequestException;
import com.dremio.exec.store.NessieNamespaceAlreadyExistsException;
import com.dremio.exec.store.NessieNamespaceNotEmptyException;
import com.dremio.exec.store.NessieNamespaceNotFoundException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundByTimestampException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.options.OptionManager;
import com.dremio.plugins.ExternalNamespaceEntry.Type;
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
  private static final MetricsInstrumenter metrics = new MetricsInstrumenter(NessieClient.class);
  private static final String SQL_TEXT = "N/A";

  static final String BRANCH_REFERENCE = "Branch";
  static final String TAG_REFERENCE = "Tag";

  private final NessieApiV2 nessieApi;
  private final boolean produceImplicitNamespaces;
  private final LoadingCache<ImmutableTriple<ContentKey, ResolvedVersionContext, String>, Content> nessieContentCache;
  private boolean apiClosed = false;
  private String apiCloseStacktrace;

  public NessieClientImpl(NessieApiV2 nessieApi, OptionManager optionManager) {
    this(nessieApi, optionManager, true);
  }

  public NessieClientImpl(NessieApiV2 nessieApi, OptionManager optionManager, boolean produceImplicitNamespaces) {
    this.nessieApi = nessieApi;
    this.produceImplicitNamespaces = produceImplicitNamespaces;
    this.nessieContentCache = Caffeine
      .newBuilder()
      .maximumSize(optionManager.getOption(NESSIE_CONTENT_CACHE_SIZE_ITEMS))
      .softValues()
      .expireAfterWrite(optionManager.getOption(NESSIE_CONTENT_CACHE_TTL_MINUTES), TimeUnit.MINUTES)
      .build(new NessieContentCacheLoader());
  }

  private class NessieContentCacheLoader implements CacheLoader<ImmutableTriple<ContentKey, ResolvedVersionContext, String>, Content> {
    @Override
    public Content load(ImmutableTriple<ContentKey, ResolvedVersionContext, String> triple) throws Exception {
      ContentKey contentKey = triple.left;
      ResolvedVersionContext resolvedVersionContext = triple.middle;
      String userId = triple.right; // Unused because RequestContext is already set. However, required for the cache key so that entries are unique per user.
      Preconditions.checkArgument(userId.equals(RequestContext.current().get(UserContext.CTX_KEY).getUserId())); // Defensive check only
      return loadNessieContent(contentKey, resolvedVersionContext).orElse(null);
    }
  }

  @Nullable
  private Content getContent(ContentKey contentKey, ResolvedVersionContext version) {
    return metrics.log("getNessieContent", () -> getContentHelper(contentKey, version));
  }

  @Nullable
  private Content getContentHelper(ContentKey contentKey, ResolvedVersionContext version) {
    UserContext userContext = RequestContext.current().get(UserContext.CTX_KEY);

    // If UserContext is not set, unsafe to share with other instances that also don't have UserContext set. Bypass the
    // cache instead.
    if (userContext == null) {
      return loadNessieContent(contentKey, version).orElse(null);
    }

    String userId = userContext.getUserId();
    return nessieContentCache.get(ImmutableTriple.of(contentKey, version, userId));
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
      case NOT_SPECIFIED:
        return getDefaultBranch();
      case REF:
        if (matchesCommitPattern(versionContext.getValue())) {
          return ResolvedVersionContext.ofCommit(versionContext.getValue());
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
      case COMMIT:
        return ResolvedVersionContext.ofCommit(versionContext.getValue());
      case REF_AS_OF_TIMESTAMP:
        if (matchesCommitPattern(versionContext.getValue())) {
          throw UserException.validationError().message("AS OF <timestamp> syntax is not supported for COMMITs.").buildSilently();
        }
        return ResolvedVersionContext.ofCommit(getHashAtTimestamp(versionContext));
      case BRANCH_AS_OF_TIMESTAMP: // Intentional fallthrough
      case TAG_AS_OF_TIMESTAMP:
        return ResolvedVersionContext.ofCommit(getHashAtTimestamp(versionContext));
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
    ContentMode contentMode,
    @Nullable Set<ExternalNamespaceEntry.Type> contentTypeFilter,
    @Nullable String celFilter) {
    return metrics.log("listEntries",
      () -> listEntriesHelper(
        catalogPath,
        version,
        nestingMode,
        contentMode,
        contentTypeFilter,
        celFilter
      ));
  }

  private Stream<ExternalNamespaceEntry> listEntriesHelper(
    @Nullable List<String> catalogPath,
    ResolvedVersionContext resolvedVersion,
    NestingMode nestingMode,
    ContentMode contentMode,
    @Nullable Set<ExternalNamespaceEntry.Type> contentTypeFilter,
    @Nullable String celFilter) {
    try {
      final GetEntriesBuilder requestBuilder = nessieApi.getEntries()
        .reference(toRef(resolvedVersion));

      final boolean isContentRequested = contentMode == ContentMode.ENTRY_WITH_CONTENT;
      if (isContentRequested) {
        requestBuilder.withContent(true);
      }

      List<String> filterTerms = new ArrayList<>();
      int depth = 1;
      if (catalogPath != null) {
        depth += catalogPath.size();
        if (depth > 1) {
          filterTerms.add(String.format("entry.encodedKey.startsWith('%s.')", Namespace.of(catalogPath).name()));
        }
      }
      boolean createVirtualImplicitNamespaces = false;
      if (nestingMode == NestingMode.IMMEDIATE_CHILDREN_ONLY) {
        createVirtualImplicitNamespaces = produceImplicitNamespaces;

        String comparator = createVirtualImplicitNamespaces ? ">=" : "==";
        filterTerms.add(String.format("size(entry.keyElements) %s %d", comparator, depth));
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

      Stream<Entry> entryStream = requestBuilder.stream();
      if (createVirtualImplicitNamespaces) {
        // http api V2 no longer supports "namespaceDepth" logic,
        // so we have to emulate it to keep supporting "produceImplicitNamespaces"
        // https://github.com/projectnessie/nessie/blob/2661599305cda2a2dfda05d7e56db92adb97817f/servers/services/src/main/java/org/projectnessie/services/impl/TreeApiImpl.java#L863-L891
        final int targetDepth = depth;
        Map<ContentKey, Entry> namespaces = new HashMap<>();
        Stream<Entry> realEntities = entryStream
          // real entries deeper than targetDepth get truncated to a namespace entry at targetDepth
          .map(e -> maybeTruncateToVirtualNamespace(e, targetDepth))
          // Since the above may produce duplicate namespace entries, we have to condense them.
          // First collect all namespaces into a map on the side and filter them out from the main stream,
          // then append deduplicated namespace entries to the end of the stream.
          .filter(e -> collectNamespaces(e, namespaces));

        // Use a lazy Spliterator supplier plus .flatMap() to allow the original stream to be iterated out before
        // the namespaces stream is constructed.
        // Note: Using Stream.concat(a, b) materializes `b` by requesting its size estimate at construction time.
        // Note: the number of namespaces is expected to be much smaller than the number of "real" entries.
        Stream<Entry> namespacesStream = StreamSupport.stream(() -> namespaces.values().spliterator(), 0, false);

        entryStream = Stream.of(realEntities, namespacesStream).flatMap(Function.identity());
      }
      return entryStream
        .map(entry -> toExternalNamespaceEntry(entry, resolvedVersion, isContentRequested));
    } catch (NessieNotFoundException e) {
      throw UserException.dataReadError(e).buildSilently();
    }
  }

  private static boolean collectNamespaces(Entry entry, Map<ContentKey, Entry> bag) {
    if (entry.getType() != Content.Type.NAMESPACE) {
      return true;
    }

    bag.compute(entry.getName(), (key, old) -> {
      if (old == null) {
        return entry;
      }

      // Prefer entries with full content data
      // Note: Having content, but no ID would be a bug in the Nessie Server.
      if (entry.getContent() != null && old.getContent() == null) {
        return entry;
      }

      // Prefer entries with IDs (relevant when content is not requested).
      if (entry.getContentId() != null && old.getContentId() == null) {
        return entry;
      }

      return old;
    });

    return false;
  }

  private static Entry maybeTruncateToVirtualNamespace(Entry entry, int depth) {
    List<String> nameElements = entry.getName().getElements();
    boolean truncateToNamespace = nameElements.size() > depth;
    if (truncateToNamespace) {
      // implicit namespace entry at target depth (virtual parent of real entry)
      ContentKey namespaceKey = ContentKey.of(nameElements.subList(0, depth));
      return Entry.entry(namespaceKey, Content.Type.NAMESPACE);
    }
    return entry;
  }

  private ExternalNamespaceEntry toExternalNamespaceEntry(
    Entry entry,
    ResolvedVersionContext resolvedVersion,
    boolean wasContentRequested
  ) {
    boolean typeHasContent = !Content.Type.NAMESPACE.equals(entry.getType());
    List<String> catalogKey = entry.getName().getElements();
    ContentKey contentKey = ContentKey.of(catalogKey);

    Content content = entry.getContent();
    if (content == null && typeHasContent && wasContentRequested) {
      logger.warn("listEntries content was requested but missing (catalogKey: {}, version: {})",
        catalogKey, resolvedVersion);
    }

    String contentId = entry.getContentId();
    if (contentId == null && typeHasContent) {
      // use content from response if available, otherwise try loading
      // note: content is not available unless explicity requested
      // note: implicit namespaces have no contentId, so there is no need to try loading
      if (content == null) {
        content = getContent(contentKey, resolvedVersion);
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

    Content.Type nessieType = entry.getType();
    Type dremioType = Type.fromNessieContentType(nessieType);
    if (contentId == null) {
      logger.warn("Nessie listEntries returned empty contentId (catalogKey: {}, version: {}): {}",
        catalogKey, resolvedVersion, nessieType);
      return ExternalNamespaceEntry.of(dremioType, catalogKey);
    }
    TableVersionContext tableVersionContext = TableVersionContext.of(resolvedVersion);
    @Nullable Optional<NessieContent> nessieContent = null;
    if (wasContentRequested) {
      nessieContent = Optional.ofNullable(content)
        .map(c -> NessieContent.buildFromRawContent(catalogKey, c));
    }
    return ExternalNamespaceEntry.of(dremioType, catalogKey, contentId, tableVersionContext, nessieContent);
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
          .message("CREATE FOLDER " + contentKey)
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

      boolean isNamespaceNotEmpty = listEntries(
        namespacePathList,
        resolvedVersion,
        // nested children not needed but allows the nessie server to read + transmit a single page
        NestingMode.INCLUDE_NESTED_CHILDREN,
        ContentMode.ENTRY_METADATA_ONLY, // content not needed for namespace empty check
        null,
        null)
        .findAny().isPresent();

      // This check is needed to support Nessie with namespace validation turned off via config; without it, Nessie
      // would delete the folder but leave all contents, essentially converting the folder to an implicit namespace
      // that is still shown in the catalog UI (i.e. deletion would silently do nothing)
      if (isNamespaceNotEmpty) {
        throw new NessieNamespaceNotEmptyException(contentKey.toPathString());
      }

      nessieApi
        .commitMultipleOperations()
        .branch((Branch) toRef(resolvedVersion))
        .operation(Operation.Delete.of(contentKey))
        .commitMeta(CommitMeta.builder()
          .author(authorName)
          .message("DROP FOLDER " + contentKey)
          .build())
        .commit();
    } catch (NessieReferenceNotFoundException e) {
      logger.error("Failed to delete namespace as Reference not found", e);
      throw new ReferenceNotFoundException(e);
    } catch (NessieConflictException e) {
      if (e instanceof NessieReferenceConflictException &&
        ((NessieReferenceConflictException) e).getErrorDetails().conflicts().stream().allMatch(
          (Conflict conflict) -> conflict.conflictType() == Conflict.ConflictType.NAMESPACE_NOT_EMPTY)) {
        logger.error("Failed to delete namespace as Namespace not empty", e);
        throw new NessieNamespaceNotEmptyException(contentKey.toPathString(), e);
      } else {
        logger.error("Failed to delete namespace due to Nessie conflict", e);
        throw new NessieNamespaceNotFoundException(e);
      }
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

  @WithSpan
  private Optional<Content> loadNessieContent(ContentKey contentKey, ResolvedVersionContext version) {
    return metrics.log("loadNessieContent", () -> loadNessieContentHelper(contentKey, version));
  }

  private Optional<Content> loadNessieContentHelper(ContentKey contentKey, ResolvedVersionContext version) {
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
      logger.debug("{} returned null", logPrefix);
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
    @Nullable IcebergCommitOrigin commitOrigin,
    String jobId,
    String userName) {
    metrics.log(
      "commitTable",
      () -> commitTableHelper(
        catalogKey,
        newMetadataLocation,
        nessieClientTableMetadata,
        version,
        baseContentId,
        commitOrigin,
        userName
      ));
  }

  private void commitTableHelper(
    List<String> catalogKey,
    String newMetadataLocation,
    NessieClientTableMetadata nessieClientTableMetadata,
    ResolvedVersionContext version,
    String baseContentId,
    @Nullable IcebergCommitOrigin commitOrigin,
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
    commitOperationHelper(
      contentKey,
      newTableBuilder.build(),
      version,
      commitOrigin,
      userName
    );
  }

  private void commitOperationHelper(
    ContentKey contentKey,
    Content content,
    ResolvedVersionContext version,
    @Nullable IcebergCommitOrigin commitOrigin,
    String userName
  ) {

    //TODO (DX-59840): Remove the UsernameContext and get the info from userName in DCS after testing there
    final UsernameContext usernameContext = RequestContext.current().get(UsernameContext.CTX_KEY);
    final String authorName = usernameContext != null ? usernameContext.getUserName() : userName;

    String commitMessage;
    if (commitOrigin == null) {
      // fallback logic
      String createOrUpdate = content.getId() == null ? "CREATE" : "UPDATE on";
      String contentType = Content.Type.ICEBERG_TABLE.equals(content.getType()) ? "TABLE" : "VIEW";
      commitMessage = String.join(" ",
        createOrUpdate, contentType, contentKey.toString());
    } else {
      if (commitOrigin == IcebergCommitOrigin.READ_ONLY) {
        throw new IllegalArgumentException("Unable to commit a READ_ONLY origin!");
      }
      VersionedPlugin.EntityType entityType = Content.Type.ICEBERG_TABLE.equals(content.getType()) ?
        EntityType.ICEBERG_TABLE : EntityType.ICEBERG_VIEW;
      commitMessage = commitOrigin.createCommitMessage(contentKey.toString(), entityType);
    }

    try {
      nessieApi
        .commitMultipleOperations()
        .branch((Branch) toRef(version))
        .operation(Operation.Put.of(contentKey, content))
        .commitMeta(CommitMeta.builder()
          .author(authorName)
          .message(commitMessage)
          .build())
        .commit();
    } catch (NessieConflictException e) {
      //Converting to CommitFailedException. Iceberg will retry the operation automatically 4 times when a  CommitFailedException is thrown.
      //Take a look in org.apache.iceberg.BaseTransaction#commitSimpleTransaction where commit and retry (exponentially)  happens
      logger.error("Commit operation failed", e);
      throw new CommitFailedException(e, "Failed to commit operation due to %s", e.getMessage());
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
    @Nullable IcebergCommitOrigin commitOrigin,
    String userName) {
    metrics.log(
      "commitView",
      () ->
        commitViewHelper(
          catalogKey,
          newMetadataLocation,
          icebergView,
          metadata,
          dialect,
          version,
          baseContentId,
          commitOrigin,
          userName
        ));
  }

  private void commitViewHelper(
    List<String> catalogKey,
    String newMetadataLocation,
    IcebergView icebergView,
    ViewVersionMetadata metadata,
    String dialect,
    ResolvedVersionContext version,
    String baseContentId,
    @Nullable IcebergCommitOrigin commitOrigin,
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
    commitOperationHelper(
      contentKey,
      newViewBuilder.build(),
      version,
      commitOrigin,
      userName
    );
  }

  @Override
  @WithSpan
  public void deleteCatalogEntry(
    List<String> catalogKey,
    VersionedPlugin.EntityType entityType,
    ResolvedVersionContext version,
    String userName
  ) {
    metrics.log("deleteCatalogEntry",
      () -> deleteCatalogEntryHelper(catalogKey, entityType, version, userName));
  }

  private void deleteCatalogEntryHelper(
    List<String> catalogKey,
    VersionedPlugin.EntityType entityType,
    ResolvedVersionContext version,
    String userName
  ) {
    ensureOperationOnBranch(version);
    final Reference versionRef = toRef(version);
    final ContentKey contentKey = ContentKey.of(catalogKey);
    logger.debug("Deleting entry in Nessie for key {} ", contentKey);
    // Check if reference exists to give back a proper error
    Content content = getContent(contentKey, version);
    if (content == null) {
      logger.debug("Tried to delete key : {} but it was not found in nessie ", catalogKey);
      throw UserException.validationError()
        .message(String.format("Key not found in nessie for  %s", catalogKey))
        .buildSilently();
    }
    NessieContent nessieContent = NessieContent.buildFromRawContent(catalogKey, content);
    VersionedPlugin.EntityType actualEntityType = nessieContent.getEntityType();
    if (entityType != null && actualEntityType != entityType) {
      throw UserException.validationError()
        .message(String.format("Key %s has type %s but expected %s", catalogKey, actualEntityType, entityType))
        .buildSilently();
    }
    String entityTypeName = toCommitMsgTypeName(actualEntityType);

    //TODO (DX-59840): Remove the UsernameContext and get the info from userName in DCS after testing there
    final UsernameContext usernameContext = RequestContext.current().get(UsernameContext.CTX_KEY);
    final String authorName = usernameContext != null ? usernameContext.getUserName() : userName;

    String commitMessage = String.join(" ",
      "DROP", entityTypeName, contentKey.toString());

    try {
      nessieApi
        .commitMultipleOperations()
        .branchName(versionRef.getName())
        .hash(versionRef.getHash())
        .operation(Operation.Delete.of(contentKey))
        .commitMeta(CommitMeta.builder()
          .author(authorName)
          .message(commitMessage)
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

  private static String toCommitMsgTypeName(EntityType entityType) {
    switch (entityType) {
      case FOLDER:
        return "FOLDER";
      case ICEBERG_TABLE:
        return "TABLE";
      case ICEBERG_VIEW:
        return "VIEW";
      case UNKNOWN:
      default:
        return "UNKNOWN_CATALOG_ENTRY";
    }
  }

  private Reference getReference(VersionContext versionContext) {
    Preconditions.checkNotNull(versionContext);
    final String referenceName = Preconditions.checkNotNull(versionContext.getValue());
    Preconditions.checkState(versionContext.getTimestamp() == null);

    try {
      return nessieApi.getReference()
        .refName(referenceName)
        .get();
    } catch (NessieNotFoundException e) {
      String error = versionContext.toStringFirstLetterCapitalized() + " is not found";
      logger.error(error, e);
      throw new ReferenceNotFoundException(error);
    }
  }

  private String getHashAtTimestamp(VersionContext versionContext) {
    Preconditions.checkNotNull(versionContext);
    final String referenceName = Preconditions.checkNotNull(versionContext.getValue());
    final Instant timestamp = Preconditions.checkNotNull(versionContext.getTimestamp());

    final String referenceTimestampLookup = "*" + timestamp.toEpochMilli();

    try {
      List<LogEntry> logEntries = nessieApi.getCommitLog()
        .refName(referenceName)
        .hashOnRef(referenceTimestampLookup)
        .fetch(FetchOption.MINIMAL)
        .maxRecords(1)
        .get()
        .getLogEntries();
      if (logEntries.isEmpty()) {
        String error = String.format(
          "There are no commits at or before timestamp '%s' in reference '%s'. Please specify another timestamp",
          Timestamp.from(timestamp),
          referenceName);
        logger.error(error);
        throw new ReferenceNotFoundByTimestampException(error);
      }
      String hash = logEntries
        .get(0)
        .getCommitMeta()
        .getHash();

      if (hash == null) {
        String error = versionContext.toStringFirstLetterCapitalized() + " is not found";
        logger.error(error);
        throw new ReferenceNotFoundByTimestampException(error);
      }
      return hash;
    } catch (NessieNotFoundException e) {
      String error = versionContext.toStringFirstLetterCapitalized() + " is not found";
      logger.error(error, e);
      throw new ReferenceNotFoundByTimestampException(error);
    }
  }

  private Reference toRef(ResolvedVersionContext resolvedVersionContext) {
    Preconditions.checkNotNull(resolvedVersionContext);
    switch (resolvedVersionContext.getType()) {
      case BRANCH:
        return Branch.of(resolvedVersionContext.getRefName(), resolvedVersionContext.getCommitHash());
      case TAG:
        return Tag.of(resolvedVersionContext.getRefName(), resolvedVersionContext.getCommitHash());
      case COMMIT:
        return Detached.of(resolvedVersionContext.getCommitHash());
      default:
        throw new IllegalStateException("Unexpected value: " + resolvedVersionContext.getType());
    }
  }

  @Override
  public Optional<NessieContent> getContent(List<String> catalogKey, ResolvedVersionContext version, String jobId) {
    Content rawContent = getContent(ContentKey.of(catalogKey), version);
    return Optional.ofNullable(rawContent)
      .map(con -> NessieContent.buildFromRawContent(catalogKey, con));
  }

  @Override
  public NessieApiV2 getNessieApi() {
    Preconditions.checkState(!apiClosed);
    return nessieApi;
  }

  @Override
  public <T> T callWithContext(String jobId, Callable<T> callable) throws Exception {
    return callable.call();
  }

  @Override
  public void close() {
    if (VM.areAssertsEnabled() && apiClosed) {
      throw new IllegalStateException("nessieApi already closed:\n" + apiCloseStacktrace);
    }
    nessieApi.close();

    apiClosed = true;
    if (VM.areAssertsEnabled()) {
      apiCloseStacktrace = VM.getCurrentStackTraceAsString();
    }
  }
}
