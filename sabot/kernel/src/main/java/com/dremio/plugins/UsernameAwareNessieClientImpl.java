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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.MergeResponse;

/**
 * This class acts as a decorator and resolves the uuid to the corresponding username string. The
 * username string can be used by referencing the {@link NessieCommitUsernameContext}.
 */
public class UsernameAwareNessieClientImpl implements NessieClient {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(UsernameAwareNessieClientImpl.class);
  private final NessieClient nessieClient;
  private final UserService userService;
  private final LoadingCache<UID, String> userNameByUserIdCache;

  public UsernameAwareNessieClientImpl(NessieClient nessieClient, UserService userService) {
    this.nessieClient = Preconditions.checkNotNull(nessieClient);
    this.userService = Preconditions.checkNotNull(userService);
    this.userNameByUserIdCache =
        Caffeine.newBuilder()
            .maximumSize(300) // items
            .expireAfterAccess(20, TimeUnit.MINUTES)
            .build(this::fetchUserNameForUser);
  }

  private @Nullable String fetchUserNameForUser(UID userId) {
    try {
      User user = userService.getUser(userId);
      return user.getUserName();
    } catch (UserNotFoundException e) {
      logger.warn("User not found: {}", userId, e);
    }
    return null;
  }

  private @Nullable String getUserNameForUserId(String userId) {
    if (UserContext.SYSTEM_USER_CONTEXT_ID.equals(userId)) {
      return UserContext.SYSTEM_USER_NAME;
    }
    return userNameByUserIdCache.get(new UID(userId));
  }

  private RequestContext getRequestContextWithUsernameContext() {
    RequestContext reqContext = RequestContext.current();
    UserContext userContext = reqContext.get(UserContext.CTX_KEY);
    boolean hasUsernameContext = reqContext.get(NessieCommitUsernameContext.CTX_KEY) != null;
    if (userContext != null && !hasUsernameContext) {
      // ideally the userName would be available in UserContext directly but since DataplanePlugin
      // calls NessieClient methods multiple times during a single request we use a cache here
      String userName = getUserNameForUserId(userContext.getUserId());
      if (userName != null) {
        reqContext =
            reqContext.with(
                NessieCommitUsernameContext.CTX_KEY, new NessieCommitUsernameContext(userName));
      }
    }
    return reqContext;
  }

  private <T> T callWithUsernameContext(Callable<T> callable) {
    try {
      return getRequestContextWithUsernameContext().call(callable);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private <E> Stream<E> callStreamWithUsernameContext(Callable<Stream<E>> callable) {
    try {
      return getRequestContextWithUsernameContext().callStream(callable);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResolvedVersionContext getDefaultBranch() {
    return callWithUsernameContext(nessieClient::getDefaultBranch);
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(VersionContext versionContext) {
    return callWithUsernameContext(() -> nessieClient.resolveVersionContext(versionContext));
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(VersionContext versionContext, String jobId) {
    return callWithUsernameContext(() -> nessieClient.resolveVersionContext(versionContext));
  }

  @Override
  public boolean commitExists(String commitHash) {
    return callWithUsernameContext(() -> nessieClient.commitExists(commitHash));
  }

  @Override
  public Stream<ReferenceInfo> listBranches() {
    return callStreamWithUsernameContext(nessieClient::listBranches);
  }

  @Override
  public Stream<ReferenceInfo> listTags() {
    return callStreamWithUsernameContext(nessieClient::listTags);
  }

  @Override
  public Stream<ReferenceInfo> listReferences() {
    return callStreamWithUsernameContext(nessieClient::listReferences);
  }

  @Override
  public Stream<ChangeInfo> listChanges(VersionContext version) {
    return callStreamWithUsernameContext(() -> nessieClient.listChanges(version));
  }

  @Override
  public Stream<ExternalNamespaceEntry> listEntries(
      @Nullable List<String> catalogPath,
      ResolvedVersionContext resolvedVersion,
      NestingMode nestingMode,
      ContentMode contentMode,
      @Nullable Set<ExternalNamespaceEntry.Type> contentTypeFilter,
      @Nullable String celFilter) {
    return callStreamWithUsernameContext(
        () ->
            nessieClient.listEntries(
                catalogPath,
                resolvedVersion,
                nestingMode,
                contentMode,
                contentTypeFilter,
                celFilter));
  }

  @Override
  public NessieListResponsePage listEntriesPage(
      @Nullable List<String> catalogPath,
      ResolvedVersionContext resolvedVersion,
      NestingMode nestingMode,
      ContentMode contentMode,
      @Nullable Set<ExternalNamespaceEntry.Type> contentTypeFilter,
      @Nullable String celFilter,
      NessieListOptions options) {
    return callWithUsernameContext(
        () ->
            nessieClient.listEntriesPage(
                catalogPath,
                resolvedVersion,
                nestingMode,
                contentMode,
                contentTypeFilter,
                celFilter,
                options));
  }

  @Override
  public void createNamespace(List<String> namespacePathList, VersionContext version) {
    getRequestContextWithUsernameContext()
        .run(() -> nessieClient.createNamespace(namespacePathList, version));
  }

  @Override
  public void deleteNamespace(List<String> namespacePathList, VersionContext version) {
    getRequestContextWithUsernameContext()
        .run(() -> nessieClient.deleteNamespace(namespacePathList, version));
  }

  @Override
  public void createBranch(String branchName, VersionContext sourceVersion) {
    getRequestContextWithUsernameContext()
        .run(() -> nessieClient.createBranch(branchName, sourceVersion));
  }

  @Override
  public void createTag(String tagName, VersionContext sourceVersion) {
    getRequestContextWithUsernameContext()
        .run(() -> nessieClient.createTag(tagName, sourceVersion));
  }

  @Override
  public void dropBranch(String branchName, String branchHash) {
    getRequestContextWithUsernameContext()
        .run(() -> nessieClient.dropBranch(branchName, branchHash));
  }

  @Override
  public void dropTag(String tagName, String tagHash) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.dropTag(tagName, tagHash));
  }

  @Override
  public MergeResponse mergeBranch(
      String sourceBranchName, String targetBranchName, MergeBranchOptions mergeBranchOptions) {
    return callWithUsernameContext(
        () -> nessieClient.mergeBranch(sourceBranchName, targetBranchName, mergeBranchOptions));
  }

  @Override
  public void assignBranch(String branchName, VersionContext sourceVersion) {
    getRequestContextWithUsernameContext()
        .run(() -> nessieClient.assignBranch(branchName, sourceVersion));
  }

  @Override
  public void assignTag(String tagName, VersionContext sourceVersion) {
    getRequestContextWithUsernameContext()
        .run(() -> nessieClient.assignTag(tagName, sourceVersion));
  }

  @Override
  public void commitTable(
      List<String> catalogKey,
      String newMetadataLocation,
      NessieTableAdapter nessieTableAdapter,
      ResolvedVersionContext version,
      String baseContentId,
      @Nullable IcebergCommitOrigin commitOrigin,
      String jobId,
      String userName) {
    getRequestContextWithUsernameContext()
        .run(
            () ->
                nessieClient.commitTable(
                    catalogKey,
                    newMetadataLocation,
                    nessieTableAdapter,
                    version,
                    baseContentId,
                    commitOrigin,
                    jobId,
                    userName));
  }

  @Override
  public void commitView(
      List<String> catalogKey,
      String newMetadataLocation,
      NessieViewAdapter nessieViewMetadata,
      ResolvedVersionContext version,
      String baseContentId,
      @Nullable IcebergCommitOrigin commitOrigin,
      String userName) {
    getRequestContextWithUsernameContext()
        .run(
            () ->
                nessieClient.commitView(
                    catalogKey,
                    newMetadataLocation,
                    nessieViewMetadata,
                    version,
                    baseContentId,
                    commitOrigin,
                    userName));
  }

  @Override
  public void commitUdf(
      List<String> catalogKey,
      String newMetadataLocation,
      NessieUdfAdapter nessieUdfMetadata,
      ResolvedVersionContext version,
      String baseContentId,
      @Nullable IcebergCommitOrigin commitOrigin,
      String userName) {
    getRequestContextWithUsernameContext()
        .run(
            () ->
                nessieClient.commitUdf(
                    catalogKey,
                    newMetadataLocation,
                    nessieUdfMetadata,
                    version,
                    baseContentId,
                    commitOrigin,
                    userName));
  }

  @Override
  public void deleteCatalogEntry(
      List<String> catalogKey,
      VersionedPlugin.EntityType entityType,
      ResolvedVersionContext version,
      String userName) {
    getRequestContextWithUsernameContext()
        .run(() -> nessieClient.deleteCatalogEntry(catalogKey, entityType, version, userName));
  }

  @Override
  public Optional<NessieContent> getContent(
      List<String> catalogKey, ResolvedVersionContext version, String jobId) {
    return callWithUsernameContext(() -> nessieClient.getContent(catalogKey, version, jobId));
  }

  @Override
  public NessieApiV2 getNessieApi() {
    return nessieClient.getNessieApi();
  }

  @Override
  public <T> T callWithContext(String jobId, Callable<T> callable) throws Exception {
    return callWithUsernameContext(() -> callable.call());
  }

  @Override
  public void close() {
    getRequestContextWithUsernameContext().run(nessieClient::close);
  }
}
