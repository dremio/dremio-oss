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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.iceberg.view.ViewVersionMetadata;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.model.IcebergView;

import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.context.UsernameContext;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;
import com.google.common.base.Preconditions;

/**
 * This class acts as a decorator and resolves the uuid to the corresponding username string.
 * The username string can be used by referencing the UsernameContext.
 *
 * TODO:
 * Once the ticket DX-64013 [Refactoring of "user_group_ctx_key"] is completed, this class is
 * unnecessary and should be removed in lieu of simply able to extract the context key directly
 * in createNamespace/ deleteNamespace methods where we need to pass the authorname only while committing to Nessie
 * Refer to the epic DX-64087: Remove UsernameAwareNessieClientImpl class
 */
public class UsernameAwareNessieClientImpl implements NessieClient {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UsernameAwareNessieClientImpl.class);
  private final NessieClient nessieClient;
  private final UserService userService;

  public UsernameAwareNessieClientImpl(NessieClient nessieClient, UserService userService) {
    this.nessieClient = nessieClient;
    logger.debug("UserService is not null: {}", !Objects.isNull(userService));
    Preconditions.checkArgument(!Objects.isNull(userService), "User Service is required");
    this.userService = userService;
  }

  private RequestContext getRequestContextWithUsernameContext() {
    RequestContext currentRequestContext = RequestContext.current();
    UserContext userContext = currentRequestContext.get(UserContext.CTX_KEY);
    boolean hasUsernameContext = Objects.nonNull(currentRequestContext.get(UsernameContext.CTX_KEY));
    if (userContext != null && !hasUsernameContext) {
      try {
        User user = userService.getUser(new UID(userContext.getUserId()));
        UsernameContext usernameContext = new UsernameContext(user.getUserName());
        currentRequestContext = currentRequestContext.with(UsernameContext.CTX_KEY, usernameContext);
      } catch (UserNotFoundException e) { // User not found. Skip adding UsernameContext.
        logger.debug("User not found due to : {}", e.getMessage());
      }
    }
    return currentRequestContext;
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
    return callWithUsernameContext(nessieClient::listBranches);
  }

  @Override
  public Stream<ReferenceInfo> listTags() {
    return callWithUsernameContext(nessieClient::listTags);
  }

  @Override
  public Stream<ReferenceInfo> listReferences() {
    try {
      return getRequestContextWithUsernameContext().call(nessieClient::listReferences);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<ChangeInfo> listChanges(VersionContext version) {
    return callWithUsernameContext(() -> nessieClient.listChanges(version));
  }

  @Override
  public Stream<ExternalNamespaceEntry> listEntries(
    @Nullable List<String> catalogPath,
    ResolvedVersionContext resolvedVersion,
    NestingMode nestingMode,
    @Nullable Set<ExternalNamespaceEntry.Type> contentTypeFilter,
    @Nullable String celFilter) {
    return callWithUsernameContext(() ->
      nessieClient.listEntries(catalogPath, resolvedVersion, nestingMode, contentTypeFilter, celFilter));
  }

  @Override
  public void createNamespace(List<String> namespacePathList, VersionContext version) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.createNamespace(namespacePathList, version));
  }

  @Override
  public void deleteNamespace(List<String> namespacePathList, VersionContext version) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.deleteNamespace(namespacePathList, version));
  }

  @Override
  public void createBranch(String branchName, VersionContext sourceVersion) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.createBranch(branchName, sourceVersion));
  }

  @Override
  public void createTag(String tagName, VersionContext sourceVersion) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.createTag(tagName, sourceVersion));
  }

  @Override
  public void dropBranch(String branchName, String branchHash) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.dropBranch(branchName, branchHash));
  }

  @Override
  public void dropTag(String tagName, String tagHash) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.dropTag(tagName, tagHash));
  }

  @Override
  public void mergeBranch(String sourceBranchName, String targetBranchName) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.mergeBranch(sourceBranchName, targetBranchName));
  }

  @Override
  public void assignBranch(String branchName, VersionContext sourceVersion) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.assignBranch(branchName, sourceVersion));
  }

  @Override
  public void assignTag(String tagName, VersionContext sourceVersion) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.assignTag(tagName, sourceVersion));
  }

  @Override
  public String getMetadataLocation(List<String> catalogKey, ResolvedVersionContext version, String jobId) {
    return callWithUsernameContext(() -> nessieClient.getMetadataLocation(catalogKey, version, jobId));
  }

  @Override
  public Optional<String> getViewDialect(List<String> catalogKey, ResolvedVersionContext version) {
    return callWithUsernameContext(() -> nessieClient.getViewDialect(catalogKey, version));
  }

  @Override
  public void commitTable(List<String> catalogKey,
                          String newMetadataLocation,
                          NessieClientTableMetadata nessieClientTableMetadata,
                          ResolvedVersionContext version,
                          String baseContent,
                          String jobId,
                          String userName) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.commitTable(catalogKey, newMetadataLocation, nessieClientTableMetadata, version, baseContent, jobId, userName));
  }

  @Override
  public void commitView(List<String> catalogKey,
                         String newMetadataLocation,
                         IcebergView icebergView,
                         ViewVersionMetadata metadata,
                         String dialect,
                         ResolvedVersionContext version,
                         String baseContentId,
                         String userName) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.commitView(catalogKey, newMetadataLocation, icebergView, metadata, dialect, version, baseContentId, userName));
  }

  @Override
  public void deleteCatalogEntry(List<String> catalogKey, ResolvedVersionContext version, String userName) {
    getRequestContextWithUsernameContext().run(() -> nessieClient.deleteCatalogEntry(catalogKey, version, userName));
  }

  @Override
  public VersionedPlugin.EntityType getVersionedEntityType(List<String> tableKey, ResolvedVersionContext version) {
    return callWithUsernameContext(() -> nessieClient.getVersionedEntityType(tableKey, version));
  }

  @Override
  public String getContentId(List<String> tableKey, ResolvedVersionContext version, String jobId) {
    return callWithUsernameContext(() -> nessieClient.getContentId(tableKey, version, jobId));
  }

  @Override
  public NessieApi getNessieApi() {
    return nessieClient.getNessieApi();
  }

  @Override
  public void close() {
    getRequestContextWithUsernameContext().run(nessieClient::close);
  }
}
