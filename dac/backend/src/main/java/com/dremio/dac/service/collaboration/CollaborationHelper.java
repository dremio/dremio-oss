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
package com.dremio.dac.service.collaboration;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.proto.model.collaboration.CollaborationWiki;
import com.dremio.dac.service.search.SearchService;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;
import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;

/** Wrapper class for interactions with the collaboration store */
public class CollaborationHelper {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CollaborationHelper.class);

  private static final String DEFAULT_HOME_WIKI_TEXT =
      "#  Wikis & Labels\n"
          + "\n"
          + "![Gnarly Catalog](https://d33wubrfki0l68.cloudfront.net/c1a54376c45a9276c080f3d10ed25ce61c17bcd2/2b946/img/home/open-source-for-everyone.svg)\n"
          + "\n"
          + "You are reading the wiki for your home space! You can create and edit this information for any source, space, or folder."
          + "\n"
          + "\n"
          + "This sidebar always shows the wiki for the current source, space or folder you are browsing.\n"
          + "\n"
          + "When previewing datasets, click on the `Catalog` tab to create a wiki or add labels to that dataset.\n"
          + "\n"
          + "**Tip:** You can hide the wiki by clicking on the sidebar icon on upper right hand side.";

  private final CollaborationTagStore tagsStore;
  private final CollaborationWikiStore wikiStore;
  private final NamespaceService namespaceService;
  private final SecurityContext securityContext;
  private final SearchService searchService;
  private final UserService userService;
  private final CatalogService catalogService;
  private final OptionManager optionManager;

  @Inject
  public CollaborationHelper(
      final LegacyKVStoreProvider kvStoreProvider,
      final NamespaceService namespaceService,
      final SecurityContext securityContext,
      final SearchService searchService,
      final UserService userService,
      final CatalogService catalogService,
      OptionManager optionManager) {
    this.tagsStore = new CollaborationTagStore(kvStoreProvider);
    this.wikiStore = new CollaborationWikiStore(kvStoreProvider);
    this.namespaceService = namespaceService;
    this.securityContext = securityContext;
    this.searchService = searchService;
    this.userService = userService;
    this.catalogService = catalogService;
    this.optionManager = optionManager;
  }

  public Optional<Tags> getTags(String entityId) throws NamespaceException {
    VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(entityId);
    if (versionedDatasetId != null) {
      checkVersionedWikiLabelFeatureFlag(versionedDatasetId);
      validateVersionedEntity(versionedDatasetId);
    } else {
      validateNameSpaceEntity(entityId);
    }
    final Optional<CollaborationTag> tags = tagsStore.getTagsForEntityId(entityId);
    return tags.map(Tags::fromCollaborationTag);
  }

  public void setTags(String entityId, Tags tags) throws NamespaceException {
    Preconditions.checkNotNull(entityId, "Entity id is required.");
    validateEntity(entityId);

    final CollaborationTag collaborationTag = new CollaborationTag();
    collaborationTag.setTagsList(tags.getTags());
    collaborationTag.setTag(tags.getVersion());
    collaborationTag.setEntityId(entityId);

    final Optional<CollaborationTag> existingTag = tagsStore.getTagsForEntityId(entityId);

    // If it is an update, copy over the id, so we overwrite the existing entry.
    collaborationTag.setId(
        existingTag.map(CollaborationTag::getId).orElse(UUID.randomUUID().toString()));

    tagsStore.save(collaborationTag);
    getSearchService().wakeupManager("Labels changed");
  }

  public Optional<Wiki> getWiki(String entityId) throws NamespaceException {
    VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(entityId);
    if (versionedDatasetId != null) {
      checkVersionedWikiLabelFeatureFlag(versionedDatasetId);
      validateVersionedEntity(versionedDatasetId);
      Optional<CollaborationWiki> wiki = getWikiStore().getLatestWikiForEntityId(entityId);
      return wiki.map(Wiki::fromCollaborationWiki);
    }

    NameSpaceContainer container = validateNameSpaceEntity(entityId);

    Optional<CollaborationWiki> wiki = getWikiStore().getLatestWikiForEntityId(entityId);

    if (!wiki.isPresent()) {
      // check if container has a description and migrate it.
      String description = getDescription(container);

      if (description != null) {
        setWiki(entityId, new Wiki(description, null));
        wiki = getWikiStore().getLatestWikiForEntityId(entityId);
      }
    }

    return wiki.map(Wiki::fromCollaborationWiki);
  }

  private String getDescription(NameSpaceContainer container) {
    String description = null;
    switch (container.getType()) {
      case SOURCE:
        description = container.getSource().getDescription();
        break;
      case SPACE:
        description = container.getSpace().getDescription();
        break;
      case HOME:
        description = DEFAULT_HOME_WIKI_TEXT;
        break;
      default:
        break;
    }

    return description;
  }

  public void setWiki(String entityId, Wiki wiki) throws NamespaceException {
    Preconditions.checkNotNull(entityId, "Entity id is required.");
    validateEntity(entityId);

    final CollaborationWiki collaborationWiki = new CollaborationWiki();
    collaborationWiki.setText(wiki.getText());
    collaborationWiki.setCreatedAt(System.currentTimeMillis());

    // store the user
    User user;
    try {
      user = getUserService().getUser(getSecurityContext().getUserPrincipal().getName());
    } catch (UserNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Could not load user [%s].", getSecurityContext().getUserPrincipal().getName()));
    }

    collaborationWiki.setId(UUID.randomUUID().toString());
    collaborationWiki.setUserId(user.getUID().getId());

    final Optional<CollaborationWiki> existingWiki =
        getWikiStore().getLatestWikiForEntityId(entityId);

    collaborationWiki.setVersion(
        existingWiki
            .map(
                l -> {
                  // check if versions match
                  Long existingVersion = l.getVersion();
                  Long newVersion = wiki.getVersion();

                  if (!existingVersion.equals(newVersion)) {
                    throw new ConcurrentModificationException(
                        String.format(
                            "The provided version [%s] does not match the stored version [%s].",
                            newVersion, existingVersion));
                  }

                  // We create a new entry for each update but keep incrementing the version (for
                  // sorting).  Because we do this we can't
                  // rely on the store to do the version incrementing and therefore do it manually.
                  return existingVersion + 1L;
                })
            .orElse(0L));

    collaborationWiki.setEntityId(entityId);

    getWikiStore().save(collaborationWiki);
  }

  private void validateEntity(String entityId) throws NamespaceException {
    VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(entityId);
    if (versionedDatasetId != null) {
      validateVersionedEntity(versionedDatasetId);
    } else {
      validateNameSpaceEntity(entityId);
    }
  }

  // will copy wiki to entity if target entity does not have wiki, throws exception otherwise
  public void copyWiki(String fromEntityId, String toEntityId) throws NamespaceException {
    Optional<Wiki> wiki = getWiki(fromEntityId);

    if (wiki.isPresent()) {
      setWiki(
          toEntityId,
          new Wiki(wiki.get().getText(), null)); // throws an exception, if there is a wiki already
    }
  }

  // will copy to entity if target entity does not have wiki, throws exception otherwise
  public void copyTags(String fromEntityId, String toEntityId) throws NamespaceException {
    Optional<Tags> tags = getTags(fromEntityId);

    if (tags.isPresent()) {
      setTags(
          toEntityId,
          new Tags(
              tags.get().getTags(), null)); // throws an exception, if there are the tags already
    }
  }

  private NameSpaceContainer validateNameSpaceEntity(String entityId) throws NamespaceException {
    final NameSpaceContainer entity = getNamespaceService().getEntityById(entityId);
    if (entity == null) {
      throw new IllegalArgumentException(
          String.format("Could not find entity with id [%s].", entityId));
    }

    return entity;
  }

  private void validateVersionedEntity(VersionedDatasetId versionedDatasetId) {
    checkVersionedWikiLabelFeatureFlag(versionedDatasetId);
    Catalog catalog =
        getCatalogService()
            .getCatalog(
                MetadataRequestOptions.newBuilder()
                    .setSchemaConfig(
                        SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME))
                            .build())
                    .setCheckValidity(false)
                    .setNeverPromote(true)
                    .build());

    if (!CatalogUtil.versionedEntityExists(catalog, versionedDatasetId)) {
      throw new IllegalArgumentException(
          String.format(
              "Could not find entity with key '%s' in '%s'.",
              CatalogEntityKey.newBuilder().keyComponents(versionedDatasetId.getTableKey()).build(),
              versionedDatasetId.getVersionContext().toString()));
    }
    checkIfDefaultBranch(versionedDatasetId, catalog);
  }

  public static int pruneOrphans(LegacyKVStoreProvider kvStoreProvider) {
    final AtomicInteger results = new AtomicInteger();
    final NamespaceServiceImpl namespaceService =
        new NamespaceServiceImpl(kvStoreProvider, new CatalogStatusEventsImpl());

    // check tags for orphans
    final CollaborationTagStore tagsStore = new CollaborationTagStore(kvStoreProvider);
    StreamSupport.stream(tagsStore.find().spliterator(), false)
        .filter(
            entry -> {
              if (VersionedDatasetId.isVersionedDatasetId(entry.getValue().getEntityId())) {
                return false;
              }
              try {
                final NameSpaceContainer container =
                    namespaceService.getEntityById(entry.getValue().getEntityId());
                return container == null;
              } catch (NamespaceException e) {
                return false;
              }
            })
        .forEach(
            entry -> {
              results.getAndIncrement();
              tagsStore.delete(entry.getKey());
            });

    // check wikis for orphans
    final CollaborationWikiStore wikiStore = new CollaborationWikiStore(kvStoreProvider);
    StreamSupport.stream(wikiStore.find().spliterator(), false)
        .filter(
            entry -> {
              if (VersionedDatasetId.isVersionedDatasetId(entry.getValue().getEntityId())) {
                return false;
              }
              try {
                final NameSpaceContainer container =
                    namespaceService.getEntityById(entry.getValue().getEntityId());
                return container == null;
              } catch (NamespaceException e) {
                return false;
              }
            })
        .forEach(
            entry -> {
              results.getAndIncrement();
              wikiStore.delete(entry.getKey());
            });

    return results.get();
  }

  public TagsSearchResult getTagsForIds(Set<String> ids) {
    // If you alter this number, alter a message in TagsAlert.js
    final int maxTagRequestCount = 200;

    LegacyFindByCondition findByCondition = new LegacyFindByCondition();
    Map<String, CollaborationTag> tags = new HashMap<>();

    List<SearchQuery> queries = new ArrayList<>();
    ids.stream()
        .limit(maxTagRequestCount)
        .forEach(
            input ->
                queries.add(SearchQueryUtils.newTermQuery(CollaborationTagStore.ENTITY_ID, input)));

    findByCondition.setCondition(SearchQueryUtils.or(queries));

    tagsStore.find(findByCondition).forEach(pair -> tags.put(pair.getKey(), pair.getValue()));

    return new TagsSearchResult(tags, ids.size() > maxTagRequestCount);
  }

  private void checkIfDefaultBranch(VersionedDatasetId versionedDatasetId, Catalog catalog) {
    String sourceName = versionedDatasetId.getTableKey().get(0);
    if ((versionedDatasetId.getVersionContext().getType() != TableVersionType.BRANCH)
        || !(CatalogUtil.getDefaultBranch(sourceName, catalog)
            .equals(versionedDatasetId.getVersionContext().getValue()))) {
      throw UserException.validationError()
          .message("Wiki and Label can only be set on the default branch")
          .build(logger);
    }
  }

  private void checkVersionedWikiLabelFeatureFlag(VersionedDatasetId versionedDatasetId) {
    if (!getOptionManager()
        .getOption(CatalogOptions.WIKILABEL_ENABLED_FOR_VERSIONED_SOURCE_DEFAULT_BRANCH)) {
      throw UserException.validationError()
          .message(
              "Wiki and Label not supported on entities in source [%s]",
              versionedDatasetId.getTableKey().get(0))
          .build(logger);
    }
  }

  protected CollaborationWikiStore getWikiStore() {
    return wikiStore;
  }

  protected NamespaceService getNamespaceService() {
    return namespaceService;
  }

  protected SecurityContext getSecurityContext() {
    return securityContext;
  }

  protected SearchService getSearchService() {
    return searchService;
  }

  protected UserService getUserService() {
    return userService;
  }

  protected CatalogService getCatalogService() {
    return catalogService;
  }

  protected OptionManager getOptionManager() {
    return optionManager;
  }
}
