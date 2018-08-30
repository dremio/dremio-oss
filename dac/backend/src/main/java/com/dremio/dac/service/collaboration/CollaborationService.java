/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.ConcurrentModificationException;
import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.proto.model.collaboration.CollaborationWiki;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Wrapper class for interactions with the collaboration store
 */
public class CollaborationService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CollaborationService.class);

  private final CollaborationTagStore tagsStore;
  private final CollaborationWikiStore wikiStore;
  private final NamespaceService namespaceService;
  private final SecurityContext context;
  private final SabotContext sabotContext;

  @Inject
  public CollaborationService(KVStoreProvider kvStoreProvider, SabotContext sabotContext, NamespaceService namespaceService, SecurityContext context) {
    this.tagsStore = new CollaborationTagStore(kvStoreProvider);
    this.wikiStore = new CollaborationWikiStore(kvStoreProvider);
    this.namespaceService = namespaceService;
    this.context = context;
    this.sabotContext = sabotContext;
  }

  public Optional<Tags> getTags(String entityId) throws NamespaceException {
    validateEntityForTag(entityId);

    final Optional<CollaborationTag> tags = tagsStore.getTagsForEntityId(entityId);

    return tags.transform(Tags::fromCollaborationTag);
  }

  public void setTags(String entityId, Tags tags) throws NamespaceException {
    Preconditions.checkNotNull(entityId, "Entity id is required.");
    validateEntityForTag(entityId);

    final CollaborationTag collaborationTag = new CollaborationTag();
    collaborationTag.setTagsList(tags.getTags());
    collaborationTag.setVersion(tags.getVersion());
    collaborationTag.setEntityId(entityId);

    final Optional<CollaborationTag> existingTag = tagsStore.getTagsForEntityId(entityId);

    // if it is a update, copy over the id so we overwrite the existing entry
    collaborationTag.setId(existingTag.transform(CollaborationTag::getId).or(UUID.randomUUID().toString()));

    tagsStore.save(collaborationTag);
  }

  public Optional<Wiki> getWiki(String entityId) throws NamespaceException {
    validateEntity(entityId);

    final Optional<CollaborationWiki> wiki = wikiStore.getLatestWikiForEntityId(entityId);

    return wiki.transform(Wiki::fromCollaborationWiki);
  }

  public void setWiki(String entityId, Wiki wiki) throws NamespaceException {
    Preconditions.checkNotNull(entityId, "Entity id is required.");
    validateEntity(entityId);

    final CollaborationWiki collaborationWiki = new CollaborationWiki();
    collaborationWiki.setText(wiki.getText());
    collaborationWiki.setCreatedAt(System.currentTimeMillis());

    // store the user
    User user = null;
    try {
      user = sabotContext.getUserService().getUser(context.getUserPrincipal().getName());
    } catch (UserNotFoundException e) {
      throw new RuntimeException(String.format("Could not load user [%s].", context.getUserPrincipal().getName()));
    }

    collaborationWiki.setId(UUID.randomUUID().toString());
    collaborationWiki.setUserId(user.getUID().getId());

    final Optional<CollaborationWiki> existingWiki = wikiStore.getLatestWikiForEntityId(entityId);

    collaborationWiki.setVersion(existingWiki.transform(l -> {
      // check if versions match
      Long existingVersion = l.getVersion();
      Long newVersion = wiki.getVersion();

      if (!existingVersion.equals(newVersion)) {
        throw new ConcurrentModificationException(String.format("The provided version [%s] does not match the stored version [%s].", newVersion, existingVersion));
      }

      // We create a new entry for each update but keep incrementing the version (for sorting).  Because we do this we can't
      // rely on the store to do the version incrementing and therefore do it manually.
      return existingVersion + 1L;
    }).or(0L));

    collaborationWiki.setEntityId(entityId);

    wikiStore.save(collaborationWiki);
  }

  private NameSpaceContainer validateEntity(String entityId) throws NamespaceException {
    final NameSpaceContainer entity = namespaceService.getEntityById(entityId);
    if (entity == null) {
      throw new IllegalArgumentException(String.format("Could not find entity with id [%s].", entityId));
    }

    return entity;
  }

  private void validateEntityForTag(String entityId) throws NamespaceException {
    final NameSpaceContainer entity = validateEntity(entityId);

    if (entity.getType() != NameSpaceContainer.Type.DATASET) {
      throw UserException.validationError()
        .message("Tags can only be set on datasets but found [%s].", entity.getType())
        .build(logger);
    }
  }
}
