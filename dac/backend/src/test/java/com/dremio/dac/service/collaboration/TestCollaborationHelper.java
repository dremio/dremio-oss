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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.proto.model.collaboration.CollaborationWiki;
import com.dremio.dac.server.DACSecurityContext;
import com.dremio.dac.service.search.SearchService;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.CatalogEventProto;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.dremio.service.users.proto.UID;
import com.dremio.services.pubsub.MessagePublisher;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.core.SecurityContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCollaborationHelper {
  private static final String userName = "testUser";
  private static final User user =
      SimpleUser.newBuilder()
          .setUserName(userName)
          .setUID(new UID(UUID.randomUUID().toString()))
          .build();

  private MessagePublisher<CatalogEventProto.CatalogEventMessage> messagePublisher;
  private LegacyKVStoreProvider kvStoreProvider;
  private NamespaceService namespaceService;
  private SecurityContext securityContext;
  private SearchService searchService;
  private UserService userService;
  private CatalogService catalogService;
  private OptionManager optionManager;

  @BeforeEach
  public void setUp() {
    kvStoreProvider = mock(LegacyKVStoreProvider.class);
    namespaceService = mock(NamespaceService.class);
    securityContext = new DACSecurityContext(new UserName(userName), user, null);
    searchService = mock(SearchService.class);
    userService = mock(UserService.class);
    catalogService = mock(CatalogService.class);
    optionManager = mock(OptionManager.class);
    messagePublisher = mock(MessagePublisher.class);
  }

  @Test
  public void testCatalogEventMessagePublishing_Wiki()
      throws NamespaceException, UserNotFoundException {
    EntityId sourceId = new EntityId().setId(UUID.randomUUID().toString());
    NameSpaceContainer sourceContainer = new NameSpaceContainer();
    sourceContainer.setType(NameSpaceContainer.Type.SOURCE);
    sourceContainer.setFullPathList(List.of("source1"));
    when(namespaceService.getEntityById(sourceId)).thenReturn(Optional.of(sourceContainer));

    LegacyIndexedStore<String, CollaborationWiki> wikiStore = mock(LegacyIndexedStore.class);
    when(kvStoreProvider.getStore(CollaborationWikiStore.CollaborationWikiStoreStoreCreator.class))
        .thenReturn(wikiStore);
    when(wikiStore.find(any(LegacyIndexedStore.LegacyFindByCondition.class))).thenReturn(List.of());
    when(userService.getUser(userName)).thenReturn(user);

    CollaborationHelper collaborationHelper = createCollaborationHelper();
    collaborationHelper.setWiki(sourceId.getId(), new Wiki("wiki1", 1L));

    verify(messagePublisher)
        .publish(
            CatalogEventProto.CatalogEventMessage.newBuilder()
                .addEvents(
                    CatalogEventProto.CatalogEventMessage.CatalogEvent.newBuilder()
                        .addAllPath(sourceContainer.getFullPathList())
                        .setEventType(
                            CatalogEventProto.CatalogEventMessage.CatalogEventType
                                .CATALOG_EVENT_TYPE_UPDATED))
                .build());
    verifyNoMoreInteractions(messagePublisher);
  }

  @Test
  public void testCatalogEventMessagePublishing_Tags() throws NamespaceException {
    EntityId tableId = new EntityId().setId(UUID.randomUUID().toString());
    NameSpaceContainer tableContainer = new NameSpaceContainer();
    tableContainer.setType(NameSpaceContainer.Type.DATASET);
    tableContainer.setFullPathList(List.of("source1", "table1"));
    when(namespaceService.getEntityById(tableId)).thenReturn(Optional.of(tableContainer));

    LegacyIndexedStore<String, CollaborationTag> tagsStore = mock(LegacyIndexedStore.class);
    when(kvStoreProvider.getStore(CollaborationTagStore.CollaborationTagsStoreCreator.class))
        .thenReturn(tagsStore);

    CollaborationHelper collaborationHelper = createCollaborationHelper();
    collaborationHelper.setTags(
        tableId.getId(), new Tags(List.of("tag1"), UUID.randomUUID().toString()));

    verify(messagePublisher)
        .publish(
            CatalogEventProto.CatalogEventMessage.newBuilder()
                .addEvents(
                    CatalogEventProto.CatalogEventMessage.CatalogEvent.newBuilder()
                        .addAllPath(tableContainer.getFullPathList())
                        .setEventType(
                            CatalogEventProto.CatalogEventMessage.CatalogEventType
                                .CATALOG_EVENT_TYPE_UPDATED))
                .build());
    verifyNoMoreInteractions(messagePublisher);
  }

  private CollaborationHelper createCollaborationHelper() {
    return new CollaborationHelper(
        kvStoreProvider,
        namespaceService,
        securityContext,
        searchService,
        userService,
        catalogService,
        () -> messagePublisher,
        optionManager);
  }
}
