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
package com.dremio.dac.service.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import com.dremio.dac.proto.model.collaboration.CollaborationTag;
import com.dremio.dac.proto.model.collaboration.CollaborationWiki;
import com.dremio.dac.service.collaboration.CollaborationTagStore;
import com.dremio.dac.service.collaboration.CollaborationWikiStore;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators;
import com.dremio.search.pubsub.SearchDocumentSubscription;
import com.dremio.service.namespace.CatalogEventProto;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.catalogpubsub.CatalogEventsTopic;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.function.proto.FunctionBody;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.function.proto.FunctionDefinition;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.search.SearchDocumentIdProto;
import com.dremio.service.search.SearchDocumentMessageProto;
import com.dremio.service.search.SearchDocumentProto;
import com.dremio.services.pubsub.ImmutableMessagePublisherOptions;
import com.dremio.services.pubsub.ImmutableMessageSubscriberOptions;
import com.dremio.services.pubsub.MessageConsumer;
import com.dremio.services.pubsub.MessageContainerBase;
import com.dremio.services.pubsub.MessagePublisher;
import com.dremio.services.pubsub.MessageSubscriber;
import com.dremio.services.pubsub.PubSubClient;
import com.dremio.services.pubsub.inprocess.InProcessPubSubClientProvider;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Tests for {@link CatalogSearchPublisher}. */
@ExtendWith(MockitoExtension.class)
public class TestCatalogSearchPublisherImpl {
  private static final String SPACE_NAME = "space";
  private static final String VIEW_NAME = "view";
  private static final ImmutableList<String> VIEW_PATH = ImmutableList.of(SPACE_NAME, VIEW_NAME);
  private static final String VIEW_PATH_STRING = String.join(".", VIEW_PATH);
  private static final String COLUMN_A = "a";
  private static final String COLUMN_B = "b";

  private static final String FUNCTION_NAME = "function";
  private static final String FUNCTION_BODY = "SELECT * FROM sys.views";
  private static final ImmutableList<String> FUNCTION_PATH = ImmutableList.of(FUNCTION_NAME);
  private static final String FUNCTION_PATH_STRING = String.join(".", FUNCTION_PATH);

  private LocalKVStoreProvider kvStoreProvider;
  private NamespaceServiceImpl namespaceService;
  private CollaborationWikiStore wikiStore;
  private CollaborationTagStore tagStore;
  @Mock private OptionManager optionManager;

  private final TestSearchDocumentConsumer searchDocumentConsumer =
      new TestSearchDocumentConsumer();
  private MessagePublisher<CatalogEventProto.CatalogEventMessage> catalogEventsPublisher;

  @Captor
  private ArgumentCaptor<SearchDocumentMessageProto.SearchDocumentMessage> documentMessageCaptor;

  @BeforeEach
  public void setUp() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    LegacyKVStoreProvider legacyKVStoreProvider = kvStoreProvider.asLegacy();
    legacyKVStoreProvider.start();

    // Mock option manager.
    doAnswer(
            (args) -> {
              TypeValidators.LongValidator arg = args.getArgument(0);
              return arg.getDefault().getNumVal();
            })
        .when(optionManager)
        .getOption(any(TypeValidators.LongValidator.class));

    // Create pubsub client.
    InProcessPubSubClientProvider pubSubClientProvider =
        new InProcessPubSubClientProvider(() -> optionManager, OpenTelemetry.noop(), null);
    PubSubClient pubSubClient = pubSubClientProvider.get();

    namespaceService = new NamespaceServiceImpl(kvStoreProvider, new CatalogStatusEventsImpl());
    wikiStore = new CollaborationWikiStore(legacyKVStoreProvider);
    tagStore = new CollaborationTagStore(legacyKVStoreProvider);

    // Start catalog events subscriber.
    CatalogSearchPublisherImpl catalogSearchPublisher =
        new CatalogSearchPublisherImpl(
            () -> namespaceService, legacyKVStoreProvider, pubSubClientProvider);
    catalogSearchPublisher.start();

    // Get catalog events publisher.
    catalogEventsPublisher =
        pubSubClient.getPublisher(
            CatalogEventsTopic.class, new ImmutableMessagePublisherOptions.Builder().build());

    // Start subscriber for search documents.
    MessageSubscriber<SearchDocumentMessageProto.SearchDocumentMessage> searchDocumentSubscriber =
        pubSubClient.getSubscriber(
            SearchDocumentSubscription.class,
            searchDocumentConsumer,
            new ImmutableMessageSubscriberOptions.Builder().build());
    searchDocumentSubscriber.start();

    // Create space and view.
    NamespaceKey spaceKey = new NamespaceKey(SPACE_NAME);
    namespaceService.addOrUpdateSpace(spaceKey, new SpaceConfig().setName(spaceKey.getName()));
    namespaceService.addOrUpdateDataset(
        new NamespaceKey(VIEW_PATH),
        CatalogSearchPublisherTestUtils.createVirtualDataset(
            spaceKey,
            VIEW_PATH.get(1),
            ImmutableList.of(
                new ViewFieldType(COLUMN_A, "INT"), new ViewFieldType(COLUMN_B, "STRING"))));

    // Create function.
    NamespaceKey functionKey = new NamespaceKey(FUNCTION_NAME);
    namespaceService.addOrUpdateFunction(
        functionKey,
        new FunctionConfig()
            .setName(FUNCTION_NAME)
            .setFunctionDefinitionsList(
                ImmutableList.of(
                    new FunctionDefinition()
                        .setFunctionBody(new FunctionBody().setRawBody(FUNCTION_BODY)))));
  }

  @AfterEach
  public void tearDown() throws Exception {
    kvStoreProvider.close();
  }

  @Test
  public void test_deleteEvent() throws Exception {
    // Notify consumer.
    CatalogEventProto.CatalogEventMessage catalogEventMessage =
        CatalogEventProto.CatalogEventMessage.newBuilder()
            .addEvents(
                CatalogEventProto.CatalogEventMessage.CatalogEvent.newBuilder()
                    .addAllPath(VIEW_PATH)
                    .setEventType(
                        CatalogEventProto.CatalogEventMessage.CatalogEventType
                            .CATALOG_EVENT_TYPE_DELETED)
                    .build())
            .build();
    CountDownLatch latch = searchDocumentConsumer.initLatch();
    catalogEventsPublisher.publish(catalogEventMessage);

    // Wait for search document.
    assertTrue(latch.await(10, TimeUnit.SECONDS));

    // Verify search document.
    assertEquals(
        SearchDocumentMessageProto.SearchDocumentMessage.newBuilder()
            .setEventType(
                SearchDocumentMessageProto.SearchDocumentMessage.SearchDocumentEventType
                    .SEARCH_DOCUMENT_EVENT_TYPE_DELETED)
            .setDocumentId(
                SearchDocumentIdProto.SearchDocumentId.newBuilder()
                    .setPath(VIEW_PATH_STRING)
                    .build())
            .build(),
        searchDocumentConsumer.getMessages().get(0).getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test_createUpdateEventDataset(boolean updateOrCreate) throws Exception {
    // Create wiki and tags.
    String datasetEntityId = namespaceService.getEntityIdByPath(new NamespaceKey(VIEW_PATH));
    wikiStore.save(
        new CollaborationWiki()
            .setId(datasetEntityId)
            .setEntityId(datasetEntityId)
            .setText("Text"));
    tagStore.save(
        new CollaborationTag()
            .setId(datasetEntityId)
            .setEntityId(datasetEntityId)
            .setTagsList(ImmutableList.of("test", "eng")));

    // Notify consumer.
    CatalogEventProto.CatalogEventMessage catalogEventMessage =
        CatalogEventProto.CatalogEventMessage.newBuilder()
            .addEvents(
                CatalogEventProto.CatalogEventMessage.CatalogEvent.newBuilder()
                    .addAllPath(VIEW_PATH)
                    .setEventType(
                        updateOrCreate
                            ? CatalogEventProto.CatalogEventMessage.CatalogEventType
                                .CATALOG_EVENT_TYPE_UPDATED
                            : CatalogEventProto.CatalogEventMessage.CatalogEventType
                                .CATALOG_EVENT_TYPE_CREATED)
                    .build())
            .build();
    CountDownLatch latch = searchDocumentConsumer.initLatch();
    catalogEventsPublisher.publish(catalogEventMessage);

    // Wait for search document.
    assertTrue(latch.await(10, TimeUnit.SECONDS));

    // Verify search document.
    assertEquals(
        SearchDocumentMessageProto.SearchDocumentMessage.newBuilder()
            .setEventType(
                updateOrCreate
                    ? SearchDocumentMessageProto.SearchDocumentMessage.SearchDocumentEventType
                        .SEARCH_DOCUMENT_EVENT_TYPE_UPDATED
                    : SearchDocumentMessageProto.SearchDocumentMessage.SearchDocumentEventType
                        .SEARCH_DOCUMENT_EVENT_TYPE_CREATED)
            .setDocumentId(
                SearchDocumentIdProto.SearchDocumentId.newBuilder()
                    .setPath(VIEW_PATH_STRING)
                    .build())
            .setDocument(
                SearchDocumentProto.SearchDocument.newBuilder()
                    .setCatalogObject(
                        SearchDocumentProto.CatalogObject.newBuilder()
                            .setPath(VIEW_PATH_STRING)
                            .setType("VIEW")
                            .setWiki("Text")
                            .addLabels("test")
                            .addLabels("eng")
                            .addColumns(COLUMN_A)
                            .addColumns(COLUMN_B)
                            .build())
                    .build())
            .build(),
        CatalogSearchPublisherTestUtils.verifyAndClearTimestamps(
            searchDocumentConsumer.getMessages().get(0).getMessage()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test_createUpdateEventFunction(boolean updateOrCreate) throws Exception {
    // Create wiki and tags.
    String functionEntityId = namespaceService.getEntityIdByPath(new NamespaceKey(FUNCTION_PATH));
    wikiStore.save(
        new CollaborationWiki()
            .setId(functionEntityId)
            .setEntityId(functionEntityId)
            .setText("Text"));
    tagStore.save(
        new CollaborationTag()
            .setId(functionEntityId)
            .setEntityId(functionEntityId)
            .setTagsList(ImmutableList.of("test", "eng")));

    // Notify consumer.
    CatalogEventProto.CatalogEventMessage catalogEventMessage =
        CatalogEventProto.CatalogEventMessage.newBuilder()
            .addEvents(
                CatalogEventProto.CatalogEventMessage.CatalogEvent.newBuilder()
                    .addAllPath(FUNCTION_PATH)
                    .setEventType(
                        updateOrCreate
                            ? CatalogEventProto.CatalogEventMessage.CatalogEventType
                                .CATALOG_EVENT_TYPE_UPDATED
                            : CatalogEventProto.CatalogEventMessage.CatalogEventType
                                .CATALOG_EVENT_TYPE_CREATED)
                    .build())
            .build();
    CountDownLatch latch = searchDocumentConsumer.initLatch();
    catalogEventsPublisher.publish(catalogEventMessage);

    // Wait for search document.
    assertTrue(latch.await(10, TimeUnit.SECONDS));

    // Verify search document.
    assertEquals(
        SearchDocumentMessageProto.SearchDocumentMessage.newBuilder()
            .setEventType(
                updateOrCreate
                    ? SearchDocumentMessageProto.SearchDocumentMessage.SearchDocumentEventType
                        .SEARCH_DOCUMENT_EVENT_TYPE_UPDATED
                    : SearchDocumentMessageProto.SearchDocumentMessage.SearchDocumentEventType
                        .SEARCH_DOCUMENT_EVENT_TYPE_CREATED)
            .setDocumentId(
                SearchDocumentIdProto.SearchDocumentId.newBuilder()
                    .setPath(FUNCTION_PATH_STRING)
                    .build())
            .setDocument(
                SearchDocumentProto.SearchDocument.newBuilder()
                    .setCatalogObject(
                        SearchDocumentProto.CatalogObject.newBuilder()
                            .setPath(FUNCTION_PATH_STRING)
                            .setType("UDF")
                            .setWiki("Text")
                            .addLabels("test")
                            .addLabels("eng")
                            .setUdfSql(FUNCTION_BODY)
                            .build())
                    .build())
            .build(),
        CatalogSearchPublisherTestUtils.verifyAndClearTimestamps(
            searchDocumentConsumer.getMessages().get(0).getMessage()));
  }

  @Test
  public void test_viewNotFoundAck() throws Exception {
    // Notify consumer.
    CatalogEventProto.CatalogEventMessage catalogEventMessage =
        CatalogEventProto.CatalogEventMessage.newBuilder()
            .addEvents(
                CatalogEventProto.CatalogEventMessage.CatalogEvent.newBuilder()
                    .addPath("non-existent")
                    .setEventType(
                        CatalogEventProto.CatalogEventMessage.CatalogEventType
                            .CATALOG_EVENT_TYPE_CREATED)
                    .build())
            .build();
    CountDownLatch latch = searchDocumentConsumer.initLatch();
    catalogEventsPublisher.publish(catalogEventMessage);

    // Search document is not published.
    assertFalse(latch.await(200, TimeUnit.MICROSECONDS));
  }

  @Test
  public void test_publishDocumentExceptionNack() throws Exception {
    // Notify consumer.
    CatalogEventProto.CatalogEventMessage catalogEventMessage =
        CatalogEventProto.CatalogEventMessage.newBuilder()
            .addEvents(
                CatalogEventProto.CatalogEventMessage.CatalogEvent.newBuilder()
                    .addAllPath(VIEW_PATH)
                    .setEventType(
                        CatalogEventProto.CatalogEventMessage.CatalogEventType
                            .CATALOG_EVENT_TYPE_CREATED)
                    .build())
            .build();
    CountDownLatch latch = searchDocumentConsumer.initLatch();
    catalogEventsPublisher.publish(catalogEventMessage);

    // Search document is not published.
    assertFalse(latch.await(200, TimeUnit.MICROSECONDS));
  }

  public static final class TestSearchDocumentConsumer
      implements MessageConsumer<SearchDocumentMessageProto.SearchDocumentMessage> {

    private final List<MessageContainerBase<SearchDocumentMessageProto.SearchDocumentMessage>>
        messages = new ArrayList<>();
    private CountDownLatch latch;

    private CountDownLatch initLatch() {
      latch = new CountDownLatch(1);
      return latch;
    }

    @Override
    public void process(
        MessageContainerBase<SearchDocumentMessageProto.SearchDocumentMessage> message) {
      messages.add(message);
      message.ack();
      latch.countDown();
    }

    private List<MessageContainerBase<SearchDocumentMessageProto.SearchDocumentMessage>>
        getMessages() {
      return messages;
    }
  }
}
